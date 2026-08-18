import json
import time
import sqlite3
import logging
import threading
import struct
import operator
from datetime import datetime

import paho.mqtt.client as mqtt
from pymodbus.client import ModbusSerialClient


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(threadName)s - %(message)s"
)
logger = logging.getLogger("edge-collector")


def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def compare(value, op, threshold):
    operators = {
        ">": operator.gt,
        ">=": operator.ge,
        "<": operator.lt,
        "<=": operator.le,
        "==": operator.eq,
        "!=": operator.ne
    }
    func = operators.get(op)
    return func(value, threshold) if func else False


class Config:
    def __init__(self, path):
        with open(path, "r", encoding="utf-8") as f:
            self.data = json.load(f)

    @property
    def device_id(self):
        return self.data["device_id"]

    @property
    def mqtt(self):
        return self.data["mqtt"]

    @property
    def database(self):
        return self.data["database"]

    @property
    def collect(self):
        return self.data["collect"]

    @property
    def serial_ports(self):
        return self.data["serial_ports"]

    @property
    def rules(self):
        return self.data.get("rules", [])


class SQLiteStore:
    def __init__(self, config):
        self.db_path = config["path"]
        self.max_records = config.get("max_records", 100000)
        self.lock = threading.Lock()

        self.conn = sqlite3.connect(
            self.db_path,
            check_same_thread=False
        )
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.create_tables()

    def create_tables(self):
        with self.lock:
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS mqtt_outbox (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    topic TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    created_at INTEGER NOT NULL,
                    retry_count INTEGER DEFAULT 0
                )
            """)
            self.conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_outbox_created
                ON mqtt_outbox(created_at)
            """)
            self.conn.commit()

    def save(self, topic, payload):
        with self.lock:
            self.conn.execute(
                """
                INSERT INTO mqtt_outbox
                (topic, payload, created_at)
                VALUES (?, ?, ?)
                """,
                (topic, payload, int(time.time()))
            )
            self.conn.commit()
            self.cleanup()

    def get_batch(self, limit=100):
        with self.lock:
            cursor = self.conn.execute(
                """
                SELECT id, topic, payload
                FROM mqtt_outbox
                ORDER BY id ASC
                LIMIT ?
                """,
                (limit,)
            )
            return cursor.fetchall()

    def delete(self, record_id):
        with self.lock:
            self.conn.execute(
                "DELETE FROM mqtt_outbox WHERE id = ?",
                (record_id,)
            )
            self.conn.commit()

    def increase_retry(self, record_id):
        with self.lock:
            self.conn.execute(
                """
                UPDATE mqtt_outbox
                SET retry_count = retry_count + 1
                WHERE id = ?
                """,
                (record_id,)
            )
            self.conn.commit()

    def count(self):
        with self.lock:
            cursor = self.conn.execute(
                "SELECT COUNT(*) FROM mqtt_outbox"
            )
            return cursor.fetchone()[0]

    def cleanup(self):
        count = self.conn.execute(
            "SELECT COUNT(*) FROM mqtt_outbox"
        ).fetchone()[0]

        if count <= self.max_records:
            return

        delete_count = count - self.max_records
        self.conn.execute(
            """
            DELETE FROM mqtt_outbox
            WHERE id IN (
                SELECT id
                FROM mqtt_outbox
                ORDER BY id ASC
                LIMIT ?
            )
            """,
            (delete_count,)
        )
        self.conn.commit()
        logger.warning(
            "SQLite cache exceeded limit, deleted %s old records",
            delete_count
        )


class ModbusPort:
    def __init__(self, config):
        self.name = config["name"]
        self.config = config

        self.client = ModbusSerialClient(
            port=config["port"],
            baudrate=config.get("baudrate", 9600),
            bytesize=config.get("bytesize", 8),
            parity=config.get("parity", "N"),
            stopbits=config.get("stopbits", 1),
            timeout=config.get("timeout", 2)
        )
        self.lock = threading.Lock()
        self.connected = False

    def connect(self):
        with self.lock:
            try:
                if self.client.connect():
                    self.connected = True
                    logger.info(
                        "%s connected: %s",
                        self.name,
                        self.config["port"]
                    )
                    return True
            except Exception as e:
                logger.error("%s connect error: %s", self.name, e)

            self.connected = False
            return False

    def ensure_connected(self):
        return True if self.connected else self.connect()

    def close(self):
        with self.lock:
            try:
                self.client.close()
            except Exception:
                pass
            self.connected = False

    def read_registers(self, slave_id, address, count, register_type):
        if not self.ensure_connected():
            return None

        with self.lock:
            try:
                if register_type == "holding":
                    response = self.client.read_holding_registers(
                        address=address, count=count, slave=slave_id
                    )
                elif register_type == "input":
                    response = self.client.read_input_registers(
                        address=address, count=count, slave=slave_id
                    )
                else:
                    logger.error("Unsupported register type: %s", register_type)
                    return None

                if response.isError():
                    logger.warning(
                        "%s slave=%s read error: %s",
                        self.name, slave_id, response
                    )
                    return None

                return response.registers

            except TypeError:
                try:
                    if register_type == "holding":
                        response = self.client.read_holding_registers(
                            address=address, count=count, device_id=slave_id
                        )
                    else:
                        response = self.client.read_input_registers(
                            address=address, count=count, device_id=slave_id
                        )

                    if response.isError():
                        return None
                    return response.registers
                except Exception as e:
                    logger.error("%s read error: %s", self.name, e)
                    self.connected = False
                    return None

            except Exception as e:
                logger.error("%s Modbus read error: %s", self.name, e)
                self.connected = False
                return None

    def write_register(
        self,
        slave_id,
        address,
        value,
        register_type="holding"
    ):
        if not self.ensure_connected():
            return False

        with self.lock:
            try:
                if register_type != "holding":
                    logger.error("Only holding register write is supported")
                    return False

                try:
                    response = self.client.write_register(
                        address=address, value=value, slave=slave_id
                    )
                except TypeError:
                    response = self.client.write_register(
                        address=address, value=value, device_id=slave_id
                    )

                if response.isError():
                    logger.error(
                        "%s write register failed: %s",
                        self.name, response
                    )
                    return False

                logger.warning(
                    "%s MODBUS WRITE: slave=%s address=%s value=%s",
                    self.name, slave_id, address, value
                )
                return True

            except Exception as e:
                logger.error("%s write register error: %s", self.name, e)
                self.connected = False
                return False


def decode_registers(registers, data_type):
    if not registers:
        return None

    if data_type == "uint16":
        return registers[0]

    if data_type == "int16":
        value = registers[0]
        return value - 65536 if value >= 32768 else value

    if data_type in ("uint32", "int32", "float32"):
        if len(registers) < 2:
            return None

        raw = struct.pack(">HH", registers[0], registers[1])

        if data_type == "uint32":
            return struct.unpack(">I", raw)[0]
        if data_type == "int32":
            return struct.unpack(">i", raw)[0]
        if data_type == "float32":
            return struct.unpack(">f", raw)[0]

    return None


def register_count(data_type):
    return 2 if data_type in ("uint32", "int32", "float32") else 1


class RuleEngine:
    def __init__(self, rules, ports):
        self.rules = rules
        self.ports = ports
        self.counters = {}
        self.triggered = {}
        self.lock = threading.Lock()

    def process(self, values):
        for rule in self.rules:
            name = rule["name"]
            source = rule["source"]

            if source not in values:
                continue

            value = values[source]
            threshold = rule["threshold"]
            op = rule["operator"]
            matched = compare(value, op, threshold)

            with self.lock:
                if matched:
                    self.counters[name] = self.counters.get(name, 0) + 1
                else:
                    self.counters[name] = 0
                    self.triggered[name] = False

                consecutive = rule.get("consecutive", 1)

                if (
                    matched
                    and self.counters[name] >= consecutive
                    and not self.triggered.get(name, False)
                ):
                    logger.error(
                        "SAFETY RULE TRIGGERED: %s value=%s threshold=%s",
                        name, value, threshold
                    )
                    self.triggered[name] = True
                    self.execute_action(rule["action"])

    def execute_action(self, action):
        action_type = action.get("type")

        if action_type == "modbus_write":
            port = self.ports.get(action["port"])

            if not port:
                logger.error(
                    "Safety action port not found: %s",
                    action["port"]
                )
                return

            success = port.write_register(
                slave_id=action["slave_id"],
                address=action["address"],
                value=action["value"],
                register_type=action.get("register_type", "holding")
            )

            if success:
                logger.critical(
                    "DEVICE SHUTDOWN ACTION EXECUTED: %s",
                    action
                )
            else:
                logger.critical(
                    "DEVICE SHUTDOWN ACTION FAILED: %s",
                    action
                )


class MQTTManager:
    def __init__(self, config, store):
        self.config = config
        self.store = store
        self.connected = False
        self.lock = threading.Lock()

        self.client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id=config["client_id"]
        )

        username = config.get("username")
        if username:
            self.client.username_pw_set(
                username,
                config.get("password")
            )

        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect

    def on_connect(
        self, client, userdata, flags, reason_code, properties=None
    ):
        if reason_code == 0:
            self.connected = True
            logger.info("MQTT connected")
        else:
            self.connected = False
            logger.error("MQTT connect failed: %s", reason_code)

    def on_disconnect(
        self, client, userdata, disconnect_flags, reason_code, properties=None
    ):
        self.connected = False
        logger.warning("MQTT disconnected: %s", reason_code)

    def start(self):
        self.client.reconnect_delay_set(min_delay=1, max_delay=30)

        try:
            self.client.connect_async(
                self.config["host"],
                self.config["port"],
                self.config.get("keepalive", 60)
            )
            self.client.loop_start()
        except Exception as e:
            logger.error("MQTT start error: %s", e)

    def publish(self, topic, payload):
        if not self.connected:
            return False

        try:
            info = self.client.publish(
                topic, payload, qos=1, retain=False
            )
            if info.rc != mqtt.MQTT_ERR_SUCCESS:
                return False
            return True
        except Exception as e:
            logger.error("MQTT publish error: %s", e)
            self.connected = False
            return False

    def send_data(self, topic, payload):
        if self.publish(topic, payload):
            return True

        self.store.save(topic, payload)
        logger.warning("MQTT unavailable, data cached locally")
        return False

    def sync_offline_data(self):
        if not self.connected:
            return

        batch_size = self.config.get("offline_sync_batch", 100)
        records = self.store.get_batch(batch_size)

        if not records:
            return

        logger.info("Syncing %s offline records", len(records))

        for record_id, topic, payload in records:
            if not self.connected:
                break

            if self.publish(topic, payload):
                self.store.delete(record_id)
            else:
                self.store.increase_retry(record_id)
                break


class CollectorService:
    def __init__(self, config):
        self.config = config
        self.ports = {}
        self.devices = []

        self.store = SQLiteStore(config.database)

        for port_config in config.serial_ports:
            port = ModbusPort(port_config)
            self.ports[port_config["name"]] = port
            self.devices.append(port_config)

        self.mqtt = MQTTManager(config.mqtt, self.store)

        self.rule_engine = RuleEngine(
            config.rules,
            self.ports
        )

        self.running = True

    def read_parameter(self, port, slave_id, parameter):
        data_type = parameter["data_type"]
        count = register_count(data_type)

        registers = port.read_registers(
            slave_id=slave_id,
            address=parameter["address"],
            count=count,
            register_type=parameter.get("register_type", "holding")
        )

        if registers is None:
            return None

        value = decode_registers(registers, data_type)
        if value is None:
            return None

        scale = parameter.get("scale", 1)
        offset = parameter.get("offset", 0)
        return value * scale + offset

    def collect_once(self):
        values = {}
        timestamp = int(time.time())

        data = {
            "device_id": self.config.device_id,
            "timestamp": timestamp,
            "datetime": now_str(),
            "data": {}
        }

        for port_config in self.devices:
            port_name = port_config["name"]
            port = self.ports[port_name]

            if not port.ensure_connected():
                logger.error("%s unavailable", port_name)
                continue

            for device in port_config["devices"]:
                slave_id = device["slave_id"]

                for parameter in device["parameters"]:
                    name = parameter["name"]

                    value = self.read_parameter(
                        port, slave_id, parameter
                    )

                    source = f"{port_name}:{slave_id}:{name}"

                    if value is None:
                        logger.warning("Read failed: %s", source)
                        continue

                    values[source] = value

                    data["data"][source] = {
                        "value": value,
                        "unit": parameter.get("unit")
                    }

                    logger.info(
                        "%s = %s %s",
                        source, value, parameter.get("unit", "")
                    )

        return data, values

    def run(self):
        self.mqtt.start()

        collect_interval = self.config.collect.get("interval", 10)
        upload_interval = self.config.collect.get("upload_interval", 60)

        last_upload = 0

        while self.running:
            loop_start = time.time()

            try:
                data, values = self.collect_once()
                self.rule_engine.process(values)

                now = time.time()

                if now - last_upload >= upload_interval:
                    payload = json.dumps(
                        data,
                        ensure_ascii=False,
                        separators=(",", ":")
                    )

                    self.mqtt.send_data(
                        self.config.mqtt["topic"],
                        payload
                    )

                    last_upload = now
                    self.mqtt.sync_offline_data()

            except Exception as e:
                logger.exception(
                    "Collector main loop error: %s",
                    e
                )

            elapsed = time.time() - loop_start
            time.sleep(max(0, collect_interval - elapsed))

    def stop(self):
        self.running = False
        self.mqtt.client.loop_stop()

        for port in self.ports.values():
            port.close()


def main():
    logger.info("Starting Edge Collector...")

    config = Config("config.json")
    service = CollectorService(config)

    try:
        service.run()
    except KeyboardInterrupt:
        logger.info("Stopping...")
        service.stop()


if __name__ == "__main__":
    main()
