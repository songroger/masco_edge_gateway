import json
import time

from .collector import Collector
from .config import Config, ConfigError
from .gpio_control import GpioController
from .logutil import logger
from .modbus_port import ModbusPort
from .mqtt_manager import MQTTManager
from .rules import RuleEngine
from .store import SQLiteStore
from .watchdog import SystemdWatchdog


class CollectorService:
    def __init__(self, config_path):
        self.config_path = config_path
        self.config = Config(config_path)
        self.running = True
        self.store = None
        self.mqtt = None
        self.ports = {}
        self.gpio = None
        self.rule_engine = None
        self.collector = None
        self.watchdog = None
        self._mqtt_fail = 0
        self._rs485_fail = {}
        self._sqlite_fail = 0
        self._build()

    def _port_timeout(self, port_config):
        return port_config.get("timeout", self.config.collect.get("request_timeout", 2))

    def _build(self, reuse_store=None, reuse_mqtt=None):
        self._close_runtime(keep_store=reuse_store is not None, keep_mqtt=reuse_mqtt is not None)

        self.store = reuse_store or SQLiteStore(self.config.database)
        self.ports = {}
        for port_config in self.config.serial_ports:
            merged = dict(port_config)
            merged["timeout"] = self._port_timeout(port_config)
            port = ModbusPort(merged)
            self.ports[port_config["name"]] = port

        self.gpio = GpioController(self.config.gpio)
        self.rule_engine = RuleEngine(self.config.rules, self.ports, self.gpio)
        self.collector = Collector(self.config, self.ports)
        self.watchdog = SystemdWatchdog(self.config.watchdog)
        self._rs485_fail = {}
        self._mqtt_fail = 0
        self._sqlite_fail = 0

        if reuse_mqtt is not None:
            reuse_mqtt.config = self.config.mqtt
            reuse_mqtt.store = self.store
            self.mqtt = reuse_mqtt
        else:
            self.mqtt = MQTTManager(self.config.mqtt, self.store)
            self.mqtt.start()

    def _close_runtime(self, keep_store=False, keep_mqtt=False):
        if self.collector:
            self.collector.close()
            self.collector = None
        for port in self.ports.values():
            try:
                port.close()
            except Exception:
                logger.exception("Error closing serial port")
        self.ports = {}
        if self.gpio:
            try:
                self.gpio.close()
            except Exception:
                logger.exception("Error closing GPIO")
            self.gpio = None
        if self.mqtt and not keep_mqtt:
            try:
                self.mqtt.stop()
            except Exception:
                logger.exception("Error stopping MQTT")
            self.mqtt = None
        if self.store and not keep_store:
            try:
                self.store.close()
            except Exception:
                logger.exception("Error closing SQLite")
            self.store = None

    def _maybe_apply_remote_config(self):
        if not self.mqtt:
            return
        payload = self.mqtt.poll_config()
        if payload is None:
            return
        if isinstance(payload, dict) and payload.get("__error__"):
            self._safe_ack(False, payload["__error__"])
            return
        try:
            old_mqtt_sig = MQTTManager.signature_from_config(self.mqtt.config)
            old_db_path = self.config.database.get("path")
            mode = self.config.apply_remote(payload)
            keep_store = self.config.database.get("path") == old_db_path
            keep_mqtt = MQTTManager.signature_from_config(self.config.mqtt) == old_mqtt_sig
            self._build(
                reuse_store=self.store if keep_store else None,
                reuse_mqtt=self.mqtt if keep_mqtt else None,
            )
            self._safe_ack(
                True,
                "config applied (%s)" % mode,
                extra={"device_id": self.config.device_id, "ts": int(time.time()), "mode": mode},
            )
            logger.info("Remote config applied mode=%s", mode)
        except ConfigError as exc:
            logger.error("Remote config rejected: %s", exc)
            self._safe_ack(False, str(exc))
        except Exception as exc:
            logger.exception("Remote config apply failed")
            self._safe_ack(False, str(exc))

    def _safe_ack(self, ok, message, extra=None):
        if not self.mqtt:
            return
        extra = extra or {"device_id": self.config.device_id}
        try:
            self.mqtt.publish_ack(ok, message, extra=extra)
        except Exception:
            logger.exception("Config ack publish failed")

    def _recover_sqlite(self):
        logger.warning("Recovering SQLite module")
        try:
            if self.store:
                self.store.recover()
            else:
                self.store = SQLiteStore(self.config.database)
            if self.mqtt:
                self.mqtt.store = self.store
            self._sqlite_fail = 0
        except Exception:
            logger.exception("SQLite recover failed")
            self._sqlite_fail += 1

    def _recover_mqtt(self):
        logger.warning("Recovering MQTT module")
        try:
            if self.mqtt:
                self.mqtt.config = self.config.mqtt
                self.mqtt.store = self.store
                self.mqtt.restart()
            else:
                self.mqtt = MQTTManager(self.config.mqtt, self.store)
                self.mqtt.start()
            self._mqtt_fail = 0
        except Exception:
            logger.exception("MQTT recover failed")
            self._mqtt_fail += 1

    def _recover_rs485(self, port_name=None):
        names = [port_name] if port_name else [item["name"] for item in self.config.serial_ports]
        for name in names:
            logger.warning("Recovering RS485 module: %s", name)
            try:
                port_config = next(
                    item for item in self.config.serial_ports if item["name"] == name
                )
                old = self.ports.get(name)
                if old:
                    try:
                        old.close()
                    except Exception:
                        pass
                merged = dict(port_config)
                merged["timeout"] = self._port_timeout(port_config)
                self.ports[name] = ModbusPort(merged)
                self.ports[name].connect()
                self._rs485_fail[name] = 0
            except Exception:
                logger.exception("RS485 recover failed: %s", name)
                self._rs485_fail[name] = self._rs485_fail.get(name, 0) + 1
        if self.collector:
            self.collector.ports = self.ports
        if self.rule_engine:
            self.rule_engine.ports = self.ports

    def _health_and_recover(self, comm_status=None):
        wd = self.config.watchdog or {}
        mqtt_limit = int(wd.get("mqtt_restart_after", 3))
        rs485_limit = int(wd.get("rs485_restart_after", 3))
        sqlite_limit = int(wd.get("sqlite_restart_after", 1))

        try:
            sqlite_ok = bool(self.store and self.store.healthy())
        except Exception:
            sqlite_ok = False
        if sqlite_ok:
            self._sqlite_fail = 0
        else:
            self._sqlite_fail += 1
            if self._sqlite_fail >= sqlite_limit:
                self._recover_sqlite()

        mqtt_ok = bool(self.mqtt and self.mqtt.healthy())
        if mqtt_ok:
            self._mqtt_fail = 0
        else:
            self._mqtt_fail += 1
            if self._mqtt_fail >= mqtt_limit:
                self._recover_mqtt()

        comm_status = comm_status or {}
        for port_config in self.config.serial_ports:
            name = port_config["name"]
            port = self.ports.get(name)
            port_ok = bool(port and port.connected)
            if port_ok:
                self._rs485_fail[name] = 0
                continue
            self._rs485_fail[name] = self._rs485_fail.get(name, 0) + 1
            if self._rs485_fail[name] >= rs485_limit:
                self._recover_rs485(name)

        if self.collector and getattr(self.collector.executor, "_shutdown", False):
            try:
                self.collector.recover_executor()
            except Exception:
                logger.exception("Collector executor recover failed")

    def _publish_alarms(self, events):
        if not events or not self.mqtt:
            return
        for event in events:
            event["device_id"] = self.config.device_id
            try:
                self.mqtt.publish_alarm(event)
            except Exception:
                logger.exception("Alarm publish failed: %s", event.get("name"))

    def run(self):
        collect_interval = self.config.collect.get("interval", 10)
        upload_interval = self.config.collect.get("upload_interval", 60)
        last_upload = 0
        self.watchdog.ready()
        logger.info("Edge collector service started")

        while self.running:
            loop_start = time.time()
            data = None
            values = {}
            comm_status = {}

            try:
                self._maybe_apply_remote_config()
            except Exception:
                logger.exception("Config hot-reload step failed")

            collect_interval = self.config.collect.get("interval", 10)
            upload_interval = self.config.collect.get("upload_interval", 60)

            try:
                data, values, comm_status = self.collector.collect_once()
            except Exception:
                logger.exception("Collect step failed")
                try:
                    self.collector.recover_executor()
                    self._recover_rs485()
                except Exception:
                    logger.exception("Collect recovery failed")
                comm_status = {}
                for port_config in self.config.serial_ports:
                    comm_status.update(self.collector._mark_port_failed(port_config) if self.collector else {})

            try:
                events = self.rule_engine.process(values, comm_status) if self.rule_engine else []
            except Exception:
                logger.exception("Rule engine step failed")
                events = []

            try:
                self._publish_alarms(events)
            except Exception:
                logger.exception("Alarm step failed")

            try:
                now = time.time()
                if data is not None and now - last_upload >= upload_interval:
                    payload = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
                    if self.mqtt:
                        self.mqtt.send_data(self.config.mqtt["topic"], payload)
                    last_upload = now
                if self.mqtt:
                    self.mqtt.sync_offline_data()
            except Exception:
                logger.exception("MQTT/SQLite upload step failed")
                self._recover_mqtt()
                self._recover_sqlite()

            try:
                self._health_and_recover(comm_status)
            except Exception:
                logger.exception("Health recover step failed")

            try:
                self.watchdog.ping()
            except Exception:
                logger.exception("Watchdog ping failed")

            elapsed = time.time() - loop_start
            time.sleep(max(0, collect_interval - elapsed))

    def stop(self):
        self.running = False
        if self.watchdog:
            try:
                self.watchdog.stopping()
            except Exception:
                pass
        self._close_runtime()
        logger.info("Edge collector service stopped")
