import threading

from pymodbus.client import ModbusSerialClient

from .logutil import logger


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
            timeout=config.get("timeout", 2),
        )
        self.lock = threading.Lock()
        self.connected = False

    def connect(self):
        with self.lock:
            try:
                if self.client.connect():
                    self.connected = True
                    logger.info("%s connected: %s", self.name, self.config["port"])
                    return True
            except Exception as exc:
                logger.error("%s connect error: %s", self.name, exc)
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

    def recover(self):
        logger.warning("%s recovering serial client: %s", self.name, self.config["port"])
        self.close()
        cfg = self.config
        self.client = ModbusSerialClient(
            port=cfg["port"],
            baudrate=cfg.get("baudrate", 9600),
            bytesize=cfg.get("bytesize", 8),
            parity=cfg.get("parity", "N"),
            stopbits=cfg.get("stopbits", 1),
            timeout=cfg.get("timeout", 2),
        )
        return self.connect()

    def read_registers(self, slave_id, address, count, register_type, retries=0):
        last_error = None
        attempts = max(1, retries + 1)
        for attempt in range(attempts):
            if not self.ensure_connected():
                last_error = "not connected"
                continue
            with self.lock:
                try:
                    if register_type == "holding":
                        response = self._read(
                            self.client.read_holding_registers,
                            slave_id,
                            address,
                            count,
                        )
                    elif register_type == "input":
                        response = self._read(
                            self.client.read_input_registers,
                            slave_id,
                            address,
                            count,
                        )
                    else:
                        logger.error("Unsupported register type: %s", register_type)
                        return None

                    if response is None or response.isError():
                        last_error = response
                        logger.warning(
                            "%s slave=%s read error (attempt %s/%s): %s",
                            self.name,
                            slave_id,
                            attempt + 1,
                            attempts,
                            response,
                        )
                        self.connected = False
                        continue
                    return response.registers
                except Exception as exc:
                    last_error = exc
                    logger.error("%s Modbus read error: %s", self.name, exc)
                    self.connected = False
        if last_error is not None:
            logger.warning(
                "%s slave=%s addr=%s count=%s failed: %s",
                self.name,
                slave_id,
                address,
                count,
                last_error,
            )
        return None

    def _read(self, method, slave_id, address, count):
        try:
            return method(address=address, count=count, slave=slave_id)
        except TypeError:
            return method(address=address, count=count, device_id=slave_id)

    def write_register(self, slave_id, address, value, register_type="holding"):
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
                    logger.error("%s write register failed: %s", self.name, response)
                    return False
                logger.warning(
                    "%s MODBUS WRITE: slave=%s address=%s value=%s",
                    self.name,
                    slave_id,
                    address,
                    value,
                )
                return True
            except Exception as exc:
                logger.error("%s write register error: %s", self.name, exc)
                self.connected = False
                return False
