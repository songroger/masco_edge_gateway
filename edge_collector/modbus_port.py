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

    def _call(self, method, **kwargs):
        """Call pymodbus with slave= or device_id= depending on version."""
        try:
            return method(**kwargs)
        except TypeError:
            if "slave" in kwargs:
                kwargs["device_id"] = kwargs.pop("slave")
                return method(**kwargs)
            raise

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
                        response = self._call(
                            self.client.read_holding_registers,
                            address=address,
                            count=count,
                            slave=slave_id,
                        )
                    elif register_type == "input":
                        response = self._call(
                            self.client.read_input_registers,
                            address=address,
                            count=count,
                            slave=slave_id,
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

    def read_bits(self, slave_id, address, count, bit_type, retries=0):
        """Read coils (0x01) or discrete inputs (0x02). Returns list of 0/1."""
        last_error = None
        attempts = max(1, retries + 1)
        for attempt in range(attempts):
            if not self.ensure_connected():
                last_error = "not connected"
                continue
            with self.lock:
                try:
                    if bit_type == "coil":
                        response = self._call(
                            self.client.read_coils,
                            address=address,
                            count=count,
                            slave=slave_id,
                        )
                    elif bit_type == "discrete":
                        response = self._call(
                            self.client.read_discrete_inputs,
                            address=address,
                            count=count,
                            slave=slave_id,
                        )
                    else:
                        logger.error("Unsupported bit type: %s", bit_type)
                        return None

                    if response is None or response.isError():
                        last_error = response
                        logger.warning(
                            "%s slave=%s bit read error (attempt %s/%s): %s",
                            self.name,
                            slave_id,
                            attempt + 1,
                            attempts,
                            response,
                        )
                        self.connected = False
                        continue
                    bits = list(response.bits[:count])
                    return [1 if bit else 0 for bit in bits]
                except Exception as exc:
                    last_error = exc
                    logger.error("%s Modbus bit read error: %s", self.name, exc)
                    self.connected = False
        if last_error is not None:
            logger.warning(
                "%s slave=%s addr=%s bit_type=%s failed: %s",
                self.name,
                slave_id,
                address,
                bit_type,
                last_error,
            )
        return None

    def write_register(self, slave_id, address, value, register_type="holding"):
        """Write one holding register. Prefers FC 0x10 (protocol), falls back to 0x06."""
        if register_type != "holding":
            logger.error("Only holding register write is supported")
            return False
        if not self.ensure_connected():
            return False
        with self.lock:
            try:
                response = self._call(
                    self.client.write_registers,
                    address=address,
                    values=[int(value) & 0xFFFF],
                    slave=slave_id,
                )
                if response is None or response.isError():
                    response = self._call(
                        self.client.write_register,
                        address=address,
                        value=int(value) & 0xFFFF,
                        slave=slave_id,
                    )
                if response is None or response.isError():
                    logger.error("%s write register failed: %s", self.name, response)
                    return False
                logger.warning(
                    "%s MODBUS WRITE holding: slave=%s address=%s value=%s",
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

    def write_registers(self, slave_id, address, values, register_type="holding"):
        """Write multiple holding registers (FC 0x10)."""
        if register_type != "holding":
            logger.error("Only holding register write is supported")
            return False
        if not self.ensure_connected():
            return False
        words = [int(v) & 0xFFFF for v in values]
        with self.lock:
            try:
                response = self._call(
                    self.client.write_registers,
                    address=address,
                    values=words,
                    slave=slave_id,
                )
                if response is None or response.isError():
                    logger.error("%s write registers failed: %s", self.name, response)
                    return False
                logger.warning(
                    "%s MODBUS WRITE holdings: slave=%s address=%s values=%s",
                    self.name,
                    slave_id,
                    address,
                    words,
                )
                return True
            except Exception as exc:
                logger.error("%s write registers error: %s", self.name, exc)
                self.connected = False
                return False

    def write_coil(self, slave_id, address, value):
        """Write single coil (FC 0x05). value truthy → ON (0xFF00), else OFF."""
        if not self.ensure_connected():
            return False
        on = bool(int(value)) if not isinstance(value, bool) else value
        with self.lock:
            try:
                response = self._call(
                    self.client.write_coil,
                    address=address,
                    value=on,
                    slave=slave_id,
                )
                if response is None or response.isError():
                    logger.error("%s write coil failed: %s", self.name, response)
                    return False
                logger.warning(
                    "%s MODBUS WRITE coil: slave=%s address=%s value=%s",
                    self.name,
                    slave_id,
                    address,
                    1 if on else 0,
                )
                return True
            except Exception as exc:
                logger.error("%s write coil error: %s", self.name, exc)
                self.connected = False
                return False
