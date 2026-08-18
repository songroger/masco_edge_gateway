import json
import os
import tempfile
from copy import deepcopy

from .decode import register_count
from .logutil import logger


REQUIRED_ROOT = ("device_id", "mqtt", "database", "collect", "serial_ports")
REQUIRED_MQTT = ("host", "port", "client_id", "topic")
REQUIRED_DB = ("path",)
REQUIRED_COLLECT = ()
REQUIRED_PORT = ("name", "port", "devices")
REQUIRED_DEVICE = ("slave_id", "parameters")
REQUIRED_PARAM = ("name", "address", "data_type")


class ConfigError(ValueError):
    pass


def _require(obj, keys, where):
    if not isinstance(obj, dict):
        raise ConfigError("%s must be an object" % where)
    for key in keys:
        if key not in obj:
            raise ConfigError("%s missing field: %s" % (where, key))


def validate_config(data):
    _require(data, REQUIRED_ROOT, "config")
    _require(data["mqtt"], REQUIRED_MQTT, "mqtt")
    _require(data["database"], REQUIRED_DB, "database")
    if not isinstance(data.get("collect"), dict):
        raise ConfigError("collect must be an object")
    if not isinstance(data["serial_ports"], list) or not data["serial_ports"]:
        raise ConfigError("serial_ports must be a non-empty list")

    names = set()
    for i, port in enumerate(data["serial_ports"]):
        _require(port, REQUIRED_PORT, "serial_ports[%s]" % i)
        if port["name"] in names:
            raise ConfigError("duplicate serial port name: %s" % port["name"])
        names.add(port["name"])
        if not isinstance(port["devices"], list) or not port["devices"]:
            raise ConfigError("%s.devices must be a non-empty list" % port["name"])
        for j, device in enumerate(port["devices"]):
            _require(device, REQUIRED_DEVICE, "%s.devices[%s]" % (port["name"], j))
            for k, param in enumerate(device["parameters"]):
                where = "%s.devices[%s].parameters[%s]" % (port["name"], j, k)
                _require(param, REQUIRED_PARAM, where)
                try:
                    register_count(param["data_type"])
                except ValueError as exc:
                    raise ConfigError("%s: %s" % (where, exc)) from exc

    rules = data.get("rules", [])
    if not isinstance(rules, list):
        raise ConfigError("rules must be a list")
    return True


def unwrap_remote_payload(payload):
    if isinstance(payload, dict) and "config" in payload and isinstance(payload["config"], dict):
        return payload["config"]
    return payload


class Config:
    def __init__(self, path):
        self.path = path
        self.data = {}
        self.load()

    def load(self):
        with open(self.path, "r", encoding="utf-8") as handle:
            data = json.load(handle)
        validate_config(data)
        self.data = data
        logger.info("Config loaded from %s", self.path)

    def reload(self):
        self.load()

    def apply_dict(self, data, backup=True):
        data = unwrap_remote_payload(data)
        validate_config(data)
        if backup and os.path.exists(self.path):
            backup_path = self.path + ".bak"
            with open(self.path, "r", encoding="utf-8") as handle:
                old = handle.read()
            with open(backup_path, "w", encoding="utf-8") as handle:
                handle.write(old)
        self._atomic_write(data)
        self.data = deepcopy(data)
        logger.info("Config applied and saved to %s", self.path)

    def _atomic_write(self, data):
        directory = os.path.dirname(os.path.abspath(self.path)) or "."
        fd, tmp_path = tempfile.mkstemp(prefix="config.", suffix=".json", dir=directory)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(data, handle, ensure_ascii=False, indent=2)
                handle.write("\n")
            os.replace(tmp_path, self.path)
        except Exception:
            try:
                os.remove(tmp_path)
            except OSError:
                pass
            raise

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

    @property
    def gpio(self):
        return self.data.get("gpio", {})

    @property
    def watchdog(self):
        return self.data.get("watchdog", {})
