import json
import os
import tempfile
from urllib.parse import urlparse
from copy import deepcopy

from .decode import parse_address, register_count
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


def normalize_config(data):
    """Normalize the documented gateway schema to the runtime schema."""
    if "gateway" not in data or "collector" not in data:
        return data
    gateway = data.get("gateway") or {}
    mqtt = data.get("mqtt") or {}
    parsed = urlparse(mqtt.get("broker", "tcp://localhost:1883"))
    topics = mqtt.get("topics") or {}
    collector = data.get("collector") or {}
    ports = []
    for channel in collector.get("channels", []):
        devices = []
        for device in channel.get("devices", []):
            devices.append({
                "id": device.get("id"),
                "slave_id": device.get("slave_id"),
                "parameters": device.get("points", []),
            })
        ports.append({
            "name": channel.get("id"), "enabled": channel.get("enabled", True),
            "port": channel.get("device"), "baudrate": channel.get("baud_rate", 9600),
            "bytesize": channel.get("data_bits", 8), "stopbits": channel.get("stop_bits", 1),
            "parity": channel.get("parity", "N"), "timeout": collector.get("request_timeout_ms", 1000) / 1000,
            "poll_ms": channel.get("poll_ms", collector.get("default_poll_ms", 60000)),
            "devices": devices,
        })
    rules = []
    for rule in data.get("rules", []):
        r = dict(rule)
        r["name"] = r.get("id", "rule")
        r["type"] = "comm_fail" if r.get("operator") == "COMM_ERROR" else r.get("type", "threshold")
        r["source"] = "%s:%s:%s" % (r.get("channel_id"), r.get("device_id"), r.get("point_name"))
        action = dict(r.get("action") or {})
        if "channel_id" in action:
            action["port"] = action.pop("channel_id")
        r["action"] = action
        rules.append(r)
    result = {
        "device_id": gateway.get("id", gateway.get("sn")),
        "sn": gateway.get("sn", gateway.get("id")),
        "mqtt": {"host": parsed.hostname or "localhost", "port": parsed.port or 1883,
                 "client_id": gateway.get("id", gateway.get("sn")), "username": mqtt.get("username"),
                 "password": mqtt.get("password"), "keepalive": mqtt.get("keep_alive_sec", 60),
                 "qos": mqtt.get("qos", 0), "topic": topics.get("telemetry"),
                 "config_topic": topics.get("config"),
                 "alarm_topic": "%s/%s" % (topics.get("event", "").rstrip("/"), gateway.get("sn", gateway.get("id"))),
                 "command_topic": "%s/%s" % (topics.get("command", "").rstrip("/"), gateway.get("sn", gateway.get("id"))),
                 "config_ack_topic": "%s/%s" % (topics.get("event", "").rstrip("/"), gateway.get("sn", gateway.get("id"))),
                 "refresh_topic": "%s/%s" % (topics.get("refresh", "").rstrip("/"), gateway.get("sn", gateway.get("id"))),
                 "tls": {"enable": (mqtt.get("tls") or {}).get("enabled", False),
                         "ca_certs": (mqtt.get("tls") or {}).get("ca_file"),
                         "certfile": (mqtt.get("tls") or {}).get("cert_file"),
                         "keyfile": (mqtt.get("tls") or {}).get("key_file"),
                         "insecure": (mqtt.get("tls") or {}).get("insecure_skip_verify", False)}},
        "database": {"path": (data.get("database") or {}).get("path", "./data/gateway.db"),
                      "max_records": (data.get("database") or {}).get("max_rows", 200000)},
        "collect": {"interval": collector.get("default_poll_ms", 60000) / 1000,
                     "upload_interval": collector.get("default_poll_ms", 60000) / 1000,
                     "request_timeout": collector.get("request_timeout_ms", 1000) / 1000,
                     "retry": collector.get("retries", 0), "batch_max_gap": collector.get("batch_max_gap", 0),
                     "batch_max_count": collector.get("batch_max_span", 125)},
        "serial_ports": ports, "rules": rules,
    }
    return result


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
        if not isinstance(port["devices"], list):
            raise ConfigError("%s.devices must be a list" % port["name"])
        for j, device in enumerate(port["devices"]):
            _require(device, REQUIRED_DEVICE, "%s.devices[%s]" % (port["name"], j))
            for k, param in enumerate(device["parameters"]):
                where = "%s.devices[%s].parameters[%s]" % (port["name"], j, k)
                _require(param, REQUIRED_PARAM, where)
                try:
                    parse_address(param["address"])
                except (TypeError, ValueError) as exc:
                    raise ConfigError("%s: invalid address: %s" % (where, exc)) from exc
                try:
                    register_count(param["data_type"])
                except ValueError as exc:
                    raise ConfigError("%s: %s" % (where, exc)) from exc
                reg_type = param.get("register_type", "holding")
                if reg_type not in ("holding", "input", "coil", "discrete"):
                    raise ConfigError("%s: unsupported register_type: %s" % (where, reg_type))

    rules = data.get("rules", [])
    if not isinstance(rules, list):
        raise ConfigError("rules must be a list")
    return True


META_KEYS = ("op", "cmd", "patch", "config", "__error__")


def unwrap_remote_payload(payload):
    if isinstance(payload, dict) and "config" in payload and isinstance(payload["config"], dict):
        return payload["config"]
    return payload


def deep_merge(base, patch):
    result = deepcopy(base)
    for key, value in (patch or {}).items():
        if key in ("serial_ports", "rules") and isinstance(value, list):
            result[key] = deepcopy(value)
        elif isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = deepcopy(value)
    return result


def parse_remote_config(payload, current):
    if not isinstance(payload, dict):
        raise ConfigError("remote config must be a JSON object")
    if payload.get("__error__"):
        raise ConfigError(str(payload["__error__"]))

    op = payload.get("op") or payload.get("cmd")
    if "patch" in payload and isinstance(payload["patch"], dict):
        merged = deep_merge(current, payload["patch"])
        validate_config(merged)
        return merged, "patch"
    if op in ("patch", "update", "merge"):
        patch = {key: value for key, value in payload.items() if key not in META_KEYS}
        if "config" in payload and isinstance(payload["config"], dict):
            patch = payload["config"]
        merged = deep_merge(current, patch)
        validate_config(merged)
        return merged, "patch"
    if "config" in payload and isinstance(payload["config"], dict):
        data = payload["config"]
        validate_config(data)
        return deepcopy(data), "replace"
    if all(key in payload for key in REQUIRED_ROOT):
        validate_config(payload)
        return deepcopy(payload), "replace"

    patch = {key: value for key, value in payload.items() if key not in META_KEYS}
    if not patch:
        raise ConfigError("empty remote config")
    merged = deep_merge(current, patch)
    validate_config(merged)
    return merged, "patch"


class Config:
    def __init__(self, path):
        self.path = path
        self.data = {}
        self.load()

    def load(self):
        with open(self.path, "r", encoding="utf-8") as handle:
            data = json.load(handle)
        data = normalize_config(data)
        validate_config(data)
        self.data = data
        logger.info("Config loaded from %s", self.path)

    def reload(self):
        self.load()

    def apply_remote(self, payload, backup=True):
        data, mode = parse_remote_config(payload, self.data)
        self.apply_dict(data, backup=backup)
        return mode

    def apply_dict(self, data, backup=True):
        data = unwrap_remote_payload(data)
        data = normalize_config(data)
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
    def sn(self):
        return self.data.get("sn", self.device_id)

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
