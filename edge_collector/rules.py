import operator
import threading
import time

from .decode import parse_address
from .logutil import logger


def compare(value, op, threshold):
    operators = {
        ">": operator.gt,
        ">=": operator.ge,
        "<": operator.lt,
        "<=": operator.le,
        "==": operator.eq,
        "!=": operator.ne,
    }
    func = operators.get(op)
    return func(value, threshold) if func else False


class RuleEngine:
    def __init__(self, rules, ports, gpio=None):
        self.rules = rules or []
        self.ports = ports
        self.gpio = gpio
        self.counters = {}
        self.triggered = {}
        self.lock = threading.Lock()

    def process(self, values, comm_status=None):
        comm_status = comm_status or {}
        events = []
        for rule in self.rules:
            try:
                event = self._process_rule(rule, values, comm_status)
                if event:
                    events.append(event)
            except Exception:
                logger.exception("Rule processing failed: %s", rule.get("name"))
        return events

    def _process_rule(self, rule, values, comm_status):
        name = rule["name"]
        rule_type = rule.get("type", "threshold")
        source = rule["source"]
        matched = False
        current_value = None

        if rule_type == "comm_fail":
            ok = comm_status.get(source)
            matched = ok is False
        else:
            if source not in values:
                with self.lock:
                    self.counters[name] = 0
                    self.triggered[name] = False
                return None
            current_value = values[source]
            matched = compare(current_value, rule["operator"], rule["threshold"])

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
                    "SAFETY RULE TRIGGERED: %s type=%s source=%s count=%s",
                    name,
                    rule_type,
                    source,
                    self.counters[name],
                )
                self.triggered[name] = True
                action = rule.get("action") or {}
                action_type = action.get("type")
                try:
                    if action_type and action_type != "alarm":
                        self.execute_action(action)
                    elif not action_type and (
                        action.get("modbus") or action.get("gpio") or action.get("port")
                    ):
                        self.execute_action(action)
                except Exception:
                    logger.exception("Safety action failed: %s", name)
                alarm_cfg = action.get("alarm") if isinstance(action.get("alarm"), dict) else {}
                return {
                    "device_id": None,
                    "name": name,
                    "type": rule_type,
                    "source": source,
                    "value": current_value,
                    "consecutive": self.counters[name],
                    "severity": alarm_cfg.get("severity", "critical"),
                    "code": alarm_cfg.get("code") or rule_type.upper(),
                    "message": alarm_cfg.get("message")
                    or "%s triggered on %s" % (name, source),
                    "ts": int(time.time()),
                }
        return None

    def execute_action(self, action):
        action_type = action.get("type", "modbus_write")
        results = []

        if action_type == "alarm":
            return True
        if action_type in ("gpio", "dual"):
            results.append(("gpio", self._gpio_action(action)))
        if action_type in ("modbus_write", "dual"):
            results.append(("modbus", self._modbus_action(action)))
        if action_type not in ("gpio", "modbus_write", "dual", "alarm"):
            logger.error("Unknown safety action type: %s", action_type)
            return False

        success = any(ok for _, ok in results)
        if success and all(ok for _, ok in results):
            logger.critical("DEVICE SHUTDOWN ACTION EXECUTED: %s", action)
        elif success:
            logger.critical("DEVICE SHUTDOWN PARTIAL SUCCESS: %s results=%s", action, results)
        else:
            logger.critical("DEVICE SHUTDOWN ACTION FAILED: %s", action)
        return success

    def _gpio_action(self, action):
        gpio_cfg = action.get("gpio") or {}
        line = gpio_cfg.get("line", action.get("gpio_line"))
        value = gpio_cfg.get("value", action.get("gpio_value", 1))
        if line is None:
            logger.error("GPIO shutdown missing line")
            return False
        if not self.gpio:
            logger.error("GPIO controller is not configured")
            return False
        return self.gpio.set_line(int(line), value)

    def _modbus_action(self, action):
        spec = action.get("modbus") or action
        port_name = spec.get("port")
        port = self.ports.get(port_name)
        if not port:
            logger.error("Safety action port not found: %s", port_name)
            return False
        address = parse_address(spec["address"])
        register_type = spec.get("register_type", "holding")
        value = spec["value"]
        if register_type == "coil":
            return port.write_coil(
                slave_id=spec["slave_id"],
                address=address,
                value=value,
            )
        if register_type == "holding":
            values = spec.get("values")
            if values is not None:
                return port.write_registers(
                    slave_id=spec["slave_id"],
                    address=address,
                    values=values,
                    register_type="holding",
                )
            return port.write_register(
                slave_id=spec["slave_id"],
                address=address,
                value=value,
                register_type="holding",
            )
        logger.error("Unsupported Modbus write register_type: %s", register_type)
        return False
