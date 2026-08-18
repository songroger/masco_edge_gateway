import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

from .decode import decode_registers, register_count, scale_value
from .logutil import logger


def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def build_batches(parameters, max_gap, max_count):
    grouped = {}
    for param in parameters:
        grouped.setdefault(param.get("register_type", "holding"), []).append(param)

    batches = []
    for register_type, items in grouped.items():
        items = sorted(items, key=lambda item: item["address"])
        current = []
        start = end = None
        for param in items:
            p_start = param["address"]
            p_end = p_start + register_count(param["data_type"])
            if not current:
                current = [param]
                start, end = p_start, p_end
                continue
            merged_end = max(end, p_end)
            gap_ok = p_start <= end + max_gap
            size_ok = (merged_end - start) <= max_count
            if gap_ok and size_ok:
                current.append(param)
                end = merged_end
            else:
                batches.append((register_type, start, end - start, current))
                current = [param]
                start, end = p_start, p_end
        if current:
            batches.append((register_type, start, end - start, current))
    return batches


class Collector:
    def __init__(self, config, ports):
        self.config = config
        self.ports = ports
        self.retry = int(config.collect.get("retry", 0))
        self.max_gap = int(config.collect.get("batch_max_gap", 0))
        self.max_count = int(config.collect.get("batch_max_count", 64))
        workers = max(1, len(ports))
        self.executor = ThreadPoolExecutor(
            max_workers=workers,
            thread_name_prefix="rs485",
        )

    def close(self):
        self.executor.shutdown(wait=False)

    def collect_once(self):
        timestamp = int(time.time())
        data = {
            "device_id": self.config.device_id,
            "timestamp": timestamp,
            "datetime": now_str(),
            "data": {},
            "comm": {},
        }
        values = {}
        comm_status = {}
        futures = []
        for port_config in self.config.serial_ports:
            port = self.ports.get(port_config["name"])
            if not port:
                continue
            futures.append(
                self.executor.submit(self._collect_port, port, port_config)
            )

        for future in as_completed(futures):
            try:
                port_values, port_data, port_comm = future.result()
            except Exception:
                logger.exception("RS485 port collect failed")
                continue
            values.update(port_values)
            data["data"].update(port_data)
            comm_status.update(port_comm)

        for source, ok in comm_status.items():
            data["comm"][source] = "ok" if ok else "fail"
        return data, values, comm_status

    def _collect_port(self, port, port_config):
        values = {}
        data = {}
        comm_status = {}
        port_name = port_config["name"]

        if not port.ensure_connected():
            logger.error("%s unavailable", port_name)
            for device in port_config["devices"]:
                slave_id = device["slave_id"]
                device_key = "%s:%s" % (port_name, slave_id)
                comm_status[device_key] = False
                for parameter in device["parameters"]:
                    source = "%s:%s:%s" % (port_name, slave_id, parameter["name"])
                    comm_status[source] = False
            return values, data, comm_status

        for device in port_config["devices"]:
            slave_id = device["slave_id"]
            device_key = "%s:%s" % (port_name, slave_id)
            device_ok = False
            batches = build_batches(device["parameters"], self.max_gap, self.max_count)
            decoded = {}

            for register_type, address, count, params in batches:
                registers = port.read_registers(
                    slave_id=slave_id,
                    address=address,
                    count=count,
                    register_type=register_type,
                    retries=self.retry,
                )
                if registers is None:
                    continue
                for parameter in params:
                    offset = parameter["address"] - address
                    nregs = register_count(parameter["data_type"])
                    slice_regs = registers[offset:offset + nregs]
                    try:
                        raw = decode_registers(
                            slice_regs,
                            parameter["data_type"],
                            parameter.get("byte_order"),
                        )
                    except ValueError as exc:
                        logger.error("Decode error %s: %s", parameter["name"], exc)
                        continue
                    value = scale_value(
                        raw,
                        parameter.get("scale", 1),
                        parameter.get("offset", 0),
                    )
                    if value is not None:
                        decoded[parameter["name"]] = (parameter, value)

            for parameter in device["parameters"]:
                source = "%s:%s:%s" % (port_name, slave_id, parameter["name"])
                item = decoded.get(parameter["name"])
                if item is None:
                    comm_status[source] = False
                    logger.warning("Read failed: %s", source)
                    continue
                _, value = item
                comm_status[source] = True
                device_ok = True
                values[source] = value
                data[source] = {
                    "value": value,
                    "unit": parameter.get("unit"),
                }
                logger.info("%s = %s %s", source, value, parameter.get("unit", ""))

            comm_status[device_key] = device_ok

        return values, data, comm_status
