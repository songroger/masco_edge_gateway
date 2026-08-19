import time
from concurrent.futures import ThreadPoolExecutor, wait
from datetime import datetime

from .decode import decode_registers, parse_address, register_count, scale_value
from .logutil import logger


BIT_TYPES = ("coil", "discrete")
# Modbus RTU 单帧实用上限（寄存器约 125；线圈可更大）
DEFAULT_MAX_COUNT = 125
DEFAULT_MAX_COUNT_BITS = 256


def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _param_address(param):
    return parse_address(param["address"])


def _param_span(param):
    start = _param_address(param)
    return start, start + register_count(param["data_type"])


def build_batches(parameters, max_gap=0, max_count=DEFAULT_MAX_COUNT,
                  max_gap_bits=None, max_count_bits=None):
    """Merge parameters into as few Modbus reads as possible.

    Strategy (per register_type):
    1. Sort by address.
    2. Always merge overlapping or back-to-back spans (e.g. 0~10 + 11~20 → one read).
    3. Also bridge holes up to ``max_gap`` (registers) / ``max_gap_bits`` (coils),
       while keeping the request size ≤ ``max_count`` / ``max_count_bits``.

    Returns list of ``(register_type, start_address, count, [params...])``.
    """
    if max_gap_bits is None:
        max_gap_bits = max_gap
    if max_count_bits is None:
        max_count_bits = DEFAULT_MAX_COUNT_BITS

    grouped = {}
    for param in parameters:
        grouped.setdefault(param.get("register_type", "holding"), []).append(param)

    batches = []
    for register_type, items in grouped.items():
        items = sorted(items, key=_param_address)
        gap_limit = max_gap_bits if register_type in BIT_TYPES else max_gap
        size_limit = max_count_bits if register_type in BIT_TYPES else max_count
        batches.extend(
            _pack_spans(register_type, items, gap_limit, size_limit)
        )
    return batches


def _pack_spans(register_type, items, max_gap, max_count):
    """Greedy left-to-right pack of sorted parameter spans into read windows."""
    if not items:
        return []

    batches = []
    current = []
    start = end = None

    for param in items:
        p_start, p_end = _param_span(param)
        if not current:
            current = [param]
            start, end = p_start, p_end
            continue

        # Hole between current window end and next parameter start (0 = adjacent).
        hole = max(0, p_start - end)
        merged_end = max(end, p_end)
        merged_count = merged_end - start

        # Adjacent/overlap always merge when size allows; else allow hole ≤ max_gap.
        contiguous = hole == 0
        gap_ok = contiguous or hole <= max_gap
        size_ok = merged_count <= max_count

        if gap_ok and size_ok:
            current.append(param)
            end = merged_end
            continue

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
        self.max_gap = int(config.collect.get("batch_max_gap", 16))
        self.max_count = int(config.collect.get("batch_max_count", DEFAULT_MAX_COUNT))
        self.max_gap_bits = int(
            config.collect.get("batch_max_gap_bits", max(self.max_gap, 32))
        )
        self.max_count_bits = int(
            config.collect.get("batch_max_count_bits", DEFAULT_MAX_COUNT_BITS)
        )
        # Precompute read windows once per device (config hot-reload rebuilds Collector).
        self._batch_cache = {}
        self._warm_batch_cache()
        workers = max(1, len(ports))
        self.executor = ThreadPoolExecutor(
            max_workers=workers,
            thread_name_prefix="rs485",
        )

    def _warm_batch_cache(self):
        for port_config in self.config.serial_ports:
            for device in port_config.get("devices", []):
                key = (port_config["name"], device["slave_id"])
                batches = build_batches(
                    device["parameters"],
                    self.max_gap,
                    self.max_count,
                    self.max_gap_bits,
                    self.max_count_bits,
                )
                self._batch_cache[key] = batches
                n_params = len(device["parameters"])
                n_reads = len(batches)
                logger.info(
                    "%s slave=%s Modbus batch: %s params → %s requests (gap=%s/%s count=%s/%s)",
                    port_config["name"],
                    device["slave_id"],
                    n_params,
                    n_reads,
                    self.max_gap,
                    self.max_gap_bits,
                    self.max_count,
                    self.max_count_bits,
                )
                for register_type, address, count, params in batches:
                    logger.debug(
                        "  batch %s addr=0x%04X count=%s points=%s",
                        register_type,
                        address,
                        count,
                        len(params),
                    )

    def _batches_for(self, port_name, device):
        key = (port_name, device["slave_id"])
        batches = self._batch_cache.get(key)
        if batches is None:
            batches = build_batches(
                device["parameters"],
                self.max_gap,
                self.max_count,
                self.max_gap_bits,
                self.max_count_bits,
            )
            self._batch_cache[key] = batches
        return batches

    def close(self):
        try:
            self.executor.shutdown(wait=False)
        except Exception:
            pass

    def recover_executor(self):
        self.close()
        workers = max(1, len(self.ports))
        self.executor = ThreadPoolExecutor(
            max_workers=workers,
            thread_name_prefix="rs485",
        )
        logger.warning("RS485 collector thread pool recreated")

    def collect_timeout(self):
        timeout = float(self.config.collect.get("request_timeout", 2))
        retry = max(0, self.retry)
        ports = max(1, len(self.config.serial_ports))
        return max(8.0, timeout * (retry + 1) * 8 + ports)

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
        future_map = {}
        for port_config in self.config.serial_ports:
            port = self.ports.get(port_config["name"])
            if not port:
                comm_status.update(self._mark_port_failed(port_config))
                continue
            future_map[self.executor.submit(self._collect_port, port, port_config)] = port_config

        timeout = self.collect_timeout()
        if future_map:
            done, pending = wait(future_map.keys(), timeout=timeout)
        else:
            done, pending = set(), set()
        for future in pending:
            port_config = future_map[future]
            logger.error("%s collect timeout after %ss", port_config["name"], timeout)
            future.cancel()
            port = self.ports.get(port_config["name"])
            if port:
                port.connected = False
            comm_status.update(self._mark_port_failed(port_config))

        for future in done:
            port_config = future_map[future]
            try:
                port_values, port_data, port_comm = future.result()
            except Exception:
                logger.exception("%s collect failed", port_config["name"])
                comm_status.update(self._mark_port_failed(port_config))
                continue
            values.update(port_values)
            data["data"].update(port_data)
            comm_status.update(port_comm)

        for source, ok in comm_status.items():
            data["comm"][source] = "ok" if ok else "fail"
        return data, values, comm_status

    def _mark_port_failed(self, port_config):
        comm_status = {port_config["name"]: False}
        for device in port_config.get("devices", []):
            slave_id = device["slave_id"]
            device_key = "%s:%s" % (port_config["name"], slave_id)
            comm_status[device_key] = False
            for parameter in device.get("parameters", []):
                source = "%s:%s:%s" % (port_config["name"], slave_id, parameter["name"])
                comm_status[source] = False
        return comm_status

    def _collect_port(self, port, port_config):
        values = {}
        data = {}
        comm_status = {}
        port_name = port_config["name"]

        if not port.ensure_connected():
            logger.error("%s unavailable", port_name)
            return values, data, self._mark_port_failed(port_config)

        for device in port_config["devices"]:
            slave_id = device["slave_id"]
            device_key = "%s:%s" % (port_name, slave_id)
            device_ok = False
            batches = self._batches_for(port_name, device)
            decoded = {}

            for register_type, address, count, params in batches:
                if register_type in BIT_TYPES:
                    bits = port.read_bits(
                        slave_id=slave_id,
                        address=address,
                        count=count,
                        bit_type=register_type,
                        retries=self.retry,
                    )
                    if bits is None:
                        continue
                    for parameter in params:
                        offset = _param_address(parameter) - address
                        if offset < 0 or offset >= len(bits):
                            continue
                        raw = bits[offset]
                        value = scale_value(
                            raw,
                            parameter.get("scale", 1),
                            parameter.get("offset", 0),
                        )
                        decoded[parameter["name"]] = (parameter, value)
                    continue

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
                    offset = _param_address(parameter) - address
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

        port_ok = any(
            comm_status.get("%s:%s" % (port_name, device["slave_id"]))
            for device in port_config["devices"]
        )
        comm_status[port_name] = bool(port_ok)
        return values, data, comm_status
