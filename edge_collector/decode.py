import struct


REGISTER_COUNT = {
    "uint16": 1,
    "int16": 1,
    "uint32": 2,
    "int32": 2,
    "float32": 2,
}


def register_count(data_type):
    count = REGISTER_COUNT.get(data_type)
    if count is None:
        raise ValueError("Unsupported data_type: %s" % data_type)
    return count


def _words_to_bytes(registers):
    parts = []
    for word in registers:
        parts.append((word >> 8) & 0xFF)
        parts.append(word & 0xFF)
    return parts


def _apply_byte_order(raw_bytes, byte_order):
    order = (byte_order or "").upper()
    n = len(raw_bytes)

    if n == 2:
        if order in ("", "AB"):
            return bytes(raw_bytes)
        if order == "BA":
            return bytes([raw_bytes[1], raw_bytes[0]])
        raise ValueError("Unsupported 16-bit byte_order: %s" % byte_order)

    if n == 4:
        a, b, c, d = raw_bytes
        mapping = {
            "": (a, b, c, d),
            "ABCD": (a, b, c, d),
            "CDAB": (c, d, a, b),
            "BADC": (b, a, d, c),
            "DCBA": (d, c, b, a),
        }
        ordered = mapping.get(order)
        if ordered is None:
            raise ValueError("Unsupported 32-bit byte_order: %s" % byte_order)
        return bytes(ordered)

    raise ValueError("Unsupported register length: %s" % n)


def decode_registers(registers, data_type, byte_order=None):
    if not registers:
        return None

    count = register_count(data_type)
    if len(registers) < count:
        return None

    raw_bytes = _words_to_bytes(registers[:count])
    ordered = _apply_byte_order(raw_bytes, byte_order)

    if data_type == "uint16":
        return struct.unpack(">H", ordered)[0]
    if data_type == "int16":
        return struct.unpack(">h", ordered)[0]
    if data_type == "uint32":
        return struct.unpack(">I", ordered)[0]
    if data_type == "int32":
        return struct.unpack(">i", ordered)[0]
    if data_type == "float32":
        return struct.unpack(">f", ordered)[0]
    return None


def scale_value(value, scale=1, offset=0):
    if value is None:
        return None
    return value * scale + offset
