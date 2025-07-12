import struct


def decode_length(data: bytes) -> tuple[int, int]:
    dlen = len(data)
    if dlen < 1:
        return 0, 0

    senc = data[0] >> 5

    if senc == 0b00:
        return 1, data[0]

    if senc == 0b01:
        if dlen < 3:
            return 0, 0
        return 2, struct.unpack(">H", data[0:2])[0] & 0x3FFF

    if senc == 0b10:
        if dlen < 5:
            return 0, 0
        return 5, struct.unpack(">L", data[1:5])[0]

    sfmt = data[0] & 0x3F

    if sfmt == 0:
        if dlen < 2:
            return 0, 0
        return 2, data[1]

    if sfmt == 1:
        if dlen < 3:
            return 0, 0
        return 3, struct.unpack("<H", data[1:3])[0]

    if sfmt == 2:
        if dlen < 5:
            return 0, 0
        return 5, struct.unpack("<L", data[1:5])[0]

    raise ValueError("unhandled length decoding format")


def encode_length(value: int) -> bytes:
    if value < 1 << 6:
        return bytes([value])
    if value < 1 << 14:
        return struct.pack(">H", 1 << 14 | value)
    return bytes([1 << 7]) + struct.pack(">L", value)


def encode_length_special(value: int) -> bytes:
    if value < 1 << 8:
        return bytes([0b11 << 6, value])
    if value < 1 << 16:
        return bytes([(0b11 << 6) | 1]) + struct.pack("<H", value)
    return bytes([(0b11 << 6) | 2]) + struct.pack("<L", value)
