from .length import decode_length, decode_length_special, encode_length


def decode_string(data: bytes) -> tuple[int, str | int]:
    dlen = len(data)
    if dlen < 1:
        return 0, ""

    senc = data[0] >> 6
    if senc == 0b11:
        return decode_length_special(data)

    spos, slen = decode_length(data)
    if spos == 0:
        return 0, ""
    return spos + slen, data[spos : spos + slen].decode()


def encode_string(value: str) -> bytes:
    return encode_length(len(value)) + value.encode()
