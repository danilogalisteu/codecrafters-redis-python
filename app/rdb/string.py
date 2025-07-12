from .length import decode_length, encode_length


def decode_string(data: bytes) -> tuple[int, str]:
    dlen = len(data)
    if dlen < 1:
        return 0, ""

    spos, slen = decode_length(data)
    if spos == 0:
        return 0, ""

    return spos + slen, data[spos : spos + slen].decode()


def encode_string(value: str) -> bytes:
    return encode_length(len(value)) + value.encode()
