from collections.abc import Callable

from app.redis.rdb.constants import RDBOpCode

# https://github.com/pasztorpisti/py-crc
# SPDX-License-Identifier: MIT-0
# SPDX-FileCopyrightText:  2023 Istvan Pasztor
"""
Arbitrary-precision CRC calculator in Python

Execute this script as a command to calculate CRCs or to run the tests.
Use this as a module to access predefined CRC algorithms (through the
CRC_CATALOGUE list, CRC_PARAMS dict and the create_crc_fn function).

The parametric_crc() function can calculate a wide range of CRCs including those
that aren't 8, 16, 32 or 64 bits wide, for example CRC-5/USB or CRC-82/DARC.
It was tested against the parameters of all the 100+ CRC algorithms that
are currently listed in the CRC catalogue of the CRC RevEng project:
https://reveng.sourceforge.io/crc-catalogue/all.htm
"""


def reverse_bits(value: int, width: int) -> int:
    assert 0 <= value < (1 << width)
    return int("{v:0{w}b}".format(v=value, w=width)[::-1], 2)


reversed_int8_bits = tuple(reverse_bits(i, 8) for i in range(256))


def parametric_crc(
    data: bytes,
    ref_init: int,
    *,
    width: int,
    ref_poly: int,
    refin: bool,
    refout: bool,
    xorout: int,
    bit_len: int | None = None,
    interim: bool = False,
    residue: bool = False,
    table: list[int] | None = None,
) -> int:
    """Parametrized CRC function. Uses a reflected (LSB-first) CRC register
    because this has simpler implementation than the unreflected (MSB-first)
    variant. The ref_init and ref_poly parameters are the reflected values of
    the init and poly parameters listed in the RevEng CRC catalogue."""
    bit_len = len(data) * 8 if bit_len is None else bit_len
    assert width > 0 and 0 <= xorout < (1 << width) and bit_len <= len(data) * 8
    crc = ref_init
    if table:  # the table is used for processing units of 8 bits (whole bytes)
        num_bytes, bit_len = bit_len >> 3, bit_len & 7
        for i in range(num_bytes):
            b = data[i] if refin else reversed_int8_bits[data[i]]
            crc = table[(crc & 0xFF) ^ b] ^ (crc >> 8)
        data = data[num_bytes : num_bytes + 1] if bit_len else b""
    for b in data:  # even with a table we may have up to 7 bits remaining
        byte = b if refin else reversed_int8_bits[b]
        if bit_len < 8:
            if bit_len <= 0:
                break
            byte &= (1 << bit_len) - 1  # zeroing the unused bits
        crc ^= byte
        for _ in range(min(bit_len, 8)):
            crc = (crc >> 1) ^ ref_poly if crc & 1 else crc >> 1
        bit_len -= 8
    if interim:
        return crc
    crc = crc if refout else reverse_bits(crc, width)
    return crc if residue else crc ^ xorout


def specialized_crc(
    width: int,
    poly: int,
    init: int,
    refin: bool,
    refout: bool,
    xorout: int,
    tableless: bool = False,
) -> Callable[[bytes], int]:
    """Creates a CRC function for a specific CRC algorithm.
    The parameters are expected in the format used in the RevEng CRC catalogue:
    https://reveng.sourceforge.io/crc-catalogue/all.htm"""
    ref_init = reverse_bits(init, width)  # compatibility with the CRC catalogue
    ref_poly = reverse_bits(poly, width)  # compatibility with the CRC catalogue
    p = {
        "width": width,
        "ref_poly": ref_poly,
        "xorout": xorout,
        "refin": refin,
        "refout": refout,
    }
    t = (
        None
        if tableless
        else [parametric_crc(b"\0", i, interim=True, **p) for i in range(256)]
    )

    def crc_fn(
        data: bytes,
        ref_init: int = ref_init,
        *,
        interim: bool = False,
        residue: bool = False,
        bit_len: int | None = None,
    ) -> int:
        return parametric_crc(
            data,
            ref_init,
            interim=interim,
            residue=residue,
            bit_len=bit_len,
            table=t,
            **p,
        )

    return crc_fn


def crc64_redis(data: bytes) -> int:
    return specialized_crc(
        width=64,
        poly=0xAD93D23594C935A9,
        init=0x0000000000000000,
        refin=True,
        refout=True,
        xorout=0x0000000000000000,
        # check=0xe9c6d914c4b8d9ca
        # residue=0x0000000000000000
    )(data + bytes([RDBOpCode.EOF]))
