from decimal import Decimal
from enum import StrEnum


REDIS_SEPARATOR = "\r\n"


class IDSimple(StrEnum):
    STRING = '+'
    ERROR = '-'
    INTEGER = ':'
    NULL = '_'
    BOOLEAN = '#'
    DOUBLE = ','
    BIGNUM = '('


class IDAggregate(StrEnum):
    BSTRING = '$'
    ARRAY = '*'
    BERROR = '!'
    VERBATIM = '='
    MAP = '%'
    ATTRIBUTE = '`'
    SET = '~'
    PUSH = '>'


def decode_simple(recv_id: str, value: str):
    match recv_id:
        case IDSimple.STRING:
            return value
        case IDSimple.INTEGER:
            return int(value)
        case IDSimple.NULL:
            assert value == ""
            return None
        case IDSimple.BOOLEAN:
            assert len(value) == 1
            assert value in "tf"
            return value == "t"
        case IDSimple.DOUBLE:
            return float(value)
        case IDSimple.BIGNUM:
            return Decimal(value)
        case _:
            ValueError("recv unhandled simple id", recv_id, value)
