import logging
from decimal import Decimal
from enum import StrEnum
from typing import Any

REDIS_SEPARATOR = "\r\n"
REDIS_SEPARATOR_LENGTH = len(REDIS_SEPARATOR)


class IDSimple(StrEnum):
    STRING = "+"
    ERROR = "-"
    INTEGER = ":"
    NULL = "_"
    BOOLEAN = "#"
    DOUBLE = ","
    BIGNUM = "("


class IDAggregate(StrEnum):
    BSTRING = "$"
    ARRAY = "*"
    BERROR = "!"
    VERBATIM = "="
    MAP = "%"
    ATTRIBUTE = "`"
    SET = "~"
    PUSH = ">"


def decode_simple(recv_id: str, value: str) -> Any:
    logging.debug("new simple %s %s", recv_id, value)
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
            raise ValueError("recv unhandled simple id", recv_id, value)


def decode_redis(message: str, message_counter: int = 0) -> tuple[Any, int]:
    logging.debug("new decode %s %d", repr(message), message_counter)

    recv_id = message[message_counter]

    if recv_id in IDSimple:
        message_counter += 1
        message_next = message.find(REDIS_SEPARATOR, message_counter)
        value_next = message[message_counter:message_next]
        message_counter = message_next + REDIS_SEPARATOR_LENGTH
        return decode_simple(recv_id, value_next), message_counter

    match recv_id:
        case IDAggregate.BSTRING:
            message_counter += 1
            message_next = message.find(REDIS_SEPARATOR, message_counter)
            value_next = message[message_counter:message_next]
            message_counter = message_next + REDIS_SEPARATOR_LENGTH
            bstr_length = int(value_next)

            message_next = message.find(REDIS_SEPARATOR, message_counter)
            bstr_value = message[message_counter:message_next]
            assert message_next == message_counter + bstr_length
            message_counter = message_next + REDIS_SEPARATOR_LENGTH
            logging.debug("new bstr_value %s", bstr_value)
            return bstr_value, message_counter

        case IDAggregate.ARRAY:
            message_counter += 1
            message_next = message.find(REDIS_SEPARATOR, message_counter)
            value_next = message[message_counter:message_next]
            message_counter = message_next + REDIS_SEPARATOR_LENGTH
            array_length = int(value_next)

            array_res = []
            logging.debug(
                "new array, length %d %d %d %s",
                array_length,
                len(array_res),
                message_counter,
                message,
            )
            while len(array_res) < array_length:
                array_value, message_counter = decode_redis(message, message_counter)
                array_res.append(array_value)
                logging.debug(
                    "new array_value %d %d %d %s",
                    array_length,
                    len(array_res),
                    message_counter,
                    array_value,
                )

            return array_res, message_counter

        case IDAggregate.MAP:
            message_counter += 1
            message_next = message.find(REDIS_SEPARATOR, message_counter)
            value_next = message[message_counter:message_next]
            message_counter = message_next + REDIS_SEPARATOR_LENGTH
            map_length = int(value_next)

            map_res = {}
            logging.debug(
                "new map, length %d %d %d %s",
                map_length,
                len(map_res),
                message_counter,
                message,
            )
            while len(map_res) < map_length:
                map_key, message_counter = decode_redis(message, message_counter)
                map_value, message_counter = decode_redis(message, message_counter)
                map_res[map_key] = map_value
                logging.debug(
                    "new map item, length %d %d %d %s %s",
                    map_length,
                    len(map_res),
                    message_counter,
                    map_key,
                    map_value,
                )

            return map_res, message_counter

        case _:
            raise ValueError("unknown redis ID", recv_id)


def encode_redis(value: Any) -> str:
    logging.debug("new encode %s", str(value))
    if isinstance(value, str):
        if len(value) == 0:
            return IDAggregate.BSTRING + "-1"
        return IDAggregate.BSTRING + str(len(value)) + REDIS_SEPARATOR + value
    if isinstance(value, int):
        return IDSimple.INTEGER + str(value)
    if value is None:
        return IDSimple.NULL
    if isinstance(value, bool):
        return IDSimple.BOOLEAN + ("t" if value else "f")
    if isinstance(value, float):
        return IDSimple.DOUBLE + str(value)
    if isinstance(value, Decimal):
        return IDSimple.BIGNUM + str(value)
    if isinstance(value, list):
        header = IDAggregate.ARRAY + str(len(value))
        data = [encode_redis(array_value) for array_value in value]
        return REDIS_SEPARATOR.join([header, *data])
    if isinstance(value, dict):
        header = IDAggregate.MAP + str(len(value))
        data = [encode_redis(k) + encode_redis(v) for k, v in value.items()]
        return REDIS_SEPARATOR.join([header, *data])
    raise ValueError("unhandled redis encoding type", type(value))


def encode_data(data: bytes) -> bytes:
    if len(data) == 0:
        return (IDAggregate.BSTRING + "-1").encode()
    return (IDAggregate.BSTRING + str(len(data)) + REDIS_SEPARATOR).encode() + data
