import logging
from decimal import Decimal
from enum import StrEnum
from typing import Any

REDIS_SEPARATOR = b"\r\n"
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


def decode_simple(recv_id: int, value: bytes) -> Any:
    logging.debug("new simple %s %s", recv_id, value)
    match chr(recv_id):
        case IDSimple.STRING:
            return value.decode()
        case IDSimple.INTEGER:
            return int(value.decode())
        case IDSimple.NULL:
            assert value == b""
            return None
        case IDSimple.BOOLEAN:
            assert len(value) == 1
            assert value in b"tf"
            return value == b"t"
        case IDSimple.DOUBLE:
            return float(value.decode())
        case IDSimple.BIGNUM:
            return Decimal(value.decode())
        case _:
            raise ValueError("recv unhandled simple id", recv_id, value)


def decode_redis(message: bytes, message_counter: int = 0) -> tuple[Any, int]:
    logging.debug("new decode %s %d", repr(message), message_counter)
    if len(message) < message_counter + 2:
        return "", message_counter

    recv_id = message[message_counter]

    if chr(recv_id) in IDSimple:
        message_next = message.find(REDIS_SEPARATOR, message_counter + 1)
        if message_next == -1:
            return "", message_counter
        value_next = message[message_counter + 1 : message_next]
        return decode_simple(recv_id, value_next), message_next + REDIS_SEPARATOR_LENGTH

    match chr(recv_id):
        case IDAggregate.BSTRING:
            message_next = message.find(REDIS_SEPARATOR, message_counter + 1)
            if message_next == -1:
                return "", message_counter
            bstr_length = int(message[message_counter + 1 : message_next])
            value_counter = message_next + REDIS_SEPARATOR_LENGTH

            message_next = message.find(REDIS_SEPARATOR, value_counter)
            if message_next == -1:
                return "", message_counter
            bstr_value = message[value_counter:message_next].decode()
            assert message_next == value_counter + bstr_length
            logging.debug("new bstr_value %s", bstr_value)
            return bstr_value, message_next + REDIS_SEPARATOR_LENGTH

        case IDAggregate.ARRAY:
            message_next = message.find(REDIS_SEPARATOR, message_counter + 1)
            if message_next == -1:
                return [], message_counter
            array_length = int(message[message_counter + 1 : message_next])
            message_next += REDIS_SEPARATOR_LENGTH

            array_res = []
            logging.debug(
                "new array, length %d %d %d %s",
                array_length,
                len(array_res),
                message_next,
                message,
            )
            while len(array_res) < array_length:
                array_value, message_next_val = decode_redis(message, message_next)
                if message_next_val == message_next:
                    return [], message_counter
                message_next = message_next_val
                array_res.append(array_value)
                logging.debug(
                    "new array_value %d %d %d %s",
                    array_length,
                    len(array_res),
                    message_next,
                    array_value,
                )

            return array_res, message_next

        case IDAggregate.MAP:
            message_next = message.find(REDIS_SEPARATOR, message_counter + 1)
            if message_next == -1:
                return {}, message_counter
            map_length = int(message[message_counter + 1 : message_next])
            message_next += REDIS_SEPARATOR_LENGTH

            map_res = {}
            logging.debug(
                "new map, length %d %d %d %s",
                map_length,
                len(map_res),
                message_counter,
                message,
            )
            while len(map_res) < map_length:
                map_key, message_next_val = decode_redis(message, message_next)
                if message_next_val == message_next:
                    return {}, message_counter
                message_next = message_next_val
                map_value, message_next_val = decode_redis(message, message_next)
                if message_next_val == message_next:
                    return {}, message_counter
                message_next = message_next_val
                map_res[map_key] = map_value
                logging.debug(
                    "new map item, length %d %d %d %s %s",
                    map_length,
                    len(map_res),
                    message_counter,
                    map_key,
                    map_value,
                )

            return map_res, message_next

        case _:
            raise ValueError("unknown redis ID", recv_id)


def encode_simple(value: Any, is_error: bool = False) -> bytes:
    if isinstance(value, str):
        return (
            (IDSimple.ERROR if is_error else IDSimple.STRING) + value
        ).encode() + REDIS_SEPARATOR
    if isinstance(value, int):
        return (IDSimple.INTEGER + str(value)).encode() + REDIS_SEPARATOR
    if value is None:
        return IDSimple.NULL.encode() + REDIS_SEPARATOR
    if isinstance(value, bool):
        return (IDSimple.BOOLEAN + ("t" if value else "f")).encode() + REDIS_SEPARATOR
    if isinstance(value, float):
        return (IDSimple.DOUBLE + str(value)).encode() + REDIS_SEPARATOR
    if isinstance(value, Decimal):
        return (IDSimple.BIGNUM + str(value)).encode() + REDIS_SEPARATOR
    raise ValueError("unhandled redis encoding type", type(value))


def encode_redis(value: Any, nil: bool = True) -> bytes:
    logging.debug("new encode %s", str(value))
    if isinstance(value, str):
        if len(value) == 0:
            if nil:
                return (IDAggregate.BSTRING + "-1").encode() + REDIS_SEPARATOR
            return (
                (IDAggregate.BSTRING + "0").encode() + REDIS_SEPARATOR + REDIS_SEPARATOR
            )
        return (
            (IDAggregate.BSTRING + str(len(value))).encode()
            + REDIS_SEPARATOR
            + value.encode()
            + REDIS_SEPARATOR
        )
    if isinstance(value, list):
        if len(value) == 0:
            if nil:
                return (IDAggregate.ARRAY + "-1").encode() + REDIS_SEPARATOR
            return (
                (IDAggregate.ARRAY + "0").encode() + REDIS_SEPARATOR
            )
        header = (IDAggregate.ARRAY + str(len(value))).encode() + REDIS_SEPARATOR
        data = [encode_redis(array_value, nil=nil) for array_value in value]
        return b"".join([header, *data])
    if isinstance(value, dict):
        if len(value) == 0:
            if nil:
                return (IDAggregate.MAP + "-1").encode() + REDIS_SEPARATOR
            return (IDAggregate.MAP + "0").encode() + REDIS_SEPARATOR
        header = (IDAggregate.MAP + str(len(value))).encode() + REDIS_SEPARATOR
        data = [encode_redis(k) + encode_redis(v, nil=nil) for k, v in value.items()]
        return b"".join([header, *data])
    return encode_simple(value)
