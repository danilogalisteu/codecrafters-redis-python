import logging

from app.redis.resp import encode_redis, encode_simple

from .data import get_current_time, get_data, set_data


def get_value(key: str) -> str | dict[str, str]:
    logging.info("GET key '%s'", key)
    data, exp = get_data(key)
    return data.get("value", "")


def increase_value(key: str) -> bytes:
    logging.info("INCR key '%s'")
    data, exp = get_data(key)
    if not data:
        set_value(key, "1")
        return encode_redis(1)

    value = get_value(key)
    try:
        res = int(value) + 1
    except ValueError:
        return encode_simple("ERR value is not an integer or out of range", True)

    set_data(key, str(res))
    return encode_redis(res)


def set_value(key: str, value: str, options: list[str] | None = None) -> bytes:
    logging.info("SET key '%s' value %s options %s", key, value, options)
    if options is None:
        options = []

    exp = None
    for opt in options:
        if opt in ["EX", "EXAT", "PX", "PXAT"]:
            idx = options.index(opt)
            if idx + 1 >= len(options):
                return encode_simple("ERR missing arguments for 'set' command", True)
            exp = int(options[idx + 1])
            if opt.startswith("EX"):
                exp *= 1000
            if not opt.endswith("AT"):
                exp += get_current_time()
            break

    set_data(key, value, exp)
    return encode_simple("OK")
