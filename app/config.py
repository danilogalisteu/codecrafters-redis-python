from .redis import encode_redis

REDIS_CONFIG = {}


def get_config(name: str) -> str:
    if name in REDIS_CONFIG:
        value = REDIS_CONFIG[name]
        return encode_redis([name, value])
    else:
        return "-ERR unknown 'CONFIG' parameter"


def set_config(name: str, value: str) -> str:
    global REDIS_CONFIG
    REDIS_CONFIG[name] = value
    return encode_redis("OK")
