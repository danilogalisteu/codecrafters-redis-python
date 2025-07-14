REDIS_CONFIG = {}


def get_config(name: str) -> str | None:
    return REDIS_CONFIG.get(name, None)


def set_config(name: str, value: str) -> None:
    REDIS_CONFIG[name] = value
