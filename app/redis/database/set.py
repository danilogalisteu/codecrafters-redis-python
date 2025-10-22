import logging

from app.redis.rdb.file.constants import DBType

from .data import check_key, get_data, set_data


def set_set_value(key: str, values: dict[str, float]) -> int:
    logging.info("ZADD key '%s' values %s", key, values)
    if not check_key(key):
        sorted_values = sorted(
            sorted(values.items(), key=lambda x: x[0]), key=lambda x: x[1]
        )
        set_data(key, sorted_values, dtype=DBType.ZSET)
        return len(sorted_values)

    vzset, _ = get_data(key)
    vdict = dict(vzset["value"])
    vdict.update(values)

    sorted_values = sorted(
        sorted(vdict.items(), key=lambda x: x[0]), key=lambda x: x[1]
    )
    set_data(key, sorted_values, dtype=DBType.ZSET)
    return len(sorted_values) - len(vzset["value"])


def get_set_rank(key: str, member: str) -> int | str:
    logging.info("ZRANK key '%s' member %s", key, member)
    if not check_key(key):
        return ""

    vzset, _ = get_data(key)
    vdict = dict(vzset["value"])
    if member not in vdict:
        return ""

    return list(vdict).index(member)


def get_set_range(key: str, start: int, end: int) -> list[str]:
    logging.info("ZRANGE key '%s' start %d end %d", key, start, end)
    if not check_key(key):
        return []

    vzset, _ = get_data(key)
    len_vzset = len(vzset["value"])
    if start >= len_vzset or (end >= 0 and start > end):
        return []

    if end > len_vzset:
        end = len_vzset
    elif end == -1:
        end = None
    else:
        end += 1

    print(vzset["value"])
    return [v[0] for v in vzset["value"][start : end]]
