import logging

from .data import check_key, get_data, set_data


def set_list_value(key: str, value: str) -> int:
    logging.info("RPUSH key '%s' value %s", key, value)
    if not check_key(key):
        set_data(key, [value])
        return 1

    vlist, _ = get_data(key)
    vlist.append(value)

    set_data(key, vlist)
    return len(vlist)
