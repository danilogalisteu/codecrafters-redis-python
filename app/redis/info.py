REDIS_INFO: dict[str, dict[str, str | int]] = {"replication": {}}


def get_info(section: str, key: str) -> str | int:
    return REDIS_INFO.get(section, {}).get(key, "")


def get_info_str(section: str | None = None) -> str:
    if section is None:
        return "\n\n".join(
            [
                f"# {section.title()}\n"
                + "\n".join(
                    [
                        f"{key}:{value}"
                        for key, value in REDIS_INFO[section.lower()].items()
                    ]
                )
                for section in REDIS_INFO
            ]
        )
    return f"# {section.title()}\n" + "\n".join(
        [f"{key}:{value}" for key, value in REDIS_INFO[section.lower()].items()]
    )


def isin_info(section: str) -> bool:
    return section.lower() in REDIS_INFO


def set_info(section: str, key: str, value: str | int) -> None:
    if section not in REDIS_INFO:
        REDIS_INFO[section] = {}
    REDIS_INFO[section][key] = value
