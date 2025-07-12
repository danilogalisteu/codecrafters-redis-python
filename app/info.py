REDIS_INFO = {"replication": {"role": "master"}}


def get_info(section: str | None = None) -> str:
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
