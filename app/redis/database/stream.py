import asyncio
import logging

from app.redis.resp import encode_redis, encode_simple

from .data import (
    check_key,
    get_current_time,
    get_data,
    set_stream_data,
)


def get_stream_last(key: str) -> str:
    logging.info("XRANGE key '%s' $")
    data, _ = get_data(key)
    if not data:
        return ""

    last_time = list(data["value"])[-1]
    last_seq = list(data["value"][last_time])[-1]
    return f"{last_time}-{last_seq}"


def get_stream_range(
    key: str, start: str, end: str, left_closed: bool = True
) -> list[list[str | list[str]]]:
    logging.info(
        "XRANGE key '%s' start %s end %s left_closed %s", key, start, end, left_closed
    )
    data, exp = get_data(key)
    if not data:
        return ""

    startTime, startSeq = (
        (0, 1)
        if start == "-"
        else map(int, start.split("-", maxsplit=1))
        if "-" in start
        else (int(start), 0)
    )
    endTime, endSeq = (
        (2**64 - 1, 2**64 - 1)
        if end == "+"
        else map(int, end.split("-", maxsplit=1))
        if "-" in end
        else (int(end), 2**64 - 1)
    )

    return [
        [f"{ktime}-{kseq}", [item for kv in kval.items() for item in kv]]
        for ktime, kdict in data["value"].items()
        for kseq, kval in kdict.items()
        if (
            (ktime > startTime)
            or (
                (ktime == startTime)
                and ((kseq >= startSeq) if left_closed else (kseq > startSeq))
            )
        )
        and ((ktime < endTime) or ((ktime == endTime) and (kseq <= endSeq)))
    ]


async def get_stream_values(
    args: dict[str, str],
    block_time: int | None = None,
) -> list[list[str | list[list[str, list[str]]]]]:
    logging.info("XREAD keys %s block_time %d", args, block_time)
    data = []
    last_id = {}
    for key, start in args.items():
        if start == "$":
            last_id[key] = get_stream_last(key)
        else:
            values = get_stream_range(key, start, "+", False)
            if values:
                data.append([key, values])
    if (len(data) > 0) or (block_time is None):
        return data

    while len(data) == 0:
        await asyncio.sleep(block_time / 1000.0)
        for key, start in args.items():
            if start == "$":
                values = get_stream_range(key, last_id[key], "+", False)
            else:
                values = get_stream_range(key, start, "+", False)
            if values:
                data.append([key, values])
        if block_time > 0:
            break

    return data if len(data) > 0 else ""


def set_stream_value(key: str, kid: str, values: dict[str, str]) -> bytes:
    logging.info("XADD key '%s' id '%s' values %s", key, kid, values)

    if kid == "*":
        millisecondsTime = get_current_time()
        sequenceNumber = 0
    elif "-" in kid:
        millisecondsTime, sequenceNumber = kid.split("-", maxsplit=1)
        millisecondsTime = int(millisecondsTime)
        if sequenceNumber == "*":
            sequenceNumber = -1 if check_key(key) else 1 if millisecondsTime == 0 else 0
        else:
            sequenceNumber = int(sequenceNumber)
            if (millisecondsTime == 0) and (sequenceNumber == 0):
                return encode_simple(
                    "ERR The ID specified in XADD must be greater than 0-0", True
                )
    else:
        return encode_simple("ERR invalid id format", True)

    if not check_key(key):
        set_stream_data(key, millisecondsTime, sequenceNumber, values)
        return encode_redis(f"{millisecondsTime}-{sequenceNumber}")

    data, _ = get_data(key)
    latestTime = max(data["value"].keys())

    error_id = "ERR The ID specified in XADD is equal or smaller than the target stream top item"
    if millisecondsTime < latestTime:
        return encode_simple(error_id, True)

    if millisecondsTime == latestTime:
        latestSeq = max(data["value"][millisecondsTime].keys())
        if sequenceNumber == -1:
            sequenceNumber = latestSeq + 1
        elif sequenceNumber <= latestSeq:
            return encode_simple(error_id, True)
    elif sequenceNumber == -1:
        sequenceNumber = 0

    set_stream_data(key, millisecondsTime, sequenceNumber, values)
    return encode_redis(f"{millisecondsTime}-{sequenceNumber}")
