import logging

from lib import curio

REDIS_SUBS: dict[str, set[curio.Queue]] = {}


async def sub_channel(channel: str, sub_queue: curio.Queue) -> None:
    logging.info("Channel %s: sub %s", channel, sub_queue)
    if channel not in REDIS_SUBS:
        REDIS_SUBS[channel] = set()
    REDIS_SUBS[channel].add(sub_queue)


async def unsub_channel(channel: str, sub_queue: curio.Queue) -> None:
    logging.info("Channel %s: unsub %s", channel, sub_queue)
    if channel in REDIS_SUBS:
        REDIS_SUBS[channel].discard(sub_queue)


async def get_clients(channel: str) -> int:
    logging.info("Channel %s: %d clients", channel, len(REDIS_SUBS.get(channel, set())))
    return len(REDIS_SUBS.get(channel, set()))


async def pub_message(channel: str, message: bytes) -> None:
    logging.info("Channel %s: pub %s", channel, str(message))
    for sub_queue in REDIS_SUBS.get(channel, set()):
        await sub_queue.put(message)
