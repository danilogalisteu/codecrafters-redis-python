from lib import curio

REDIS_SUBS: dict[str, set[curio.io.Socket]] = {}


def sub_channel(channel: str, client: curio.io.Socket) -> None:
    if channel not in REDIS_SUBS:
        REDIS_SUBS[channel] = set()
    REDIS_SUBS[channel].add(client)


def get_clients(channel: str) -> int:
    return len(REDIS_SUBS.get(channel, set()))


async def pub_message(channel: str, message: bytes) -> None:
    for client in REDIS_SUBS.get(channel, set()):
        await client.sendall(message)
