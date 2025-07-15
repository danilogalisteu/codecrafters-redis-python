import asyncio

from .resp import REDIS_SEPARATOR, encode_redis


async def send_handshake(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter, slave_port: int
) -> tuple[str, int]:
    message = encode_redis(["PING"]) + REDIS_SEPARATOR
    print(f"Sending: {message!r}")
    writer.write(message.encode())
    await writer.drain()

    data = await reader.read()
    print(f"Received: {data.decode()!r}")
    assert data.decode().removesuffix(REDIS_SEPARATOR) == "+PONG"

    message = (
        encode_redis(["REPLCONF", "listening-port", str(slave_port)]) + REDIS_SEPARATOR
    )
    print(f"Sending: {message!r}")
    writer.write(message.encode())
    await writer.drain()

    data = await reader.read()
    print(f"Received: {data.decode()!r}")
    assert data.decode().removesuffix(REDIS_SEPARATOR) == "+OK"

    message = encode_redis(["REPLCONF", "capa", "psync2"]) + REDIS_SEPARATOR
    print(f"Sending: {message!r}")
    writer.write(message.encode())
    await writer.drain()

    data = await reader.read()
    print(f"Received: {data.decode()!r}")
    assert data.decode().removesuffix(REDIS_SEPARATOR) == "+OK"

    message = encode_redis(["PSYNC", "?", "-1"]) + REDIS_SEPARATOR
    print(f"Sending: {message!r}")
    writer.write(message.encode())
    await writer.drain()

    data = await reader.read()
    print(f"Received: {data!r}")

    recv_message = data.decode().removesuffix(REDIS_SEPARATOR).split(" ")
    assert recv_message[0] == "+FULLRESYNC"

    return recv_message[1], int(recv_message[2])
