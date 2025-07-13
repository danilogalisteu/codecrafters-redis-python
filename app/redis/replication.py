import asyncio

from .resp import REDIS_SEPARATOR, encode_redis


async def send_handshake(host: str, port: int) -> None:
    reader, writer = await asyncio.open_connection(host, port)

    message = encode_redis(["PING"]) + REDIS_SEPARATOR
    print(f"Send: {message!r}")
    writer.write(message.encode())
    await writer.drain()

    data = await reader.read(100)
    print(f"Received: {data.decode()!r}")

    print("Close the connection")
    writer.close()
    await writer.wait_closed()
