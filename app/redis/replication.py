import asyncio

from .resp import REDIS_SEPARATOR, encode_redis


async def send_handshake(master_host: str, master_port: int, slave_port: int) -> None:
    reader, writer = await asyncio.open_connection(master_host, master_port)

    message = encode_redis(["PING"]) + REDIS_SEPARATOR
    print(f"Sending: {message!r}")
    writer.write(message.encode())
    await writer.drain()

    data = await reader.read(100)
    print(f"Received: {data.decode()!r}")

    message = (
        encode_redis(["REPLCONF", "listening-port", str(slave_port)]) + REDIS_SEPARATOR
    )
    print(f"Sending: {message!r}")
    writer.write(message.encode())
    await writer.drain()

    data = await reader.read(100)
    print(f"Received: {data.decode()!r}")

    message = encode_redis(["REPLCONF", "capa", "psync2"]) + REDIS_SEPARATOR
    print(f"Sending: {message!r}")
    writer.write(message.encode())
    await writer.drain()

    data = await reader.read(100)
    print(f"Received: {data.decode()!r}")

    print("Close the connection")
    writer.close()
    await writer.wait_closed()
