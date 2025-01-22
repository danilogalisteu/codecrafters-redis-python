import asyncio
import socket  # noqa: F401


REDIS_PORT = 6379


async def client_connected_cb(reader, writer):
    while True:
        recv_message = (await reader.read(100)).decode()
        addr = writer.get_extra_info('peername')
        print(f"Received {recv_message!r} from {addr!r}")

        send_message = "+PONG\r\n"
        print(f"Send: {send_message!r}")
        writer.write(send_message.encode())
        await writer.drain()


async def run_server():
    redis_server = await asyncio.start_server(
        client_connected_cb,
        host="localhost",
        port=REDIS_PORT,
    )

    addrs = ', '.join(str(sock.getsockname()) for sock in redis_server.sockets)
    print(f'Serving on {addrs}')

    async with redis_server:
        await redis_server.serve_forever()


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # server_socket = socket.create_server(("localhost", REDIS_PORT), reuse_port=True)
    # server_socket.accept() # wait for client

    asyncio.run(run_server())

if __name__ == "__main__":
    main()
