import asyncio

from .redis import REDIS_SEPARATOR, IDSimple, IDAggregate, decode_simple
REDIS_PORT = 6379


def decode_redis(message, message_counter):
    data = message[message_counter]

    recv_id = data[0]
    value = data[1:]

    if recv_id in IDSimple:
        return decode_simple(recv_id, value), message_counter + 1

    match recv_id:
        case IDAggregate.BSTRING:
            bstr_length = int(value)
            assert len(message[message_counter+1:]) >= 1
            message_counter += 1
            bstr_value = message[message_counter]
            assert bstr_length == len(bstr_value)
            return bstr_value, message_counter + 1

        case IDAggregate.ARRAY:
            array_length = int(value)
            assert len(message[message_counter+1:]) >= array_length
            array_res = []
            for _ in range(array_length):
                message_counter += 1
                array_value, message_counter = decode_redis(message, message_counter)
                array_res.append(array_value)
            return array_res

        case _:
            print("unknown redis ID", recv_id)


async def parse_redis(message: str):
    assert message[-2:] == REDIS_SEPARATOR
    message = message[:-2]

    message = message.split(REDIS_SEPARATOR)
    print(message)

    values, msg_end = decode_redis(message, 0)
    print(msg_end, values)



async def client_connected_cb(reader, writer):
    addr = writer.get_extra_info('peername')
    print(f"[{addr!r}] New connection")

    while True:
        recv_message = (await reader.read(100)).decode()
        print(f"[{addr!r}] Recv {recv_message!r}")
        await parse_redis(recv_message)

        send_message = "+PONG\r\n"
        print(f"[{addr!r}] Send {send_message!r}")
        writer.write(send_message.encode())
        await writer.drain()

    print(f"[{addr!r}] Closing connection")
    writer.close()
    await writer.wait_closed()


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
