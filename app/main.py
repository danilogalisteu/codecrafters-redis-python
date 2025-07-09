import asyncio
import logging
from time import time_ns

logging.basicConfig(level=logging.WARNING)

from .redis import REDIS_SEPARATOR, decode_redis, encode_redis


REDIS_PORT = 6379
REDIS_DB = {}


async def parse_redis(message: str):
    logging.debug(f"new parse {message!r}")
    return decode_redis(message)


async def client_connected_cb(reader, writer):
    addr = writer.get_extra_info('peername')
    logging.info(f"[{addr!r}] New connection")

    recv_message = ""
    while True:
        await asyncio.sleep(0.1)
        recv_message += (await reader.read(100)).decode()

        if len(recv_message) > 0:
            logging.info(f"[{addr!r}] Recv {recv_message!r}")
            command_line, parsed_length = await parse_redis(recv_message)
            logging.debug("buffer %d %d %s", parsed_length, len(recv_message), recv_message)
            recv_message = recv_message[parsed_length:]
            logging.debug("new buffer %d %d %s", 0, len(recv_message), recv_message)

            logging.info(f"[{addr!r}] Command line {command_line} ({parsed_length})")
            command = command_line[0]
            arguments = command_line[1:] if len(command_line) > 0 else []

            send_message = ""
            match command.upper():
                case "QUIT":
                    break
                case "PING":
                    if arguments:
                        if len(arguments) == 1:
                            send_message = encode_redis(arguments[0])
                        else:
                            send_message = "-ERR wrong number of arguments for 'ping' command"
                    else:
                        send_message = "+PONG"
                case "ECHO":
                    if len(arguments) == 1:
                        send_message = encode_redis(arguments[0])
                    else:
                        send_message = "-ERR wrong number of arguments for 'echo' command"
                case "SET":
                    if len(arguments) < 2:
                        send_message = "-ERR wrong number of arguments for 'set' command"
                    else:
                        set_key = arguments[0]
                        set_value = arguments[1]
                        set_dict = {"value": set_value, "exp": None}
                        if len(arguments) == 2:
                            REDIS_DB[set_key] = set_dict
                            send_message = encode_redis("OK")
                        else:
                            options = arguments[2:]
                            opt_counter = 0
                            while opt_counter < len(options):
                                opt_cmd = options[opt_counter].upper()
                                match opt_cmd:
                                    case "EX":
                                        opt_counter += 1
                                        if opt_counter < len(options):
                                            opt_arg = int(options[opt_counter])
                                            set_time = time_ns()
                                            set_dict["exp"] = set_time + opt_arg * 1e9
                                            REDIS_DB[set_key] = set_dict
                                            send_message = encode_redis("OK")
                                            opt_counter += 1
                                    case "EXAT":
                                        opt_counter += 1
                                        if opt_counter < len(options):
                                            opt_arg = int(options[opt_counter])
                                            set_dict["exp"] = opt_arg * 1e9
                                            REDIS_DB[set_key] = set_dict
                                            send_message = encode_redis("OK")
                                            opt_counter += 1
                                    case "PX":
                                        opt_counter += 1
                                        if opt_counter < len(options):
                                            opt_arg = int(options[opt_counter])
                                            set_time = time_ns()
                                            set_dict["exp"] = set_time + opt_arg * 1e6
                                            REDIS_DB[set_key] = set_dict
                                            send_message = encode_redis("OK")
                                            opt_counter += 1
                                    case "PXAT":
                                        opt_counter += 1
                                        if opt_counter < len(options):
                                            opt_arg = int(options[opt_counter])
                                            set_dict["exp"] = opt_arg * 1e6
                                            REDIS_DB[set_key] = set_dict
                                            send_message = encode_redis("OK")
                                            opt_counter += 1
                                    case _:
                                        logging.warning("unhandled option %s", opt_cmd)

                case "GET":
                    if len(arguments) != 1:
                        send_message = "-ERR wrong number of arguments for 'get' command"
                    else:
                        get_key = arguments[0]
                        get_dict = REDIS_DB.get(get_key, {})
                        get_exp = get_dict.get("exp", None)
                        if get_exp is not None:
                            get_time = time_ns()
                            if get_exp < get_time:
                                del REDIS_DB[get_key]
                                send_message = encode_redis("")
                            else:
                                get_value = get_dict.get("value", "")
                                send_message = encode_redis(get_value)
                        else:
                            get_value = get_dict.get("value", "")
                            send_message = encode_redis(get_value)
                case _:
                    logging.warning("unhandled command %s", command)

            send_message += REDIS_SEPARATOR
            logging.info(f"[{addr!r}] Send {send_message!r}")
            writer.write(send_message.encode())
            await writer.drain()

    logging.info(f"[{addr!r}] Closing connection")
    writer.close()
    await writer.wait_closed()


async def run_server():
    redis_server = await asyncio.start_server(
        client_connected_cb,
        host="localhost",
        port=REDIS_PORT,
    )

    addrs = ', '.join(str(sock.getsockname()) for sock in redis_server.sockets)
    logging.info(f'Serving on {addrs}')

    async with redis_server:
        await redis_server.serve_forever()


def main():
    asyncio.run(run_server())

if __name__ == "__main__":
    main()
