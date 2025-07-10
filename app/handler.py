import logging

from .database import get_value, set_value
from .redis import REDIS_SEPARATOR, decode_redis, encode_redis

REDIS_CONFIG = {}
REDIS_QUIT = REDIS_SEPARATOR + REDIS_SEPARATOR


def handle_redis(recv_message: str) -> tuple[int, str]:
    command_line, parsed_length = decode_redis(recv_message)
    logging.debug(
        "buffer %d %d %s",
        parsed_length,
        len(recv_message),
        recv_message,
    )
    logging.debug("new buffer %d %d %s", 0, len(recv_message), recv_message)

    logging.info(
        "Command line %s (%d)",
        str(command_line),
        parsed_length,
    )
    command = command_line[0].upper()
    arguments = command_line[1:] if len(command_line) > 1 else []

    send_message = ""
    match command:
        case "QUIT":
            send_message = REDIS_SEPARATOR
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
                res = set_value(
                    arguments[0],
                    arguments[1],
                    [arg.upper() for arg in arguments[2:]],
                )
                send_message = encode_redis(res)
        case "GET":
            if len(arguments) != 1:
                send_message = "-ERR wrong number of arguments for 'get' command"
            else:
                res = get_value(arguments[0])
                send_message = encode_redis(res)
        case "CONFIG":
            if len(arguments) < 1:
                send_message = "-ERR missing arguments for 'CONFIG' command"
            else:
                option = arguments[0].upper()
                if option == "GET":
                    if len(arguments) < 2:
                        send_message = (
                            "-ERR missing parameters for 'CONFIG GET' command"
                        )
                    else:
                        name = arguments[1]
                        if name in REDIS_CONFIG:
                            value = REDIS_CONFIG[name]
                            send_message = encode_redis([name, value])
                        else:
                            send_message = "-ERR unknown 'CONFIG' parameter"
                elif option == "SET":
                    if len(arguments) < 3:
                        send_message = (
                            "-ERR missing parameters for 'CONFIG SET' command"
                        )
                    else:
                        name = arguments[1]
                        value = arguments[2]
                        REDIS_CONFIG[name] = value
                        send_message = "OK"
                else:
                    send_message = "-ERR unhandled 'CONFIG' option"
        case _:
            logging.info("unhandled command %s", command)

    return parsed_length, send_message + REDIS_SEPARATOR
