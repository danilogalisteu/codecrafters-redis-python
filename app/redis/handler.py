import logging

from .config import get_config, set_config
from .database import get_keys, get_value, save_db, set_value
from .info import get_info, get_info_str, isin_info
from .resp import REDIS_SEPARATOR, decode_redis, encode_redis

REDIS_QUIT = REDIS_SEPARATOR + REDIS_SEPARATOR


def handle_redis(recv_message: str) -> tuple[int, str, bool, str]:
    command_line, parsed_length = decode_redis(recv_message)
    logging.debug(
        "buffer %d %d %s",
        parsed_length,
        len(recv_message),
        recv_message,
    )
    logging.info(
        "Command line %s",
        str(command_line),
    )
    command = command_line[0].upper()
    arguments = command_line[1:] if len(command_line) > 1 else []

    is_replica = False
    send_message = ""
    send_replica = ""
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
                send_replica = encode_redis(command_line) + REDIS_SEPARATOR
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
                        value = get_config(name)
                        if value is not None:
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
                        set_config(name, value)
                        send_message = encode_redis("OK")
                else:
                    send_message = "-ERR unhandled 'CONFIG' option"
        case "SAVE":
            save_db(get_config("dir"), get_config("dbfilename"))
            send_message = encode_redis("OK")
        case "KEYS":
            if len(arguments) != 1:
                send_message = "-ERR wrong number of arguments for 'KEYS' command"
            else:
                res = get_keys(arguments[0])
                send_message = encode_redis(res)
        case "INFO":
            if len(arguments) == 0:
                send_message = encode_redis(get_info_str())
            elif all(isin_info(section) for section in arguments):
                send_message = encode_redis(
                    "\n\n".join([get_info_str(section) for section in arguments])
                )
            else:
                send_message = "-ERR unknown section for 'INFO' command"
        case "REPLCONF":
            if len(arguments) == 0:
                send_message = "+OK"
            elif arguments[0].upper() == "GETACK":
                send_replica = encode_redis(["REPLCONF", "ACK", "0"]) + REDIS_SEPARATOR
        case "PSYNC":
            repl_id = get_info("replication", "master_replid")
            if repl_id == "":
                send_message = "-ERR not master"
            send_message = f"+FULLRESYNC {repl_id} 0"
            is_replica = True
        case _:
            logging.info("unhandled command %s", command)

    return parsed_length, send_message + REDIS_SEPARATOR, is_replica, send_replica
