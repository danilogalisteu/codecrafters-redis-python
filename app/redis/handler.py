import logging

from .config import get_config, set_config
from .database import (
    get_keys,
    get_stream_range,
    get_stream_values,
    get_type,
    get_value,
    increase_value,
    save_db,
    set_stream_value,
    set_value,
)
from .info import get_info, get_info_str, isin_info
from .resp import REDIS_SEPARATOR, decode_redis, encode_redis, encode_simple
from .slave import wait_slaves

REDIS_QUIT = REDIS_SEPARATOR + REDIS_SEPARATOR


async def handle_redis(
    recv_message: bytes,
    master_offset: int = 0,
    multi_state: bool = False,
    multi_commands: list[list[str]] | None = None,
) -> tuple[int, bytes, bool, bytes, bytes, bool, list[list[str]]]:
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
    if parsed_length == 0:
        return (
            parsed_length,
            REDIS_SEPARATOR,
            False,
            b"",
            b"",
            multi_state,
            multi_commands,
        )

    command = command_line[0].upper()
    arguments = command_line[1:] if len(command_line) > 1 else []

    is_replica = False
    send_message = b""
    send_replica = b""
    send_master = b""
    match command:
        case "QUIT":
            send_message = REDIS_QUIT
        case "PING":
            if arguments:
                if len(arguments) == 1:
                    send_message = encode_redis(arguments[0])
                else:
                    send_message = encode_simple(
                        "ERR wrong number of arguments for 'ping' command", True
                    )
            else:
                send_message = encode_simple("PONG")
        case "ECHO":
            if len(arguments) == 1:
                send_message = encode_redis(arguments[0])
            else:
                send_message = encode_simple(
                    "ERR wrong number of arguments for 'echo' command", True
                )
        case "SET":
            if len(arguments) < 2:
                send_message = encode_simple(
                    "ERR wrong number of arguments for 'set' command", True
                )
            elif multi_state:
                multi_commands.append(command_line)
                send_message = encode_simple("QUEUED")
                send_replica = encode_redis(command_line)
            else:
                send_message = set_value(
                    arguments[0],
                    arguments[1],
                    [arg.upper() for arg in arguments[2:]],
                )
                send_replica = encode_redis(command_line)
        case "GET":
            if len(arguments) != 1:
                send_message = encode_simple(
                    "ERR wrong number of arguments for 'get' command", True
                )
            elif multi_state:
                multi_commands.append(command_line)
                send_message = encode_simple("QUEUED")
            else:
                res = get_value(arguments[0])
                send_message = encode_redis(res)
        case "INCR":
            if len(arguments) != 1:
                send_message = encode_simple(
                    "ERR wrong number of arguments for 'INCR' command", True
                )
            elif multi_state:
                multi_commands.append(command_line)
                send_message = encode_simple("QUEUED")
            else:
                send_message = increase_value(arguments[0])
        case "TYPE":
            if len(arguments) != 1:
                send_message = encode_simple(
                    "ERR wrong number of arguments for 'TYPE' command", True
                )
            elif multi_state:
                multi_commands.append(command_line)
                send_message = encode_simple("QUEUED")
            else:
                send_message = encode_simple(get_type(arguments[0]))
        case "MULTI":
            if len(arguments) != 0:
                send_message = encode_simple(
                    "ERR wrong number of arguments for 'MULTI' command", True
                )
            elif multi_state:
                multi_commands.append(command_line)
                send_message = encode_simple("QUEUED")
            else:
                multi_state = True
                multi_commands = []
                send_message = encode_simple("OK")
        case "EXEC":
            if len(arguments) != 0:
                send_message = encode_simple(
                    "ERR wrong number of arguments for 'EXEC' command", True
                )
            elif multi_state:
                # TODO run transaction
                multi_state = False
                multi_commands = []
                send_message = encode_redis([])
            else:
                send_message = encode_simple("ERR EXEC without MULTI", True)
        case "XADD":
            if (len(arguments) < 2) or (len(arguments) % 2 != 0):
                send_message = encode_simple(
                    "ERR wrong number of arguments for 'XADD' command", True
                )
            elif multi_state:
                multi_commands.append(command_line)
                send_message = encode_simple("QUEUED")
                send_replica = encode_redis(command_line)
            else:
                values = dict(zip(arguments[2::2], arguments[3::2], strict=True))
                send_message = set_stream_value(arguments[0], arguments[1], values)
                send_replica = encode_redis(command_line)
        case "XRANGE":
            if len(arguments) < 3:
                send_message = encode_simple(
                    "ERR wrong number of arguments for 'XRANGE' command", True
                )
            elif multi_state:
                multi_commands.append(command_line)
                send_message = encode_simple("QUEUED")
            else:
                send_message = encode_redis(
                    get_stream_range(arguments[0], arguments[1], arguments[2])
                )
        case "XREAD":
            if len(arguments) < 2:
                send_message = encode_simple(
                    "ERR wrong number of arguments for 'XREAD' command", True
                )
            elif multi_state:
                multi_commands.append(command_line)
                send_message = encode_simple("QUEUED")
            else:
                block_time = None
                if arguments[0].upper() == "BLOCK":
                    block_time = int(arguments[1])
                    arguments = arguments[2:]

                if arguments[0].upper() == "STREAMS":
                    arguments = arguments[1:]
                    if len(arguments) % 2 == 0:
                        middle = int(len(arguments) / 2)
                        args = dict(
                            zip(
                                arguments[:middle],
                                arguments[middle:],
                                strict=True,
                            )
                        )
                        send_message = encode_redis(
                            await get_stream_values(args, block_time)
                        )
                    else:
                        send_message = encode_simple(
                            "ERR odd number of key/id pairs", True
                        )
                else:
                    send_message = encode_simple(
                        f"ERR unhandled option {arguments[0]}", True
                    )
        case "CONFIG":
            if len(arguments) < 1:
                send_message = encode_simple(
                    "ERR missing arguments for 'CONFIG' command", True
                )
            else:
                option = arguments[0].upper()
                if option == "GET":
                    if len(arguments) < 2:
                        send_message = encode_simple(
                            "ERR missing parameters for 'CONFIG GET' command", True
                        )
                    else:
                        name = arguments[1]
                        value = get_config(name)
                        if value is not None:
                            send_message = encode_redis([name, value])
                        else:
                            send_message = encode_simple(
                                "ERR unknown 'CONFIG' parameter", True
                            )
                elif option == "SET":
                    if len(arguments) < 3:
                        send_message = encode_simple(
                            "ERR missing parameters for 'CONFIG SET' command", True
                        )
                    else:
                        name = arguments[1]
                        value = arguments[2]
                        set_config(name, value)
                        send_message = encode_redis("OK")
                else:
                    send_message = encode_simple("ERR unhandled 'CONFIG' option", True)
        case "SAVE":
            if multi_state:
                multi_commands.append(command_line)
                send_message = encode_simple("QUEUED")
            else:
                save_db(get_config("dir"), get_config("dbfilename"))
                send_message = encode_redis("OK")
        case "KEYS":
            if len(arguments) != 1:
                send_message = encode_simple(
                    "ERR wrong number of arguments for 'KEYS' command", True
                )
            elif multi_state:
                multi_commands.append(command_line)
                send_message = encode_simple("QUEUED")
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
                send_message = encode_simple(
                    "ERR unknown section for 'INFO' command", True
                )
        case "REPLCONF":
            if len(arguments) < 1:
                send_message = encode_simple(
                    "ERR wrong number of arguments for 'REPLCONF' command", True
                )
            elif multi_state:
                multi_commands.append(command_line)
                send_message = encode_simple("QUEUED")
            elif arguments[0].upper() == "GETACK":
                send_master = encode_redis(["REPLCONF", "ACK", str(master_offset)])
            else:
                send_message = encode_simple("OK")
        case "PSYNC":
            if multi_state:
                multi_commands.append(command_line)
                send_message = encode_simple("QUEUED")
            else:
                repl_id = get_info("replication", "master_replid")
                if repl_id == "":
                    send_message = encode_simple("ERR not master", True)
                send_message = encode_simple(f"FULLRESYNC {repl_id} 0")
                is_replica = True
        case "WAIT":
            if len(arguments) < 2:
                send_message = encode_simple(
                    "ERR wrong number of arguments for 'WAIT' command", True
                )
            elif multi_state:
                multi_commands.append(command_line)
                send_message = encode_simple("QUEUED")
            else:
                exp_slaves = int(arguments[0])
                timeout_ms = int(arguments[1])
                num_slaves = await wait_slaves(exp_slaves, timeout_ms)
                send_message = encode_redis(num_slaves)
        case _:
            logging.info("unhandled command %s", command)

    return (
        parsed_length,
        send_message,
        is_replica,
        send_replica,
        send_master,
        multi_state,
        multi_commands,
    )
