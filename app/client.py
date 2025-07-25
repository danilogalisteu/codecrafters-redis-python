import logging

from app.redis import REDIS_QUIT, decode_redis, send_handshake
from lib import curio


async def run_client(cmd_queue: curio.Queue, replicaof: str, slave_port: int) -> None:
    master_host, master_port = replicaof.split(" ")
    logging.info("Connecting to master on %s:%s", master_host, master_port)

    sock = await curio.open_connection(master_host, int(master_port))
    async with sock:
        master_id, master_offset, recv_message = await send_handshake(sock, slave_port)
        logging.info("Connected to master %s:%s", master_id, master_offset)

        res_queue = curio.Queue()
        master_offset = 0
        multi_state = False
        multi_commands: list[list[str]] = []
        while True:
            if len(recv_message) > 0:
                logging.info("Master recv  %s", repr(recv_message))
                send_message = b""
                while len(recv_message) > 0:
                    command_line, parsed_length = decode_redis(recv_message)
                    if parsed_length == 0:
                        break
                    logging.info(
                        "Master command line %s (%d)",
                        str(command_line),
                        parsed_length,
                    )

                    await cmd_queue.put(
                        (
                            res_queue,
                            command_line,
                            master_offset,
                            multi_state,
                            multi_commands,
                        )
                    )
                    (
                        send_message,
                        _,
                        _,
                        send_master,
                        multi_state,
                        multi_commands,
                    ) = await res_queue.get()

                    recv_message = recv_message[parsed_length:]
                    master_offset += parsed_length

                    if send_master:
                        logging.info("Master send %s", repr(send_master))
                        await sock.sendall(send_master)

                    if send_message == REDIS_QUIT:
                        break

                if send_message == REDIS_QUIT:
                    break

            recv_message += await sock.recv(100)
            if not recv_message:
                break

        logging.info("Closing connection to master")
