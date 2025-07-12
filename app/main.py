import argparse
import asyncio
import logging

from .server import REDIS_PORT, run_server

logging.basicConfig(
    format="[%(asctime)s|%(levelname)s] %(message)s", level=logging.INFO
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="my-redis-server",
        description="a redis server implementation",
    )
    parser.add_argument("--dir")
    parser.add_argument("--dbfilename")
    parser.add_argument("--replicaof")
    parser.add_argument("--port", type=int, default=REDIS_PORT)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    asyncio.run(run_server(args.dir, args.dbfilename, args.port, args.replicaof))


if __name__ == "__main__":
    main()
