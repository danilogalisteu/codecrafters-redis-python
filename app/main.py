import argparse
import asyncio
import logging

from .server import run_server

logging.basicConfig(
    format="[%(asctime)s|%(levelname)s] %(message)s", level=logging.WARNING
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="my-redis-server",
        description="a redis server implementation",
    )
    parser.add_argument("--dir")
    parser.add_argument("--dbfilename")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    asyncio.run(run_server(args.dir, args.dbfilename))


if __name__ == "__main__":
    main()
