import argparse
import asyncio
import logging

from .server import run_server

logging.basicConfig(
    format="[%(asctime)s|%(levelname)s] %(message)s", level=logging.WARNING
)


def main() -> None:
    asyncio.run(run_server())


if __name__ == "__main__":
    main()
