from .config import set_config
from .database import init_db
from .info import set_info


async def setup_redis(
    dirname: str | None,
    dbfilename: str | None = None,
    is_slave: bool = False,
) -> None:
    if dirname:
        set_config("dir", dirname)
    if dbfilename:
        set_config("dbfilename", dbfilename)

    if dirname and dbfilename:
        init_db(dirname, dbfilename)

    if is_slave:
        set_info("replication", "role", "slave")
    else:
        set_info("replication", "role", "master")
        set_info(
            "replication",
            "master_replid",
            "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
        )
        set_info("replication", "master_repl_offset", 0)
