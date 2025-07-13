from enum import IntEnum


RDB_NAME = "REDIS"
RDB_VERSION = "0011"


class RDBOpCode(IntEnum):
    AUX = 0xFA
    RESIZEDB = 0xFB
    EXPIRETIMEMS = 0xFC
    EXPIRETIME = 0xFD
    SELECTDB = 0xFE
    EOF = 0xFF


class RDBValue(IntEnum):
    STR = 0
    LIST = 1
    SET = 2
    SORTEDSET = 3
    HASH = 4
    ZIPMAP = 9
    ZIPLIST = 10
    INTSET = 11
    ZIPLIST_SORTEDSET = 12
    ZIPLIST_HASH = 13
    QUICKLIST_LIST = 14
