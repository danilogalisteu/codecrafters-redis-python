# RDB Persistence

Welcome to the RDB Persistence Extension! In this extension, you'll add support for reading [RDB files](https://redis.io/docs/management/persistence/) (Redis Database files).

## RDB file format overview

Here are the different sections of the RDB file, in order:

- Header section
  - Magic string: "REDIS"
  - Version number: "0003"
- Metadata section (each record)
  - Start: 0xFA
  - Name: string encoded
  - Value: string encoded
- Database section (each database)
  - Start: 0xFE
  - DB index: 1 byte
  - Hash table size:
    - Start: 0xFB
    - Size of keys: length encoded
    - Size of expirations: length encoded
  - Record (each)
    - Type: 1 byte
    - [optional] Expiration (UNIX time)
      - Start: 0xFC (time in ms) or 0xFD (time in s)
      - Value: length encoded, 8 bytes (in ms) or 4bytes (in s)
    - Key: string encoded
    - Value
- End of file section
  - Start: 0xFF
  - Checksum: CRC-64 REDIS (calculated including start 0xFF)

## References

[RDB File Format](https://rdb.fnordig.de/file_format.html) by Jan-Erik Rediger.
