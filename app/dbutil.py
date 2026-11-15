"""SQLite 连接工具。

两库均启用 WAL：读事务（全量快照）与写事务（源端更正）互不阻塞，
保证“全量期间源端更正仍能完成”。
"""
import sqlite3


def connect(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path, timeout=30.0, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA foreign_keys=ON")
    # synchronous 保持默认 FULL：已提交的进度与数据在进程重启后不丢失。
    return conn
