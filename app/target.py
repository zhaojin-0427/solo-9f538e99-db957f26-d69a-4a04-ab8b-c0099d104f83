"""分析库（目标端）。

镜像表 + sync_state 持久进度：
  phase:      idle（待发起）/ full_running（全量进行中）/ ready（增量同步中）
  high_water: 全量快照对应的源端 tx 水位
  last_tx_id: 已整笔应用到的源端 tx_id（重启续传的依据）

关键约束：每一次目标写入（全量装载 / 增量重放）都与进度更新处于同一事务，
因此“写入失败不推进进度、重试不重复生效、进程重启按持久进度接续”。
"""
import logging
from contextlib import closing

from .dbutil import connect

log = logging.getLogger("geosync.target")

SCHEMA = """
CREATE TABLE IF NOT EXISTS samples (
  sample_no  TEXT PRIMARY KEY,
  site       TEXT NOT NULL,
  applied_tx INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS results (
  sample_no  TEXT NOT NULL,
  item       TEXT NOT NULL,
  value      REAL NOT NULL,
  applied_tx INTEGER NOT NULL,
  PRIMARY KEY (sample_no, item)
);
CREATE TABLE IF NOT EXISTS sync_state (
  id               INTEGER PRIMARY KEY CHECK (id = 1),
  phase            TEXT NOT NULL,
  high_water       INTEGER,
  last_tx_id       INTEGER NOT NULL DEFAULT 0,
  full_started_at  TEXT,
  full_done_at     TEXT,
  updated_at       TEXT NOT NULL DEFAULT (datetime('now'))
);
"""

PHASE_IDLE = "idle"
PHASE_FULL = "full_running"
PHASE_READY = "ready"


def init_db(path: str) -> None:
    with closing(connect(path)) as conn:
        conn.executescript(SCHEMA)
        conn.execute(
            "INSERT OR IGNORE INTO sync_state(id, phase, last_tx_id) "
            "VALUES(1, 'idle', 0)"
        )
    log.info("分析库就绪: %s", path)


def get_state(path: str) -> dict:
    with closing(connect(path)) as conn:
        row = conn.execute("SELECT * FROM sync_state WHERE id=1").fetchone()
    return dict(row)


def request_full_sync(path: str) -> bool:
    """仅 idle 状态可发起首次全量；ready 后拒绝重复全量（防清库重来）。"""
    with closing(connect(path)) as conn:
        cur = conn.execute(
            "UPDATE sync_state SET phase=?, full_started_at=datetime('now'), "
            "updated_at=datetime('now') WHERE id=1 AND phase=?",
            (PHASE_FULL, PHASE_IDLE),
        )
        return cur.rowcount == 1


def full_load(path: str, high_water: int, samples: list[dict],
              results: list[dict]) -> None:
    """全量装载：单事务内清表 + 写入快照 + 推进水位。

    事务外（查询侧）要么看到旧数据、要么看到完整新快照，不会看到半批；
    且本阶段查询接口尚未开放（phase != ready）。
    """
    with closing(connect(path)) as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.execute("DELETE FROM results")
            conn.execute("DELETE FROM samples")
            conn.executemany(
                "INSERT INTO samples(sample_no, site, applied_tx) VALUES(?,?,?)",
                [(s["sample_no"], s["site"], high_water) for s in samples],
            )
            conn.executemany(
                "INSERT INTO results(sample_no, item, value, applied_tx) "
                "VALUES(?,?,?,?)",
                [(r["sample_no"], r["item"], r["value"], high_water)
                 for r in results],
            )
            conn.execute(
                "UPDATE sync_state SET high_water=?, "
                "last_tx_id=MAX(last_tx_id, ?), updated_at=datetime('now') "
                "WHERE id=1",
                (high_water, high_water),
            )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
    log.info("全量装载完成: %d 样品 / %d 结果, high_water=%d",
             len(samples), len(results), high_water)


def apply_tx(path: str, tx: dict) -> bool:
    """把源端一笔事务整笔重放到分析库，与进度推进同一事务提交。

    幂等：tx_id <= last_tx_id 直接跳过；操作本身为 INSERT OR REPLACE /
    DELETE，崩溃后重放同一笔不会产生重复或二次生效。
    """
    tx_id = tx["tx_id"]
    with closing(connect(path)) as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            last = conn.execute(
                "SELECT last_tx_id FROM sync_state WHERE id=1"
            ).fetchone()[0]
            if tx_id <= last:
                conn.execute("ROLLBACK")
                return False
            for op in tx["ops"]:
                if op["entity"] == "sample":
                    if op["op"] == "upsert":
                        conn.execute(
                            "INSERT INTO samples(sample_no, site, applied_tx) "
                            "VALUES(?,?,?) ON CONFLICT(sample_no) DO UPDATE SET "
                            "site=excluded.site, applied_tx=excluded.applied_tx",
                            (op["sample_no"], op["site"], tx_id),
                        )
                    elif op["op"] == "delete":
                        conn.execute("DELETE FROM samples WHERE sample_no=?",
                                     (op["sample_no"],))
                        conn.execute("DELETE FROM results WHERE sample_no=?",
                                     (op["sample_no"],))
                elif op["entity"] == "result":
                    if op["op"] == "upsert":
                        conn.execute(
                            "INSERT INTO results(sample_no, item, value, applied_tx) "
                            "VALUES(?,?,?,?) ON CONFLICT(sample_no, item) DO UPDATE SET "
                            "value=excluded.value, applied_tx=excluded.applied_tx",
                            (op["sample_no"], op["item"], op["value"], tx_id),
                        )
                    elif op["op"] == "delete":
                        conn.execute(
                            "DELETE FROM results WHERE sample_no=? AND item=?",
                            (op["sample_no"], op["item"]),
                        )
            conn.execute(
                "UPDATE sync_state SET last_tx_id=?, updated_at=datetime('now') "
                "WHERE id=1",
                (tx_id,),
            )
            conn.execute("COMMIT")
            return True
        except Exception:
            conn.execute("ROLLBACK")
            raise


def mark_ready(path: str) -> None:
    with closing(connect(path)) as conn:
        conn.execute(
            "UPDATE sync_state SET phase=?, full_done_at=datetime('now'), "
            "updated_at=datetime('now') WHERE id=1",
            (PHASE_READY,),
        )
    log.info("首次全量同步完成，分析库进入增量同步阶段（查询已开放）")


def counts(path: str) -> dict:
    with closing(connect(path)) as conn:
        return {
            "samples": conn.execute("SELECT COUNT(*) FROM samples").fetchone()[0],
            "results": conn.execute("SELECT COUNT(*) FROM results").fetchone()[0],
        }


def query_sample(path: str, sample_no: str) -> dict | None:
    """关联明细查询：样品 + 其全部化验结果。"""
    with closing(connect(path)) as conn:
        s = conn.execute(
            "SELECT sample_no, site, applied_tx FROM samples WHERE sample_no=?",
            (sample_no,),
        ).fetchone()
        if s is None:
            return None
        items = [dict(r) for r in conn.execute(
            "SELECT item, value, applied_tx FROM results "
            "WHERE sample_no=? ORDER BY item", (sample_no,),
        )]
    return {"sample_no": s["sample_no"], "site": s["site"],
            "applied_tx": s["applied_tx"], "results": items}


def list_samples(path: str, limit: int = 200) -> list[dict]:
    with closing(connect(path)) as conn:
        return [dict(r) for r in conn.execute(
            "SELECT s.sample_no, s.site, s.applied_tx, "
            "(SELECT COUNT(*) FROM results r WHERE r.sample_no=s.sample_no) "
            "AS n_results FROM samples s ORDER BY s.sample_no LIMIT ?", (limit,),
        )]


def dump_state(path: str) -> dict:
    """规范化全量导出，供对账（与作业库逐行比对）。"""
    with closing(connect(path)) as conn:
        samples = [tuple(r) for r in conn.execute(
            "SELECT sample_no, site FROM samples ORDER BY sample_no"
        )]
        results = [tuple(r) for r in conn.execute(
            "SELECT sample_no, item, value FROM results ORDER BY sample_no, item"
        )]
    return {"samples": samples, "results": results}
