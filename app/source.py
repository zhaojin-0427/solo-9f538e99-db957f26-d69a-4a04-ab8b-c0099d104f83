"""作业库（本地示例源）。

业务表：
  samples(sample_no, site)            样品：样品号、采样点
  results(sample_no, item, value)     化验结果：样品号、项目、检测值

变更日志（与业务写入同一事务提交）：
  tx_log(tx_id, note, created_at)     一行 = 源端一笔已提交事务
  tx_ops(tx_id, seq, entity, op, ...) 该事务内的有序操作流水

tx_id 使用普通 INTEGER PRIMARY KEY（rowid 别名，非 AUTOINCREMENT）：
SQLite 单写者串行提交，rowid 顺序即源端提交顺序；回滚事务不占用号段，
因此已提交的 tx_id 连续无空洞，增量端可按 tx_id 严格依序追赶。
"""
import logging
import random
import threading
from contextlib import closing

from .dbutil import connect

log = logging.getLogger("geosync.source")

SCHEMA = """
CREATE TABLE IF NOT EXISTS samples (
  sample_no  TEXT PRIMARY KEY,
  site       TEXT NOT NULL,
  updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE TABLE IF NOT EXISTS results (
  id        INTEGER PRIMARY KEY,
  sample_no TEXT NOT NULL REFERENCES samples(sample_no),
  item      TEXT NOT NULL,
  value     REAL NOT NULL,
  UNIQUE(sample_no, item)
);
CREATE TABLE IF NOT EXISTS tx_log (
  tx_id      INTEGER PRIMARY KEY,
  note       TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE TABLE IF NOT EXISTS tx_ops (
  tx_id     INTEGER NOT NULL REFERENCES tx_log(tx_id),
  seq       INTEGER NOT NULL,
  entity    TEXT NOT NULL,          -- 'sample' | 'result'
  op        TEXT NOT NULL,          -- 'upsert' | 'delete'
  sample_no TEXT NOT NULL,
  item      TEXT,
  site      TEXT,
  value     REAL,
  PRIMARY KEY (tx_id, seq)
);
CREATE INDEX IF NOT EXISTS idx_results_sample ON results(sample_no);
"""

ITEMS = ["Au", "Ag", "Cu", "Pb", "Zn", "Fe"]

# 进程内写锁：串行化演示端发起的更正事务（SQLite 本身亦单写者，双保险）。
_write_lock = threading.Lock()


def init_db(path: str) -> None:
    with closing(connect(path)) as conn:
        conn.executescript(SCHEMA)
    log.info("作业库就绪: %s", path)


def seed_if_empty(path: str, n_samples: int = 20, rng: random.Random | None = None) -> int:
    """空库时写入一批示例样品与化验结果（基线数据，由首次全量装载）。"""
    rng = rng or random.Random(20241115)
    with _write_lock, closing(connect(path)) as conn:
        cnt = conn.execute("SELECT COUNT(*) FROM samples").fetchone()[0]
        if cnt:
            return 0
        conn.execute("BEGIN IMMEDIATE")
        try:
            for i in range(1, n_samples + 1):
                no = f"YT-{i:04d}"
                site = f"ZK-{rng.randint(1, 12):03d}"
                conn.execute(
                    "INSERT INTO samples(sample_no, site) VALUES(?, ?)", (no, site)
                )
                for item in rng.sample(ITEMS, k=rng.randint(3, len(ITEMS))):
                    conn.execute(
                        "INSERT INTO results(sample_no, item, value) VALUES(?,?,?)",
                        (no, item, round(rng.uniform(0.01, 50.0), 3)),
                    )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
    log.info("示例源已播种 %d 个样品", n_samples)
    return n_samples


def _apply_correction_txn(conn, sample_no: str, new_site: str | None,
                          adds: list[dict], dels: list[str], note: str) -> int:
    """在 conn 上执行一笔完整更正事务：改样品 + 增删多条结果 + 写变更日志。

    全部操作处于同一事务：要么整体提交，要么整体回滚，
    分析库随后按日志整笔重放，两端语义一致。
    """
    cur = conn.execute(
        "SELECT sample_no, site FROM samples WHERE sample_no=?", (sample_no,)
    ).fetchone()
    if cur is None:
        raise ValueError(f"样品不存在: {sample_no}")
    if new_site is None:
        new_site = cur["site"]

    conn.execute("BEGIN IMMEDIATE")
    try:
        conn.execute(
            "UPDATE samples SET site=?, updated_at=datetime('now') WHERE sample_no=?",
            (new_site, sample_no),
        )
        for item in dels:
            conn.execute(
                "DELETE FROM results WHERE sample_no=? AND item=?", (sample_no, item)
            )
        for a in adds:
            conn.execute(
                "INSERT OR REPLACE INTO results(sample_no, item, value) VALUES(?,?,?)",
                (sample_no, a["item"], float(a["value"])),
            )
        tx_id = conn.execute("INSERT INTO tx_log(note) VALUES(?)", (note,)).lastrowid
        seq = 0
        insert_op = (
            "INSERT INTO tx_ops(tx_id, seq, entity, op, sample_no, item, site, value)"
            " VALUES(?,?,?,?,?,?,?,?)"
        )
        conn.execute(
            insert_op,
            (tx_id, seq, "sample", "upsert", sample_no, None, new_site, None),
        )
        for item in dels:
            seq += 1
            conn.execute(
                insert_op,
                (tx_id, seq, "result", "delete", sample_no, item, None, None),
            )
        for a in adds:
            seq += 1
            conn.execute(
                insert_op,
                (tx_id, seq, "result", "upsert", sample_no, a["item"], None,
                 float(a["value"])),
            )
        conn.execute("COMMIT")
        return tx_id
    except Exception:
        conn.execute("ROLLBACK")
        raise


def apply_correction(path: str, sample_no: str, new_site: str | None = None,
                     adds: list[dict] | None = None, dels: list[str] | None = None,
                     note: str = "manual correction") -> int:
    """源端对外入口：一笔事务内更正样品并增删多条结果，返回 tx_id。"""
    adds = adds or []
    dels = dels or []
    if not adds and not dels and new_site is None:
        raise ValueError("更正内容为空")
    with _write_lock, closing(connect(path)) as conn:
        tx_id = _apply_correction_txn(conn, sample_no, new_site, adds, dels, note)
    log.info("源端更正已提交 tx=%d 样品=%s (+%d/-%d 条结果)", tx_id, sample_no,
             len(adds), len(dels))
    return tx_id


def random_correction(path: str, rng: random.Random | None = None,
                      note: str = "auto correction") -> dict:
    """生成一笔随机更正：改采样点 + 删 1~2 条结果 + 增/改 1~3 条结果。"""
    rng = rng or random.Random()
    with _write_lock, closing(connect(path)) as conn:
        rows = conn.execute(
            "SELECT sample_no, site FROM samples ORDER BY sample_no"
        ).fetchall()
        if not rows:
            raise ValueError("作业库为空，无法更正")
        picked = rng.choice(rows)
        sample_no, site = picked["sample_no"], picked["site"]
        existing = [r["item"] for r in conn.execute(
            "SELECT item FROM results WHERE sample_no=? ORDER BY item", (sample_no,)
        )]

        n_del = rng.randint(1, min(2, max(1, len(existing) - 1))) if existing else 0
        dels = rng.sample(existing, k=n_del) if existing else []
        remaining = [i for i in existing if i not in dels]
        new_items = [i for i in ITEMS if i not in remaining]
        adds = []
        k_max = min(3, len(new_items))
        if k_max >= 1:
            for item in rng.sample(new_items, k=rng.randint(1, k_max)):
                adds.append({"item": item, "value": round(rng.uniform(0.01, 50.0), 3)})
        # 没有可新增的项目（或随机决定）时，复测一条现有项目（更新检测值）
        if remaining and (not adds or rng.random() < 0.5):
            adds.append({"item": rng.choice(remaining),
                         "value": round(rng.uniform(0.01, 50.0), 3)})
        new_site = f"{site.split('-R')[0]}-R{rng.randint(1, 999)}"

        tx_id = _apply_correction_txn(conn, sample_no, new_site, adds, dels, note)
    log.info("源端更正已提交 tx=%d 样品=%s (+%d/-%d 条结果)", tx_id, sample_no,
             len(adds), len(dels))
    return {"tx_id": tx_id, "sample_no": sample_no, "new_site": new_site,
            "adds": adds, "dels": dels}


def read_snapshot(path: str) -> tuple[int, list[dict], list[dict]]:
    """一致性全量快照：同一读事务内取 high_water 与两张业务表。

    WAL 下读事务看到事务开始时刻的完整快照，且期间源端写事务可正常提交；
    快照之后提交的更正由增量日志（tx_id > high_water）追平，不会遗漏。
    """
    with closing(connect(path)) as conn:
        conn.execute("BEGIN")
        try:
            high = conn.execute(
                "SELECT COALESCE(MAX(tx_id),0) FROM tx_log"
            ).fetchone()[0]
            samples = [dict(r) for r in conn.execute(
                "SELECT sample_no, site FROM samples ORDER BY sample_no"
            )]
            results = [dict(r) for r in conn.execute(
                "SELECT sample_no, item, value FROM results ORDER BY sample_no, item"
            )]
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
    return high, samples, results


def read_txs_after(path: str, after: int, limit: int = 500) -> list[dict]:
    """按源端提交顺序读取 tx_id > after 的变更事务（含有序操作流水）。"""
    with closing(connect(path)) as conn:
        txs = [dict(r) for r in conn.execute(
            "SELECT tx_id, note, created_at FROM tx_log "
            "WHERE tx_id>? ORDER BY tx_id LIMIT ?", (after, limit),
        )]
        if not txs:
            return []
        lo, hi = txs[0]["tx_id"], txs[-1]["tx_id"]
        ops = conn.execute(
            "SELECT tx_id, seq, entity, op, sample_no, item, site, value "
            "FROM tx_ops WHERE tx_id BETWEEN ? AND ? ORDER BY tx_id, seq", (lo, hi),
        ).fetchall()
    by_tx: dict[int, list[dict]] = {t["tx_id"]: [] for t in txs}
    for o in ops:
        if o["tx_id"] in by_tx:
            by_tx[o["tx_id"]].append(dict(o))
    for t in txs:
        t["ops"] = by_tx[t["tx_id"]]
    return txs


def max_tx(path: str) -> int:
    with closing(connect(path)) as conn:
        return conn.execute("SELECT COALESCE(MAX(tx_id),0) FROM tx_log").fetchone()[0]


def recent_txs(path: str, limit: int = 20) -> list[dict]:
    with closing(connect(path)) as conn:
        txs = [dict(r) for r in conn.execute(
            "SELECT tx_id, note, created_at FROM tx_log "
            "ORDER BY tx_id DESC LIMIT ?", (limit,)
        )]
        if not txs:
            return []
        ids = ",".join(str(t["tx_id"]) for t in txs)
        ops = conn.execute(
            f"SELECT tx_id, seq, entity, op, sample_no, item, site, value "
            f"FROM tx_ops WHERE tx_id IN ({ids}) ORDER BY tx_id, seq"
        ).fetchall()
    by_tx: dict[int, list[dict]] = {t["tx_id"]: [] for t in txs}
    for o in ops:
        by_tx[o["tx_id"]].append(dict(o))
    for t in txs:
        t["ops"] = by_tx[t["tx_id"]]
    return txs


def counts(path: str) -> dict:
    with closing(connect(path)) as conn:
        return {
            "samples": conn.execute("SELECT COUNT(*) FROM samples").fetchone()[0],
            "results": conn.execute("SELECT COUNT(*) FROM results").fetchone()[0],
            "txs": conn.execute("SELECT COUNT(*) FROM tx_log").fetchone()[0],
        }


def dump_state(path: str) -> dict:
    """规范化全量导出，供对账（与目标库逐行比对）。"""
    with closing(connect(path)) as conn:
        samples = [tuple(r) for r in conn.execute(
            "SELECT sample_no, site FROM samples ORDER BY sample_no"
        )]
        results = [tuple(r) for r in conn.execute(
            "SELECT sample_no, item, value FROM results ORDER BY sample_no, item"
        )]
    return {"samples": samples, "results": results}
