"""更正撤销服务（作业库侧）。

业务背景：地质化验数据员误改采样点或检测值后，需要撤销整笔更正，同时保留
该笔更正之后发生的其他修改。

设计要点（与需求逐条对应）：

1. 仅支持"启用后的普通更正"：只有在 undo_change 中留有提交前后实际变化的
   事务才可撤销；旧卷里功能启用前的事务没有这些行，直接标明不可撤销。
2. 以【采样点 + 各检测项目】为单位识别实际变化：增、删、改都体现为
   before_state / after_state 的缺失（NULL）或具体值。
3. 冲突判断按受影响项逐个进行：只要任一受影响项在该更正之后又被其他事务
   改过（哪怕改回原值，或删除后重建），整笔拒绝并列明冲突项；其余项的
   变化不影响撤销。每次确认都在写事务内重新判断，避免预览与确认之间的
   并发修改漏网。
4. 撤销成功 = 提交一笔新的源事务（补偿事务），把各受影响项恢复为该更正
   提交前的值或缺失状态；其他项原样保留。补偿事务是普通 tx_log/tx_ops，
   经原有增量链路整笔进入分析库，水位 last_tx_id 只增不减（不回退）。
5. 原子性：undo_record 撤销登记、业务数据恢复、补偿日志（tx_log/tx_ops/
   undo_change）在同一 SQLite 事务提交，任一失败整体回滚、数据不变。
6. 幂等：undo_record 以 original_tx_id 为主键、compensation_tx_id 唯一；
   并发或进程重启后重复撤销同一笔，返回同一个补偿事务号，不会重复补偿。
7. 补偿事务本身不可撤销（preview/confirm 都会拒绝）。
"""
import json
import logging
from contextlib import closing

from . import source
from .dbutil import connect

log = logging.getLogger("geosync.undo")


class UndoError(Exception):
    """撤销被拒绝的业务基类（映射为 HTTP 409/400）。"""


class UndoNotFound(UndoError):
    """源事务号不存在。"""


class UndoNotSupported(UndoError):
    """不可撤销：旧事务缺前值，或目标为补偿事务。"""


class UndoConflict(UndoError):
    """存在后来的修改冲突，整笔拒绝。"""


def _decode_state(raw):
    return None if raw is None else json.loads(raw)


def _tx_exists(conn, tx_id: int) -> bool:
    return conn.execute(
        "SELECT 1 FROM tx_log WHERE tx_id=?", (tx_id,)
    ).fetchone() is not None


def _is_compensation(conn, tx_id: int) -> bool:
    return conn.execute(
        "SELECT 1 FROM undo_record WHERE compensation_tx_id=?", (tx_id,)
    ).fetchone() is not None


def _existing_undo(conn, original_tx_id: int) -> int | None:
    row = conn.execute(
        "SELECT compensation_tx_id FROM undo_record WHERE original_tx_id=?",
        (original_tx_id,),
    ).fetchone()
    return None if row is None else row["compensation_tx_id"]


def _load_changes(conn, tx_id: int) -> list[dict]:
    rows = conn.execute(
        "SELECT entity, sample_no, item, before_state, after_state "
        "FROM undo_change WHERE tx_id=? ORDER BY entity, sample_no, item",
        (tx_id,),
    ).fetchall()
    out = []
    for r in rows:
        out.append({
            "entity": r["entity"],
            "sample_no": r["sample_no"],
            "item": r["item"],
            "before": _decode_state(r["before_state"]),
            "after": _decode_state(r["after_state"]),
            # 缺失状态（None）需要与 JSON null 区分，用两个布尔标志表达给前端。
            "before_missing": r["before_state"] is None,
            "after_missing": r["after_state"] is None,
        })
    return out


def _find_conflicts(conn, original_tx_id: int, changes: list[dict]) -> list[dict]:
    """逐个受影响项检查 original_tx_id 之后是否又被改过。

    只要 undo_change 中存在 tx_id 更大、且命中同一 (entity, sample_no, item)
    的变化行，即视为该项后来被改过——无论其当前值是否已经改回、是否删后
    重建。NULL 键用 IS 比较，正确覆盖检测项目的增/删场景。
    """
    conflicts = []
    for ch in changes:
        row = conn.execute(
            "SELECT tx_id FROM undo_change "
            "WHERE tx_id > ? AND entity=? AND sample_no=? "
            "AND (item IS ? OR item = ?) "
            "ORDER BY tx_id LIMIT 1",
            (original_tx_id, ch["entity"], ch["sample_no"],
             ch["item"], ch["item"]),
        ).fetchone()
        if row is not None:
            conflicts.append({
                "entity": ch["entity"],
                "sample_no": ch["sample_no"],
                "item": ch["item"],
                "later_tx_id": row["tx_id"],
                "message": ("采样点" if ch["entity"] == "sample"
                            else f"检测项目 {ch['item']}")
                           + f" 在更正 tx={original_tx_id} 之后又被 tx="
                           f"{row['tx_id']} 修改（含改回原值或删后重建）",
            })
    return conflicts


def preview_undo(path: str, original_tx_id: int) -> dict:
    """按源事务号查看撤销所需的全部信息（只读，不产生任何写入）。"""
    with source._write_lock, closing(connect(path)) as conn:
        if not _tx_exists(conn, original_tx_id):
            raise UndoNotFound(f"源事务不存在: tx={original_tx_id}")
        comp_tx = _existing_undo(conn, original_tx_id)
        if comp_tx is not None:
            return {
                "tx_id": original_tx_id,
                "undoable": True,
                "already_undone": True,
                "compensation_tx_id": comp_tx,
                "changes": [],
                "conflicts": [],
                "message": f"该事务已撤销，补偿事务为 tx={comp_tx}",
            }
        if _is_compensation(conn, original_tx_id):
            raise UndoNotSupported(
                f"tx={original_tx_id} 是撤销产生的补偿事务，补偿事务不可撤销")
        changes = _load_changes(conn, original_tx_id)
        if not changes:
            return {
                "tx_id": original_tx_id,
                "undoable": False,
                "already_undone": False,
                "compensation_tx_id": None,
                "changes": [],
                "conflicts": [],
                "reason": "该事务缺少提交前后值记录（功能启用前的旧事务），不可撤销",
            }
        conflicts = _find_conflicts(conn, original_tx_id, changes)
        return {
            "tx_id": original_tx_id,
            "undoable": not conflicts,
            "already_undone": False,
            "compensation_tx_id": None,
            "changes": changes,
            "conflicts": conflicts,
            "reason": None if not conflicts else "存在受影响项在该更正之后被修改，整笔不可撤销",
        }


def _build_compensation_ops(changes: list[dict]) -> dict:
    """把受影响项的提交前状态翻译成一笔补偿事务的业务操作。

    返回 {'sample_no', 'new_site', 'adds', 'dels'}：
      - 采样点：恢复原 site（普通更正只改不删样品，故前值必存在）
      - 检测项目：前值缺失 -> 删除（恢复原缺失）；前值存在 -> upsert 回原值
    """
    if not changes:
        raise UndoNotSupported("该更正没有识别到任何受影响项，无需撤销")
    sample_no = changes[0]["sample_no"]
    new_site = None
    adds: list[dict] = []
    dels: list[str] = []
    for ch in changes:
        if ch["entity"] == "sample":
            new_site = ch["before"]  # 采样点前值存在（普通更正不删除样品）
        elif ch["before_missing"]:
            dels.append(ch["item"])
        else:
            adds.append({"item": ch["item"], "value": ch["before"]})
    return {"sample_no": sample_no, "new_site": new_site,
            "adds": adds, "dels": dels}


def confirm_undo(path: str, original_tx_id: int) -> dict:
    """确认撤销：在一个源事务内完成恢复 + 日志 + 登记（原子、幂等）。"""
    with source._write_lock, closing(connect(path)) as conn:
        if not _tx_exists(conn, original_tx_id):
            raise UndoNotFound(f"源事务不存在: tx={original_tx_id}")

        # 幂等：并发或重启后重复撤销，直接返回同一补偿事务。
        existing = _existing_undo(conn, original_tx_id)
        if existing is not None:
            log.info("重复撤销 tx=%d -> 返回既有补偿事务 tx=%d",
                     original_tx_id, existing)
            return {"tx_id": original_tx_id, "compensation_tx_id": existing,
                    "idempotent": True, "restored": 0}

        if _is_compensation(conn, original_tx_id):
            raise UndoNotSupported(
                f"tx={original_tx_id} 是补偿事务，补偿事务不可撤销")

        changes = _load_changes(conn, original_tx_id)
        if not changes:
            raise UndoNotSupported(
                f"tx={original_tx_id} 缺少提交前后值记录（旧事务），不可撤销")

        # 确认时刻在写事务内重新判断冲突（预览之后可能又发生了更正）。
        conflicts = _find_conflicts(conn, original_tx_id, changes)
        if conflicts:
            raise UndoConflict(
                f"tx={original_tx_id} 存在 {len(conflicts)} 个受影响项后来被修改，"
                "整笔撤销被拒绝")

        plan = _build_compensation_ops(changes)
        sample_no = plan["sample_no"]
        # 补偿可能只涉及检测项目、不含采样点恢复；tx_log 仍需一条 sample 行，
        # 因此取该样品当前 site（重放为幂等 upsert，不改变采样点）。
        if plan["new_site"] is None:
            row = conn.execute(
                "SELECT site FROM samples WHERE sample_no=?", (sample_no,)
            ).fetchone()
            plan["new_site"] = row["site"] if row is not None else None

        conn.execute("BEGIN IMMEDIATE")
        try:
            # 二次门禁：拿到写锁/写事务后再查一次登记，彻底挡住并发重复撤销。
            existing2 = _existing_undo(conn, original_tx_id)
            if existing2 is not None:
                conn.execute("ROLLBACK")
                log.info("并发重复撤销 tx=%d -> 返回既有补偿事务 tx=%d",
                         original_tx_id, existing2)
                return {"tx_id": original_tx_id, "compensation_tx_id": existing2,
                        "idempotent": True, "restored": 0}

            source._apply_business_writes(
                conn, sample_no, plan["new_site"], plan["adds"], plan["dels"]
            )
            note = f"undo compensation of tx={original_tx_id}"
            comp_tx = source._write_tx_log(
                conn, sample_no, plan["new_site"], plan["adds"], plan["dels"], note
            )

            # 补偿事务自身的 undo_change：记录每个受影响项从"当前值"恢复到
            # "原前值"的实际变化，保证后续冲突判断与"补偿不可撤销"成立。
            comp_rows = []
            for ch in changes:
                comp_rows.append({
                    "entity": ch["entity"],
                    "sample_no": ch["sample_no"],
                    "item": ch["item"],
                    # 补偿前 = 原更正后的状态；补偿后 = 原更正前的状态。
                    "before_state": None if ch["after_missing"]
                    else json.dumps(ch["after"], ensure_ascii=False),
                    "after_state": None if ch["before_missing"]
                    else json.dumps(ch["before"], ensure_ascii=False),
                })
            source._insert_undo_changes(conn, comp_tx, comp_rows)

            conn.execute(
                "INSERT INTO undo_record(original_tx_id, compensation_tx_id) "
                "VALUES(?, ?)",
                (original_tx_id, comp_tx),
            )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise

    log.info("撤销已提交: 原事务 tx=%d -> 补偿事务 tx=%d（恢复 %d 个受影响项）",
             original_tx_id, comp_tx, len(changes))
    return {"tx_id": original_tx_id, "compensation_tx_id": comp_tx,
            "idempotent": False, "restored": len(changes)}
