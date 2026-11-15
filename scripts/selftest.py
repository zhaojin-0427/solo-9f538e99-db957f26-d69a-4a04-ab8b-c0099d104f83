#!/usr/bin/env python3
"""端到端自测：启动真实服务进程，按需求逐条验证。

覆盖：
  1. 首次全量前查询不开放（409）
  2. 全量期间源端更正仍能完成，且切换增量后不漏更正
  3. 增量同步：改样品 + 增删结果按源端提交顺序整笔生效；删除不复活
  4. 目标写入失败（注入）不推进进度，重试不重复生效
  5. 进程中断重启：按持久进度接续，不清库、不重做全量
  6. ready 后拒绝重复发起全量
  7. 每个阶段结束后源/目标两库逐行精确对账
"""
import os
import shutil
import signal
import sqlite3
import subprocess
import sys
import tempfile
import time

import requests

BASE = "http://127.0.0.1:5055"
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

FAILURES = []


def check(name, cond, detail=""):
    mark = "PASS" if cond else "FAIL"
    print(f"  [{mark}] {name}" + (f" — {detail}" if detail and not cond else ""))
    if not cond:
        FAILURES.append(f"{name} {detail}")


def wait_until(fn, timeout=30, interval=0.2, desc=""):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if fn():
            return True
        time.sleep(interval)
    print(f"  [超时] 等待条件未满足: {desc}")
    return False


def status():
    return requests.get(f"{BASE}/api/status", timeout=5).json()


def dump(db_path):
    conn = sqlite3.connect(db_path)
    samples = conn.execute(
        "SELECT sample_no, site FROM samples ORDER BY sample_no").fetchall()
    results = conn.execute(
        "SELECT sample_no, item, value FROM results ORDER BY sample_no, item"
    ).fetchall()
    conn.close()
    return samples, results


def reconcile(src_db, tgt_db):
    return dump(src_db) == dump(tgt_db)


def wait_synced(src_db, tgt_db, timeout=30):
    """等待增量追平且两库逐行一致。"""
    def ok():
        s = status()
        return (s["phase"] == "ready"
                and s["last_tx_id"] >= s["source_max_tx"]
                and reconcile(src_db, tgt_db))
    return wait_until(ok, timeout=timeout, desc="两库追平且一致")


def start_server(data_dir, snapshot_delay_ms=0):
    env = dict(os.environ)
    env.update({
        "DATA_DIR": data_dir,
        "PORT": "5055",
        "SYNC_POLL_INTERVAL": "0.3",
        "SYNC_SNAPSHOT_DELAY_MS": str(snapshot_delay_ms),
        "SEED_SAMPLES": "20",
    })
    proc = subprocess.Popen(
        [sys.executable, "-m", "app"], cwd=ROOT, env=env,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )
    assert wait_until(lambda: _healthy(), timeout=30, desc="服务就绪"), "服务未能启动"
    return proc


def _healthy():
    try:
        return requests.get(f"{BASE}/api/health", timeout=2).json().get("ok")
    except Exception:
        return False


def stop_server(proc):
    proc.send_signal(signal.SIGTERM)
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)


def main():
    data_dir = tempfile.mkdtemp(prefix="geosync-test-")
    src_db = os.path.join(data_dir, "source.db")
    tgt_db = os.path.join(data_dir, "target.db")
    # 快照后停顿 1.5s：人为放大“全量期间源端更正”的竞态窗口
    proc = start_server(data_dir, snapshot_delay_ms=1500)
    try:
        print("== 阶段 1：初始状态与查询门禁 ==")
        s = status()
        check("初始阶段为 idle", s["phase"] == "idle", str(s))
        r = requests.get(f"{BASE}/api/query", params={"sample_no": "YT-0001"}, timeout=5)
        check("全量前关联查询被拒绝(409)", r.status_code == 409, str(r.status_code))

        print("== 阶段 2：全量前源端更正（应被快照覆盖，不会重复应用）==")
        r = requests.post(f"{BASE}/api/source/correct", timeout=5).json()
        check("全量前更正提交成功", r.get("ok") is True, str(r))

        print("== 阶段 3：全量期间源端更正仍能完成，且切换增量后不漏 ==")
        r = requests.post(f"{BASE}/api/sync/full", timeout=5)
        check("发起首次全量返回202", r.status_code == 202, str(r.status_code))
        check("等待进入 full_running",
              wait_until(lambda: status()["phase"] == "full_running", 10,
                         desc="full_running"))
        r = requests.get(f"{BASE}/api/query", params={"sample_no": "YT-0001"}, timeout=5)
        check("全量期间关联查询仍被拒绝(409)", r.status_code == 409, str(r.status_code))
        mid_txs = []
        for _ in range(3):  # 全量装载窗口内连续提交 3 笔源端更正
            r = requests.post(f"{BASE}/api/source/correct", timeout=5).json()
            mid_txs.append(r.get("tx_id"))
        check("全量期间 3 笔源端更正全部提交成功",
              all(t is not None for t in mid_txs), str(mid_txs))
        check("全量完成进入 ready",
              wait_until(lambda: status()["phase"] == "ready", 30, desc="ready"))
        check("全量+追平后两库逐行一致", wait_synced(src_db, tgt_db, 30))
        r = requests.get(f"{BASE}/api/query", params={"sample_no": "YT-0001"}, timeout=5)
        check("ready 后关联查询开放", r.status_code == 200 and r.json().get("ok"),
              str(r.status_code))

        print("== 阶段 4：增量同步（指定更正：改样品 + 删 2 条 + 增 2 条）==")
        tgt_samples, _ = dump(tgt_db)
        victim = tgt_samples[0][0]
        old_items = [it for (no, it, _v) in dump(tgt_db)[1] if no == victim]
        dels = old_items[:2]
        adds = [{"item": "Mn", "value": 7.777}, {"item": "Co", "value": 0.555}]
        r = requests.post(f"{BASE}/api/source/correct", json={
            "sample_no": victim, "site": "ZK-999-复测",
            "adds": adds, "dels": dels, "note": "selftest explicit",
        }, timeout=5).json()
        check("指定更正提交成功", r.get("ok") is True, str(r))
        check("指定更正同步后两库一致", wait_synced(src_db, tgt_db, 30))
        detail = requests.get(f"{BASE}/api/query",
                              params={"sample_no": victim}, timeout=5).json()["sample"]
        items = {x["item"]: x["value"] for x in detail["results"]}
        check("样品采样点已更正", detail["site"] == "ZK-999-复测", str(detail))
        check("新增项目 Mn/Co 已同步",
              items.get("Mn") == 7.777 and items.get("Co") == 0.555, str(items))
        check("删除项目未复活", all(d not in items for d in dels),
              f"dels={dels} items={sorted(items)}")
        for _ in range(3):  # 后续随机事务不得让已删记录回流
            requests.post(f"{BASE}/api/source/correct", timeout=5)
        check("更多更正后两库一致", wait_synced(src_db, tgt_db, 30))
        # 反复活不变式：分析库不得存在作业库中已不存在的记录
        src_rows = set(dump(src_db)[1])
        tgt_rows = set(dump(tgt_db)[1])
        ghosts = tgt_rows - src_rows
        check("已删除数据未回流（分析库无源端不存在的记录）",
              not ghosts, str(ghosts)[:200])

        print("== 阶段 5：目标写入失败不推进进度，重试不重复生效 ==")
        before = status()["last_tx_id"]
        for _ in range(3):  # 武装 3 次 -> 连续失败约 3 秒，便于观察停滞
            requests.post(f"{BASE}/api/fault/inject", timeout=5)
        r = requests.post(f"{BASE}/api/source/correct", timeout=5).json()
        new_tx = r["tx_id"]
        stalled = wait_until(
            lambda: status()["last_tx_id"] == before and status()["last_error"],
            timeout=5, desc="进度停滞且报错")
        check("注入失败期间进度未推进", stalled,
              f"before={before} now={status()['last_tx_id']}")
        check("失败信息已暴露", "注入" in (status()["last_error"] or ""),
              str(status()["last_error"]))
        check("自动重试后进度推进且两库一致（无重复生效）",
              wait_synced(src_db, tgt_db, 30)
              and status()["last_tx_id"] >= new_tx)

        print("== 阶段 6：进程中断重启，按持久进度接续 ==")
        snap = status()
        tgt_before = dump(tgt_db)
        stop_server(proc)
        proc = start_server(data_dir, snapshot_delay_ms=0)
        s = status()
        check("重启后直接处于 ready（未重做全量）", s["phase"] == "ready", str(s))
        check("重启后 last_tx_id 与重启前一致",
              s["last_tx_id"] == snap["last_tx_id"],
              f"{s['last_tx_id']} vs {snap['last_tx_id']}")
        check("重启后 full_done_at 未变化（未清库重来）",
              s["full_done_at"] == snap["full_done_at"], str(s))
        check("重启后分析库数据未被动过", dump(tgt_db) == tgt_before)
        r = requests.post(f"{BASE}/api/sync/full", timeout=5)
        check("ready 后重复发起全量被拒绝(409)", r.status_code == 409,
              str(r.status_code))
        r = requests.post(f"{BASE}/api/source/correct", timeout=5).json()
        check("重启后新更正继续增量同步",
              r.get("ok") is True and wait_synced(src_db, tgt_db, 30))

        print("== 阶段 7：自动更正发生器（持续装入演示）==")
        requests.post(f"{BASE}/api/source/auto",
                      json={"enabled": True}, timeout=5)
        tx0 = status()["source_max_tx"]
        check("自动更正持续产生并同步",
              wait_until(lambda: status()["source_max_tx"] >= tx0 + 3
                         and status()["last_tx_id"] >= status()["source_max_tx"],
                         timeout=20, desc="自动更正流水"))
        requests.post(f"{BASE}/api/source/auto",
                      json={"enabled": False}, timeout=5)
        check("自动更正后两库最终一致", wait_synced(src_db, tgt_db, 30))
    finally:
        stop_server(proc)
        out = proc.stdout.read() if proc.stdout else ""
        if FAILURES:
            print("\n---- 服务日志（尾部）----")
            print("\n".join(out.splitlines()[-40:]))
        shutil.rmtree(data_dir, ignore_errors=True)

    print()
    if FAILURES:
        print(f"自测失败 {len(FAILURES)} 项：")
        for f in FAILURES:
            print("  -", f)
        return 1
    print("全部自测通过 ✔")
    return 0


if __name__ == "__main__":
    sys.exit(main())
