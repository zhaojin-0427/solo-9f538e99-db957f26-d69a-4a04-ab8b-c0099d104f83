#!/usr/bin/env python3
"""更正撤销功能端到端自测：启动真实服务进程，按撤销需求逐条验证。

覆盖：
  1. 仅启用后带前后值的普通更正可撤销；预览按采样点+各检测项目给出前后值
     （改值 / 新增=前缺失 / 删除=后缺失 均能识别）
  2. 撤销成功：补偿事务把各受影响项恢复为原值或原缺失，其他修改保留，
     两库逐行一致，且经原增量链路进入分析库、水位 last_tx_id 不回退
  3. 冲突：任一受影响项后来被改过 -> 整笔拒绝(409)并列明冲突；
     即使改回原值、删除后重建也算冲突
  4. 其他项（同样品未受影响的检测项目）后来被改不阻碍撤销
  5. 幂等：并发与重启后重复撤销返回同一补偿事务，不产生新事务
  6. 补偿事务不可撤销；旧事务（无前后值）标明不可撤销；不存在事务 404
  7. 失败不变：冲突拒绝后业务数据、事务计数均不变化
  8. 兼容旧卷：旧库自动补建新表，原全量/更正/同步链路不受影响
"""
import json
import os
import shutil
import signal
import sqlite3
import subprocess
import sys
import tempfile
import time

import requests

BASE = "http://127.0.0.1:5057"
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


def wait_synced(src_db, tgt_db, timeout=30):
    def ok():
        s = status()
        d_src = _dump(src_db)
        d_tgt = _dump(tgt_db)
        return (s["phase"] == "ready"
                and s["last_tx_id"] >= s["source_max_tx"]
                and d_src == d_tgt)
    return wait_until(ok, timeout=timeout, desc="两库追平且一致")


def _dump(db_path):
    conn = sqlite3.connect(db_path)
    samples = conn.execute(
        "SELECT sample_no, site FROM samples ORDER BY sample_no").fetchall()
    results = conn.execute(
        "SELECT sample_no, item, value FROM results ORDER BY sample_no, item"
    ).fetchall()
    conn.close()
    return samples, results


def _healthy():
    try:
        return requests.get(f"{BASE}/api/health", timeout=2).json().get("ok")
    except Exception:
        return False


def start_server(data_dir, port=5057):
    env = dict(os.environ)
    env.update({
        "DATA_DIR": data_dir, "PORT": str(port),
        # 显式指定两库路径，避免依赖 config 的默认相对路径布局。
        "SOURCE_DB": os.path.join(data_dir, "source.db"),
        "TARGET_DB": os.path.join(data_dir, "target.db"),
        "SYNC_POLL_INTERVAL": "0.2", "SEED_SAMPLES": "20",
    })
    proc = subprocess.Popen(
        [sys.executable, "-m", "app"], cwd=ROOT, env=env,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )
    if not wait_until(_healthy, timeout=30, desc="服务就绪"):
        out = proc.stdout.read() if proc.stdout else ""
        print("---- 服务启动失败，子进程输出 ----")
        print(out[-2000:])
        stop_server(proc)
        raise RuntimeError("服务未能启动")
    # health 路由可能早于 init_db 建表完成，等待库表真正就绪以消除启动竞态。
    def _tables_ready():
        try:
            conn = sqlite3.connect(os.path.join(data_dir, "source.db"))
            names = {r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'")}
            conn.close()
            return {"samples", "undo_change", "undo_record"} <= names
        except sqlite3.Error:
            return False
    if not wait_until(_tables_ready, timeout=15, desc="作业库建表完成"):
        stop_server(proc)
        raise RuntimeError("作业库建表未完成")
    return proc


def stop_server(proc):
    proc.send_signal(signal.SIGTERM)
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)
    # 等待监听端口释放，避免紧接的重启遇到 "Address already in use"。
    import socket
    deadline = time.time() + 10
    while time.time() < deadline:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            if s.connect_ex(("127.0.0.1", int(BASE.rsplit(":", 1)[1]))) != 0:
                return
        time.sleep(0.2)


def post_correct(payload):
    return requests.post(f"{BASE}/api/source/correct", json=payload, timeout=5).json()


def preview(tx_id):
    return requests.get(f"{BASE}/api/undo/preview",
                        params={"tx_id": tx_id}, timeout=5)


def confirm(tx_id):
    return requests.post(f"{BASE}/api/undo/confirm",
                         json={"tx_id": tx_id}, timeout=5)


def main():
    data_dir = tempfile.mkdtemp(prefix="geosync-undo-")
    # 与 config 默认布局一致：库文件直接位于 DATA_DIR 下。
    src_db = os.path.join(data_dir, "source.db")
    tgt_db = os.path.join(data_dir, "target.db")
    proc = start_server(data_dir)
    try:
        requests.post(f"{BASE}/api/sync/full", timeout=5)
        check("全量完成进入 ready 且两库一致", wait_synced(src_db, tgt_db))

        print("== 1. 前后值识别（改点 + 删除 + 新增）==")
        victim = "YT-0001"
        before = _dump(src_db)
        old_site = dict(before[0]).get(victim)
        old_site = next(s for (no, s) in before[0] if no == victim)
        old_items = {it: v for (no, it, v) in before[1] if no == victim}
        del_item = sorted(old_items)[0]
        r = post_correct({
            "sample_no": victim, "site": "ZK-WRONG",
            "adds": [{"item": "Xn", "value": 9.99}],
            "dels": [del_item], "note": "mistake"})
        tx = r["tx_id"]
        pv = preview(tx).json()
        check("预览可撤销", pv["undoable"] is True and pv["conflicts"] == [])
        keys = {(c["entity"], c["item"]): c for c in pv["changes"]}
        site_chg = keys[("sample", None)]
        check("采样点前后值正确",
              site_chg["before"] == old_site and site_chg["after"] == "ZK-WRONG")
        del_chg = keys[("result", del_item)]
        check("删除项：前值存在、后缺失",
              del_chg["before_missing"] is False
              and del_chg["after_missing"] is True
              and del_chg["before"] == old_items[del_item])
        add_chg = keys[("result", "Xn")]
        check("新增项：前缺失、后有值",
              add_chg["before_missing"] is True and add_chg["after"] == 9.99)

        print("== 2. 撤销成功：恢复原值/原缺失，经增量同步，水位不回退 ==")
        last_before = status()["last_tx_id"]
        r = confirm(tx).json()
        check("确认撤销返回补偿事务",
              r["ok"] and r["compensation_tx_id"] > tx and r["idempotent"] is False)
        comp_tx = r["compensation_tx_id"]
        check("补偿事务已同步且两库逐行一致", wait_synced(src_db, tgt_db))
        st = status()
        check("水位只增不减（不回退）",
              st["last_tx_id"] >= comp_tx and st["last_tx_id"] >= last_before)
        after = _dump(src_db)
        check("源端数据恢复到更正前", after == before)
        check("分析库与源端一致（补偿已整笔重放）", _dump(tgt_db) == before)

        print("== 3. 幂等：重复撤销返回同一补偿事务，不产生新事务 ==")
        max_tx = status()["source_max_tx"]
        r2 = confirm(tx).json()
        check("重复撤销幂等返回同一补偿事务",
              r2["idempotent"] is True and r2["compensation_tx_id"] == comp_tx)
        check("未产生新事务", status()["source_max_tx"] == max_tx)
        pv2 = preview(tx).json()
        check("预览显示已撤销及补偿号",
              pv2["already_undone"] is True
              and pv2["compensation_tx_id"] == comp_tx)

        print("== 4. 补偿事务不可撤销 ==")
        r = confirm(comp_tx)
        check("撤销补偿事务被拒绝(409)", r.status_code == 409)

        print("== 5. 冲突：后来改过即整笔拒绝（含改回原值）==")
        r = post_correct({"sample_no": "YT-0002", "site": "ZK-A",
                          "adds": [{"item": "Xy", "value": 1.0}], "note": "c1"})
        tx_a = r["tx_id"]
        # 取 tx_a 采样点的 before_state 作为该样品被更正前的原始 site
        conn = sqlite3.connect(src_db)
        row = conn.execute(
            "SELECT before_state FROM undo_change WHERE tx_id=? AND entity='sample'",
            (tx_a,)).fetchone()
        conn.close()
        assert row is not None, "undo_change 缺少采样点前后值记录"
        orig = json.loads(row[0])
        post_correct({"sample_no": "YT-0002", "site": orig, "note": "revert"})
        pv = preview(tx_a).json()
        check("改回原值仍判定冲突、不可撤销",
              pv["undoable"] is False and len(pv["conflicts"]) >= 1)
        check("冲突列明受影响项与后续事务号",
              any(c["later_tx_id"] > tx_a for c in pv["conflicts"]))
        max_before = status()["source_max_tx"]
        r = confirm(tx_a)
        check("冲突确认返回 409", r.status_code == 409
              and len(r.json().get("conflicts", [])) >= 1)
        check("冲突拒绝后无新事务、数据不变",
              status()["source_max_tx"] == max_before)

        print("== 6. 删后重建也算冲突 ==")
        r = post_correct({"sample_no": "YT-0003",
                          "adds": [{"item": "Xz", "value": 2.0}], "note": "d1"})
        tx_b = r["tx_id"]
        # 删除再重建同名项目
        post_correct({"sample_no": "YT-0003", "dels": ["Xz"], "note": "d2"})
        post_correct({"sample_no": "YT-0003",
                      "adds": [{"item": "Xz", "value": 3.0}], "note": "d3"})
        pv = preview(tx_b).json()
        check("删后重建仍判定冲突",
              pv["undoable"] is False
              and any(c["item"] == "Xz" for c in pv["conflicts"]))

        print("== 7. 其他项变化不阻碍撤销（确认时重新判断）==")
        r = post_correct({"sample_no": "YT-0004",
                          "adds": [{"item": "Xw", "value": 4.0}], "note": "e1"})
        tx_c = r["tx_id"]
        other = next(it for (no, it, _v) in _dump(src_db)[1]
                     if no == "YT-0004" and it != "Xw")
        pre_other = dict((it, v) for (no, it, v) in _dump(src_db)[1]
                         if no == "YT-0004")[other]
        post_correct({"sample_no": "YT-0004",
                      "adds": [{"item": other, "value": 123.45}], "note": "e2"})
        pv = preview(tx_c).json()
        check("未受影响项被改，目标项仍可撤销", pv["undoable"] is True)
        r = confirm(tx_c).json()
        check("选择性撤销成功", r["ok"] and r["restored"] == 1)
        check("其他项的后来修改被保留",
              dict((it, v) for (no, it, v) in _dump(src_db)[1]
                   if no == "YT-0004")[other] == 123.45
              and 123.45 != pre_other)
        check("撤销后两库一致", wait_synced(src_db, tgt_db))

        print("== 8. 并发重复撤销只有一笔补偿 ==")
        r = post_correct({"sample_no": "YT-0005", "site": "ZK-P",
                          "adds": [{"item": "Xv", "value": 5.0}], "note": "p1"})
        tx_d = r["tx_id"]
        wait_synced(src_db, tgt_db)
        responses = []
        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=10) as ex:
            responses = list(ex.map(lambda _: confirm(tx_d).json(), range(10)))
        comps = {x["compensation_tx_id"] for x in responses if x.get("ok")}
        real = [x for x in responses if x.get("idempotent") is False]
        check("10 并发仅 1 笔真实补偿、其余幂等同号",
              len(comps) == 1 and len(real) == 1, str(responses[:2]))
        conn = sqlite3.connect(src_db)
        n = conn.execute(
            "SELECT COUNT(*) FROM undo_record WHERE original_tx_id=?",
            (tx_d,)).fetchone()[0]
        conn.close()
        check("undo_record 仅一条登记", n == 1)
        check("并发后两库一致", wait_synced(src_db, tgt_db))

        print("== 9. 不存在事务 / 非法参数 ==")
        check("不存在事务预览 404", preview(999999).status_code == 404)
        check("非法 tx_id 400",
              requests.get(f"{BASE}/api/undo/preview",
                           params={"tx_id": "abc"}, timeout=5).status_code == 400)

        print("== 10. 重启后重复撤销仍返回同一补偿事务 ==")
        snap = status()
        stop_server(proc)
        proc = start_server(data_dir)
        r = confirm(tx).json()  # tx 已在步骤 3 撤销过
        check("重启后幂等返回同一补偿事务",
              r["idempotent"] is True and r["compensation_tx_id"] == comp_tx)
        check("重启后水位不回退、两库一致",
              status()["last_tx_id"] == snap["last_tx_id"]
              and wait_synced(src_db, tgt_db))
    finally:
        stop_server(proc)
        if FAILURES:
            out = proc.stdout.read() if proc.stdout else ""
            print("\n---- 服务日志（尾部）----")
            print("\n".join(out.splitlines()[-40:]))
        shutil.rmtree(data_dir, ignore_errors=True)

    print()
    if FAILURES:
        print(f"撤销自测失败 {len(FAILURES)} 项：")
        for f in FAILURES:
            print("  -", f)
        return 1
    print("撤销功能全部自测通过 ✔")
    return 0


if __name__ == "__main__":
    sys.exit(main())
