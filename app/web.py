"""Flask Web 入口：状态面板 + 操作 + 关联明细查询。"""
import logging
import threading

from flask import Flask, jsonify, render_template, request

from . import config, source, target
from .syncer import FaultInjector, Syncer

log = logging.getLogger("geosync.web")


class AutoCorrector(threading.Thread):
    """自动更正发生器：模拟源端持续产生更正事务。"""

    def __init__(self, source_path: str, interval: float) -> None:
        super().__init__(name="geosync-autocorrect", daemon=True)
        self.source_path = source_path
        self.interval = interval
        self.enabled = threading.Event()
        self._stop_event = threading.Event()

    def run(self) -> None:
        while not self._stop_event.is_set():
            if self.enabled.is_set():
                try:
                    source.random_correction(self.source_path, note="auto correction")
                except Exception as exc:
                    log.error("自动更正失败: %s", exc)
            self._stop_event.wait(self.interval)


def create_app() -> Flask:
    app = Flask(__name__)

    # 两库独立初始化；作业库为空时播种示例数据（重启不会重复播种）。
    source.init_db(config.SOURCE_DB)
    source.seed_if_empty(config.SOURCE_DB, config.SEED_SAMPLES)
    target.init_db(config.TARGET_DB)

    injector = FaultInjector()
    syncer = Syncer(config.SOURCE_DB, config.TARGET_DB, injector,
                    poll_interval=config.POLL_INTERVAL,
                    snapshot_delay_ms=config.SNAPSHOT_DELAY_MS)
    syncer.start()
    auto = AutoCorrector(config.SOURCE_DB, config.AUTO_CORRECT_INTERVAL)
    auto.start()
    app.extensions["syncer"] = syncer
    app.extensions["injector"] = injector
    app.extensions["auto"] = auto

    # ------------------------------------------------------------- 页面
    @app.get("/")
    def index():
        return render_template("index.html")

    # ------------------------------------------------------------- 状态
    @app.get("/api/health")
    def health():
        return jsonify({"ok": True})

    @app.get("/api/status")
    def status():
        st = target.get_state(config.TARGET_DB)
        src_max = source.max_tx(config.SOURCE_DB)
        src_counts = source.counts(config.SOURCE_DB)
        tgt_counts = target.counts(config.TARGET_DB)
        consistent = (source.dump_state(config.SOURCE_DB)
                      == target.dump_state(config.TARGET_DB))
        return jsonify({
            "phase": st["phase"],
            "last_tx_id": st["last_tx_id"],
            "high_water": st["high_water"],
            "source_max_tx": src_max,
            "pending": max(0, src_max - st["last_tx_id"]),
            "full_started_at": st["full_started_at"],
            "full_done_at": st["full_done_at"],
            "source_counts": src_counts,
            "target_counts": tgt_counts,
            "consistent": consistent,
            "auto_correct": auto.enabled.is_set(),
            "fault_armed": injector.armed,
            "last_error": syncer.last_error,
        })

    # ------------------------------------------------------------- 同步控制
    @app.post("/api/sync/full")
    def start_full_sync():
        if target.request_full_sync(config.TARGET_DB):
            return jsonify({"ok": True, "message": "首次全量同步已启动"}), 202
        st = target.get_state(config.TARGET_DB)
        return jsonify({"ok": False,
                        "message": f"当前状态 {st['phase']} 不允许发起全量"
                                   "（首次全量只允许一次，之后为增量同步）"}), 409

    # ------------------------------------------------------------- 源端操作
    @app.post("/api/source/correct")
    def correct():
        body = request.get_json(silent=True) or {}
        try:
            if body.get("sample_no"):
                tx_id = source.apply_correction(
                    config.SOURCE_DB,
                    sample_no=body["sample_no"],
                    new_site=body.get("site"),
                    adds=body.get("adds") or [],
                    dels=body.get("dels") or [],
                    note=body.get("note") or "manual correction",
                )
                return jsonify({"ok": True, "tx_id": tx_id})
            detail = source.random_correction(config.SOURCE_DB,
                                              note="manual correction")
            return jsonify({"ok": True, **detail})
        except ValueError as exc:
            return jsonify({"ok": False, "message": str(exc)}), 400

    @app.post("/api/source/auto")
    def toggle_auto():
        body = request.get_json(silent=True) or {}
        if body.get("enabled"):
            auto.enabled.set()
        else:
            auto.enabled.clear()
        return jsonify({"ok": True, "auto_correct": auto.enabled.is_set()})

    # ------------------------------------------------------------- 故障注入
    @app.post("/api/fault/inject")
    def inject_fault():
        injector.arm(1)
        return jsonify({"ok": True,
                        "message": "已武装：下一次目标写入将失败一次（随后自动重试）"})

    # ------------------------------------------------------------- 查询
    @app.get("/api/query")
    def query():
        st = target.get_state(config.TARGET_DB)
        if st["phase"] != target.PHASE_READY:
            return jsonify({"ok": False,
                            "message": "首次全量同步未完成，关联明细查询暂未开放"}), 409
        sample_no = (request.args.get("sample_no") or "").strip()
        if not sample_no:
            return jsonify({"ok": False, "message": "请提供样品号"}), 400
        detail = target.query_sample(config.TARGET_DB, sample_no)
        if detail is None:
            return jsonify({"ok": False,
                            "message": f"分析库中不存在样品 {sample_no}"}), 404
        return jsonify({"ok": True, "sample": detail})

    @app.get("/api/source/txs")
    def source_txs():
        return jsonify({"txs": source.recent_txs(config.SOURCE_DB, limit=20)})

    @app.get("/api/target/samples")
    def target_samples():
        return jsonify({"samples": target.list_samples(config.TARGET_DB)})

    return app
