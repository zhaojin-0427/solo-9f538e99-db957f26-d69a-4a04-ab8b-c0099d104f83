"""同步引擎：后台线程驱动 全量 -> 追平 -> 增量 的状态机。

- 全量：源端一致性快照 -> 分析库单事务装载 -> 按日志追平快照后的变更 -> ready
- 增量：按 tx_id 升序逐笔重放，每笔一个目标事务（整笔生效，查询不见半笔）
- 重启：ready 直接从持久化的 last_tx_id 续传；首次成功前中断则重做全量
        （需求仅禁止“首次同步成功后”清库/重做全量）
"""
import logging
import threading
import time

from . import source, target

log = logging.getLogger("geosync.syncer")


class InjectedFault(Exception):
    """故障注入：模拟目标写入失败。"""


class FaultInjector:
    """内存开关：武装后，下一次目标写入前抛出 InjectedFault。"""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._armed = 0

    def arm(self, times: int = 1) -> None:
        with self._lock:
            self._armed += times
        log.warning("故障注入已武装 %d 次", times)

    @property
    def armed(self) -> int:
        with self._lock:
            return self._armed

    def maybe_fail(self, tx_id: int) -> None:
        with self._lock:
            if self._armed > 0:
                self._armed -= 1
                raise InjectedFault(
                    f"注入故障：目标写入失败（tx={tx_id} 未应用，进度不推进）"
                )


class Syncer(threading.Thread):
    def __init__(self, source_path: str, target_path: str,
                 injector: FaultInjector, poll_interval: float = 0.5,
                 snapshot_delay_ms: int = 0) -> None:
        super().__init__(name="geosync-syncer", daemon=True)
        self.source_path = source_path
        self.target_path = target_path
        self.injector = injector
        self.poll_interval = poll_interval
        self.snapshot_delay_ms = snapshot_delay_ms
        self._stop_event = threading.Event()
        self.last_error: str | None = None
        self.last_applied_tx: int = 0

    def stop(self) -> None:
        self._stop_event.set()

    # ------------------------------------------------------------------ #
    def run(self) -> None:
        state = target.get_state(self.target_path)
        log.info("同步引擎启动：phase=%s last_tx_id=%d（按持久进度接续）",
                 state["phase"], state["last_tx_id"])
        while not self._stop_event.is_set():
            try:
                state = target.get_state(self.target_path)
                if state["phase"] == target.PHASE_FULL:
                    self._do_full_sync()
                elif state["phase"] == target.PHASE_READY:
                    self._incremental_pass()
                else:  # idle：等待浏览器发起首次全量
                    self._stop_event.wait(self.poll_interval)
            except Exception as exc:  # 任何失败都不推进进度，下轮重试
                self.last_error = f"{type(exc).__name__}: {exc}"
                log.error("同步异常（进度未推进，将重试）: %s", self.last_error)
                self._stop_event.wait(max(1.0, self.poll_interval))

    # ------------------------------------------------------------------ #
    def _do_full_sync(self) -> None:
        log.info("开始首次全量同步 …")
        high, samples, results = source.read_snapshot(self.source_path)
        if self.snapshot_delay_ms > 0:  # 测试钩子：放大“全量期间源端更正”窗口
            log.warning("测试钩子：快照后停顿 %d ms", self.snapshot_delay_ms)
            self._stop_event.wait(self.snapshot_delay_ms / 1000.0)
        target.full_load(self.target_path, high, samples, results)
        self._incremental_pass()  # 追平快照之后提交的更正
        target.mark_ready(self.target_path)

    def _incremental_pass(self) -> None:
        """把 last_tx_id 之后的源端事务按提交顺序逐笔应用，直到追平。"""
        while not self._stop_event.is_set():
            last = target.get_state(self.target_path)["last_tx_id"]
            txs = source.read_txs_after(self.source_path, last, limit=500)
            if not txs:
                return
            for tx in txs:
                # 故障注入点：在目标写入前失败 -> 数据与进度均未落库
                self.injector.maybe_fail(tx["tx_id"])
                if target.apply_tx(self.target_path, tx):
                    self.last_applied_tx = tx["tx_id"]
                    log.info("已应用源端事务 tx=%d（%d 个操作）",
                             tx["tx_id"], len(tx["ops"]))
