"""集中配置：路径与运行参数均可通过环境变量覆盖，便于容器化与测试。"""
import os

_BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

DATA_DIR = os.environ.get("DATA_DIR", os.path.join(_BASE, "data"))
os.makedirs(DATA_DIR, exist_ok=True)

# 两库独立存放：作业库（源）与分析库（目标）是两个互不相干的 SQLite 文件。
SOURCE_DB = os.environ.get("SOURCE_DB", os.path.join(DATA_DIR, "source.db"))
TARGET_DB = os.environ.get("TARGET_DB", os.path.join(DATA_DIR, "target.db"))
for _p in (SOURCE_DB, TARGET_DB):
    os.makedirs(os.path.dirname(os.path.abspath(_p)), exist_ok=True)

PORT = int(os.environ.get("PORT", "5000"))

# 增量轮询间隔（秒）
POLL_INTERVAL = float(os.environ.get("SYNC_POLL_INTERVAL", "0.5"))

# 自动更正发生器间隔（秒）
AUTO_CORRECT_INTERVAL = float(os.environ.get("AUTO_CORRECT_INTERVAL", "2.5"))

# 示例源初始样品数
SEED_SAMPLES = int(os.environ.get("SEED_SAMPLES", "20"))

# 测试钩子：全量同步在“读取一致性快照之后、写入分析库之前”人为停顿的毫秒数，
# 用于在自测中稳定复现“全量期间源端发生更正”的竞态窗口。生产环境保持 0。
SNAPSHOT_DELAY_MS = int(os.environ.get("SYNC_SNAPSHOT_DELAY_MS", "0"))
