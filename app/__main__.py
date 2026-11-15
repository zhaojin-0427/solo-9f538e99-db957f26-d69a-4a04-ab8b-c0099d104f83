"""入口：python -m app"""
import logging

from . import config
from .web import create_app

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

app = create_app()

if __name__ == "__main__":
    # 演示用开发服务器（threaded 即可满足演示并发）。
    # 生产可换 gunicorn，但必须单 worker（同步引擎为进程内单线程）。
    app.run(host="0.0.0.0", port=config.PORT, threaded=True)
