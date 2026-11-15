FROM python:3.12-slim

WORKDIR /srv

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

ENV DATA_DIR=/data \
    PORT=5000 \
    SYNC_POLL_INTERVAL=0.5

# 持久化由 compose 的两个独立卷负责（source-data / target-data）
EXPOSE 5000

CMD ["python", "-m", "app"]
