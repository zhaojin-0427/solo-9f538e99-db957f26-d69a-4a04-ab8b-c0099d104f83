# 地质化验数据同步平台（作业库 → 分析库）

Flask + SQLite 演示系统：把作业库中的样品与化验结果持续、可靠地装入分析库。
两库为**相互独立的 SQLite 文件**，通过"变更日志 + 有序重放"实现
**首次全量 + 持续增量**同步，满足事务级一致与断点续传。

## 快速开始

### Docker Compose（推荐）

```bash
docker compose up --build
# 浏览器打开 http://localhost:5000
```

- 两库分别存放在两个独立卷：`source-data`（作业库）、`target-data`（分析库）。
- 验证断点续传：执行若干更正后 `docker compose restart`，页面可见
  `last_tx_id` 与数据保持不变，新更正继续增量同步，**不会清库或重做全量**。

### 本地运行

```bash
pip install -r requirements.txt
python -m app            # http://localhost:5000 ，数据在 ./data/
```

### 端到端自测

```bash
pip install requests
python scripts/selftest.py
```

自测会真实启动服务进程，覆盖：查询门禁、全量期间源端并发更正、
增量增删改、删除不复活、故障注入（失败不推进/重试不重复）、
进程重启接续、禁止重复全量、自动更正持续装入，并在各阶段做
**源/目标两库逐行精确对账**。

## 演示路径（页面操作）

1. 打开首页，初始为"待发起全量"，关联明细查询返回 409（未开放）。
2. 点击 **① 发起首次全量同步** → 完成后进入"增量同步中"，查询开放。
3. 点击 **② 模拟一笔更正事务**：源端在一笔事务内改样品采样点并增删多条结果；
   约 1 秒内分析库可见，且两库对账标识保持"一致"。
4. **自动更正**开关：源端每 2.5 秒产生一笔更正，演示持续装入。
5. **注入下一次目标写入失败**：观察 `last_tx_id` 停滞、异常信息出现，
   随后自动重试成功——进度不丢、数据不重。

## 一致性设计要点

| 需求 | 实现 |
| --- | --- |
| 一笔事务更正样品并增删多条结果 | 源端 `apply_correction`：业务写入与变更日志（`tx_log`/`tx_ops`）同一 SQLite 事务提交 |
| 按源端提交顺序同步 | `tx_id` 为 rowid（非 AUTOINCREMENT），SQLite 单写者串行 ⇒ rowid 序即提交序；增量严格按 `tx_id` 升序重放 |
| 全量期间源端更正仍能完成 | 源库 WAL 模式，快照读事务不阻塞写事务 |
| 切换增量不漏更正 | 全量快照与 `high_water=MAX(tx_id)` 取自**同一读事务**；装载后先追平 `tx_id > high_water` 再开放查询 |
| 不重复结果 | 目标表主键约束 + `INSERT OR REPLACE`；`tx_id <= last_tx_id` 的事务跳过 |
| 删除记录不复活 | 删除作为日志操作整笔重放；目标永不出现源端不存在的记录（自测不变式断言） |
| 整笔生效、查询不见半笔 | 每笔源事务在分析库**一个事务**内应用全部操作并推进进度；查询在 `ready` 后开放 |
| 写入失败不推进进度 | 数据写入与 `last_tx_id` 更新同事务，失败整体回滚 |
| 重试不重复生效 | 幂等重放（upsert/delete + 已应用跳过），自测含重复重放验证 |
| 中断重启按持久进度接续 | `sync_state` 持久化于分析库；重启后 `ready` 直接续传；仅"首次全量从未成功"时才重做全量 |

## 架构

```
浏览器 ──► Flask (app/web.py) ──┬── 作业库 source.db（app/source.py）
                                │     samples / results / tx_log / tx_ops
                                └── 分析库 target.db（app/target.py）
                                      samples / results / sync_state
            后台同步引擎 app/syncer.py：idle → full_running → ready
```

- `app/source.py`：示例源。建库、播种、更正事务（手动/随机）、一致性快照、日志读取。
- `app/target.py`：分析库。进度状态机、全量装载（单事务）、整笔重放（幂等）。
- `app/syncer.py`：后台线程驱动全量→追平→增量；故障注入点；失败重试不推进。
- `app/web.py`：状态/操作/查询 API 与页面；自动更正发生器。

## API 摘要

| 方法/路径 | 说明 |
| --- | --- |
| `GET /` | 演示页面 |
| `GET /api/status` | 阶段、进度、对账、异常 |
| `POST /api/sync/full` | 发起首次全量（仅 `idle`，重复发起返回 409） |
| `POST /api/source/correct` | 源端更正事务（空 body 随机；可指定 `sample_no/site/adds/dels`） |
| `POST /api/source/auto` | 自动更正开关 `{enabled: bool}` |
| `POST /api/fault/inject` | 武装下一次目标写入失败 |
| `GET /api/query?sample_no=` | 关联明细查询（`ready` 前 409） |
| `GET /api/source/txs` / `GET /api/target/samples` | 源端事务日志 / 分析库样品一览 |

## 环境变量

`PORT`（默认 5000）、`DATA_DIR`（默认 `./data`）、`SOURCE_DB`/`TARGET_DB`（覆盖库文件路径）、
`SYNC_POLL_INTERVAL`（增量轮询秒，默认 0.5）、`AUTO_CORRECT_INTERVAL`（默认 2.5）、
`SEED_SAMPLES`（默认 20）、`SYNC_SNAPSHOT_DELAY_MS`（**测试钩子**，默认 0：
在全量快照后人为停顿，用于稳定复现"全量期间源端更正"的竞态窗口，生产保持 0）。

## 已知限制（演示级实现）

- 变更日志只增不减，未做归档/清理；数据规模为演示量级（全量装载为单事务批量写入）。
- 同步引擎为 Web 进程内单线程；若改用 gunicorn 部署必须 `--workers 1`。
- 演示源为本地 SQLite 文件直读；接入真实外部源时，把 `source.py` 的
  快照/日志读取替换为对应源端 API 即可，目标端重放逻辑不变。
