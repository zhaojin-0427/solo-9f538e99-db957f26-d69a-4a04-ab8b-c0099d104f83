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
python scripts/selftest.py        # 同步平台原有 7 阶段回归自测
python scripts/undo_selftest.py   # 更正撤销功能专项自测（30 项检查）
```

自测会真实启动服务进程。`selftest.py` 覆盖：查询门禁、全量期间源端并发更正、
增量增删改、删除不复活、故障注入（失败不推进/重试不重复）、进程重启接续、
禁止重复全量、自动更正持续装入，并在各阶段做**源/目标两库逐行精确对账**。
`undo_selftest.py` 覆盖：前后值识别（改/增/删）、撤销恢复与增量同步、
冲突拒绝（含改回原值、删后重建）、其他项不阻碍撤销、并发与重启幂等、
补偿事务不可撤销、旧事务不可撤销、失败不变与旧卷兼容。

## 演示路径（页面操作）

1. 打开首页，初始为"待发起全量"，关联明细查询返回 409（未开放）。
2. 点击 **① 发起首次全量同步** → 完成后进入"增量同步中"，查询开放。
3. 点击 **② 模拟一笔更正事务**：源端在一笔事务内改样品采样点并增删多条结果；
   约 1 秒内分析库可见，且两库对账标识保持"一致"。
4. **自动更正**开关：源端每 2.5 秒产生一笔更正，演示持续装入。
5. **注入下一次目标写入失败**：观察 `last_tx_id` 停滞、异常信息出现，
   随后自动重试成功——进度不丢、数据不重。
6. **更正撤销**（见下节）：在"更正撤销"卡片输入源事务号 → 查看前后值 →
   确认撤销；或在"最近事务日志"里直接点可撤销事务的"可撤销"按钮。

## 更正撤销（误改回退）

数据员误改采样点或检测值后，可按**源事务号**整笔撤销该笔更正，同时保留
该笔之后发生的其他修改。

### 规则与语义

- **仅支持启用后的普通更正**：撤销功能上线后提交的更正，在同一事务内把
  每个受影响项的提交前后实际值写入 `undo_change`；旧卷里功能启用前的事务
  没有这些记录，页面/API 标明"不可撤销"，原更正与同步链路照常可用。
- **以采样点 + 各检测项目为单位识别实际变化**：改值、新增（前缺失→后有值）、
  删除（前有值→后缺失）都精确记录；新增的表通过 `CREATE TABLE IF NOT EXISTS`
  在启动时自动补建，**直接兼容已有卷**。
- **冲突整笔拒绝**：任一受影响项在该更正之后又被其他事务改过——**即使改回
  原值、或删除后重建**——也整笔拒绝并列明冲突项及后续事务号；同样品其他
  未受影响项的变化不阻碍撤销。预览与确认都判断，**确认时在写事务内重新判断**。
- **撤销 = 一笔新的补偿事务**：把各受影响项恢复为该更正提交前的值或原缺失
  状态，其余修改原样保留。补偿事务是普通 `tx_log/tx_ops`，**经原增量链路
  整笔进入分析库**，水位 `last_tx_id` 只增不减（不回退）。
- **原子与幂等**：撤销登记（`undo_record`）、业务恢复、补偿日志
  （`tx_log/tx_ops/undo_change`）在同一 SQLite 事务提交，失败整体回滚、
  数据不变；并发或重启后重复撤销返回**同一个补偿事务号**，不重复补偿。
- **补偿事务不可撤销**。

### 数据表（作业库新增）

| 表 | 作用 |
| --- | --- |
| `undo_change(tx_id, entity, sample_no, item, before_state, after_state)` | 每笔普通更正各受影响项的提交前后状态（NULL=缺失，JSON 存值），同事务写入 |
| `undo_record(original_tx_id PK, compensation_tx_id UNIQUE)` | 原事务→补偿事务登记；主键与唯一约束共同保证幂等 |

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
浏览器 ──► Flask (app/web.py) ──┬── 作业库 source.db（app/source.py + app/undo.py）
                                │     samples / results / tx_log / tx_ops
                                │     undo_change / undo_record（更正撤销）
                                └── 分析库 target.db（app/target.py）
                                      samples / results / sync_state
            后台同步引擎 app/syncer.py：idle → full_running → ready
```

- `app/source.py`：示例源。建库、播种、更正事务（手动/随机）、一致性快照、日志读取、撤销前后值记录（`undo_change`）。
- `app/undo.py`：更正撤销服务。前后值读取、冲突检测、补偿事务构建、原子提交与幂等登记。
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
| `GET /api/undo/preview?tx_id=` | 按源事务号查看前后值、可撤销性与冲突（只读，不改数据） |
| `POST /api/undo/confirm` | 确认撤销，body `{tx_id}`；原子提交补偿事务，重复撤销幂等返回同号 |
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
