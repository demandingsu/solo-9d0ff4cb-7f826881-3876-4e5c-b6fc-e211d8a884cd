# 冷库超温占比查询 API

冷库质检员在明细读取配额内查询任意整点区间的超温（>8℃）占比。
FastAPI + SQLite，查询时从同一已提交状态的有效摘要与小时明细中，
选出恰好覆盖全段、读取明细行数最少的组合。

## 启动

```bash
docker compose up --build
# 打开 http://localhost:8000/docs （Swagger UI，可直接操作全部接口）
```

本地开发（Python 3.11+）：

```bash
pip install -r requirements-dev.txt
uvicorn app.main:app --reload
pytest tests/ -v
```

数据库文件路径由环境变量 `COLDCHAIN_DB` 指定（compose 中为 `/data/coldchain.db`，挂在命名卷上）。

## 接口

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/hours/{hour}/readings` | 录入某 UTC 整点的非空温度明细；已有数据返回 409 |
| PUT | `/hours/{hour}/readings` | 更正该小时全部明细（整小时替换，版本 +1） |
| GET | `/hours` | 已录入小时一览（版本号、行数） |
| POST | `/summaries` | 为齐备的整点区间 `[start, end)` 生成摘要；缺小时返回 409；发布时版本已变化也返回 409（`summary_conflict`，列明变化小时，摘要不登记） |
| GET | `/summaries` | 摘要一览，含当前有效性 |
| POST | `/query` | 在 `max_detail_rows` 配额内查询 `[start, end)` 超温占比 |

时间一律为带时区的 UTC 整点（分秒为 0），区间含起点、不含终点。超温判定为严格大于 8℃。

## 实现

**数据模型**（`app/db.py`）

- `hours(hour_ts, version, row_count)`：每个已录入小时一行，`version` 每次更正 +1，`row_count` 为当前明细行数（规划阶段的成本元数据）。
- `readings(hour_ts, temperature)`：温度明细行。
- `summaries(start_ts, end_ts, total_count, over_count)` + `summary_hours(summary_id, hour_ts, version)`：摘要生成时快照每个小时的版本号。

**摘要失效**：更正某小时 → `hours.version` +1 → 查询时用 `NOT EXISTS(summary_hours 快照版本 ≠ hours 当前版本)` 判定有效性。
按该小时旧明细生成的摘要自动失效，不相关摘要不受影响（`app/logic.py` 的 `_VALID_SUMMARIES_SQL`）。

**摘要生成（两阶段 OCC，不阻塞更正）**：旧实现用 `BEGIN IMMEDIATE` 在一个写事务里完成
长区间聚合，持锁期间任何整点更正都拿不到写锁，忙等超时后未捕获的 `database is locked`
直接变成 500。现拆为两阶段（`app/logic.py: make_summary`）：

1. **只读阶段**（`BEGIN` 读事务）：在同一已提交快照上核对小时齐备性，并以
   `GROUP BY hour_ts` 的 SQL 聚合逐小时取计数——应用内存里每个小时只有一行
   `(hour_ts, cnt, over)`，整段温度明细绝不加载进应用内存。此阶段不持写锁，
   更正请求可随时提交，互不阻塞。
2. **发布阶段**（短 `BEGIN IMMEDIATE`）：重新读取区间内每个小时的当前版本，与只读阶段
   的快照版本逐小时比对。全部一致才登记 `summaries` 与 `summary_hours`；任一变化则
   整体回滚并抛 `SummaryConflictError`，接口返回 409（`reason=summary_conflict`，
   `changed_hours` 列出发生变化的 UTC 小时，最多 24 条并带 `truncated`），**不登记这份摘要**，
   客户端可在更正完成后重新发起。区间外的更正不参与版本比对，摘要照常发布。

**查询规划**（`app/logic.py: query_range`）

1. 整条查询在**一个读事务**内完成：有效摘要集合与小时行数来自同一已提交快照。
2. 规划只读元数据：以小时为格点做 DP，`dp[i]` = 覆盖前 i 小时所需最少明细行数
   （明细段成本 = 行数，摘要段成本 = 0；并列取段数更少者），回溯后相邻明细小时合并为一段。
3. **缺档 / 超预算在读取明细前返回**：`dp` 不可达 → 409 `missing_data`（列出缺档小时）；
   最优成本 > 配额 → 409 `budget_exceeded`（附所需行数）。
4. 通过检查后才读取明细，且读取同样下推为 SQL 聚合（`COUNT` / 条件 `SUM`，按小时一行），
   应用不持有温度明细行；响应标注每段来源（`summary` + `summary_id` 或 `details`）。

**健壮性**：自定义 422 处理器只回显 `type/loc/msg`，避免含 NaN 的非法输入在序列化错误体时变成 500；
每条连接设置 `busy_timeout=10000`，为发布阶段的瞬时写锁竞争兜底（长汇总已不再持写锁）。

## 实测

**单元/端到端测试**（`tests/test_api.py`，TestClient + 临时库 + 独立连接线程）：`12 passed`

- 录入校验：重复录入 409、空明细 / 非整点 / 缺时区 / NaN → 422
- 摘要有重叠（S1=[00,04)、S2=[02,06)）时查询 [00,06) 选最优组合
- 预算不足、缺档均在读明细前返回原因及所需行数/缺档小时
- 更正 h1 → S1 失效、S2 照用；更正后查询改用 S2+明细且反映新数据；
  重新生成 S1′ 后配额 0 可查；更正两个摘要共有的小时 → 全部失效退回全明细
- **汇总与更正并发交错**（确定性屏障插在“只读汇总完成、发布前”）：
  - 区间内更正：汇总未结束更正即成功（实测耗时 0.01s，不再卡到超时），
    发布返回 409 `summary_conflict` 且 `changed_hours` 精确列明变化小时，摘要不登记；
    随后配额内查询反映更正；重发成功，白盒核对 `summary_hours.version` 与 `hours.version` 逐小时一致
  - 区间外更正：照常提交，摘要 201 正常发布且保持有效
  - HTTP 层同样验证 409 路径（两个后台请求经 TestClient 门户串行处理）
- 单小时 2 万行明细：摘要计数正确，聚合由 SQL 完成，应用内存不随行数膨胀

**修复前对照**（旧实现 + 人为持锁 8s 模拟长汇总，独立脚本）：更正请求忙等 5.0s 后收到
`HTTP 500 database is locked`；同一长汇总场景在新实现下更正 0.01s 返回 200，
摘要发布返回 409 列明变化小时，重发 201。

**真实服务冒烟**（uvicorn + urllib，端口 8765/8767，临时库，脚本在仓库外 `/tmp/selftest`）：

```
场景 A（区间内更正 × 长汇总交错）
  更正 PUT -> 200 version=2，elapsed=0.01s（汇总仍在进行）
  摘要发布 -> 409 summary_conflict changed_hours=[01:00]，摘要总数不增
  查询 [00,04) 配额100 -> 全明细 11 笔/超温 3（更正已生效）
  重新发布 -> 201 {total:11, over:3}；配额 0 查询命中新摘要
  白盒：summary_hours 4 个小时的版本快照与 hours 当前版本完全一致
场景 B（区间外更正 × 汇总交错）
  h5 更正 -> 200 version=2；摘要 -> 201 且 valid=true
  查询 [00,06) -> 15 笔/超温 6（区间外更正已生效）
场景 C（大区间内存）
  单小时 100,000 行明细：摘要 0.12s 完成，计数 100000/100000，
  进程峰值 RSS ≈ 66 MiB（明细未加载进应用内存）
场景 D（模拟 8s 长汇总）
  汇总期间更正 -> 200，等待 0.01s；发布 -> 409；更正后重发 -> 201
```

## 余项

- 本环境无 Docker 守护进程（`docker`/`podman` 等均不存在），`docker compose up --build`
  未实机执行；`Dockerfile`/`docker-compose.yml` 为未改动的标准配置，应用已用同版本
  依赖经 uvicorn 实跑全部 HTTP 交错场景，`/docs` 与 OpenAPI schema 正常返回。
- 无鉴权与多租户；单 worker 跑 SQLite（写串行），如需并发写可换 PostgreSQL，模型与 SQL 基本可平移
  （两阶段 OCC 的版本核对 SQL 与 `GROUP BY` 聚合均为标准语法）。
- 摘要只增不删，长期运行需归档/清理策略；失效摘要目前保留供审计。
- 更正是整小时整体替换，不支持单条修改；`PUT` 对不存在的小时按建档处理（version=1）。
- 查询 DP 复杂度 O(小时数 + 摘要数)，超长区间（如数年）未做分片；缺档/冲突小时列表最多回显 24 条。
- 409 `summary_conflict` 是可重试冲突：客户端更正落库后重新 POST `/summaries` 即可；
  若同一区间持续被高频更正，调用方需自行限退。
- 并发测试通过 `logic.summary_publish_barrier` 测试钩子制造确定性交错；生产环境该钩子默认为 `None`，无额外开销。
