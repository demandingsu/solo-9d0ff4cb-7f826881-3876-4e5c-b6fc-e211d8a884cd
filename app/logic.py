"""核心业务：录入/更正整点明细、生成摘要、按配额规划并执行查询。

事务约定：
- 写操作（录入、更正）用 BEGIN IMMEDIATE，单事务提交，只在短写入期间持写锁。
- 生成摘要分两阶段（OCC）：
  1. 只读事务取一致快照并做整段聚合，全程不持写锁，更正请求不会被汇总阻塞；
  2. 短写事务（BEGIN IMMEDIATE）发布前重新核对每小时版本，任一已变化则整体回滚、
     抛 SummaryConflictError（不登记摘要），由接口层返回 409 并列明变化的小时。
- 查询用一条读事务（BEGIN），有效摘要集合与小时行数来自同一已提交快照；
  规划阶段只读 hours/summaries 元数据，确认不缺档、不超预算后才真正读取明细行。

内存约定：整段温度明细绝不加载进应用内存——摘要与明细段的计数都由 SQL 聚合完成，
应用侧每小时只拿到一行 (hour_ts, cnt, over)，内存开销与小时数成正比，与明细行数无关。
"""
from __future__ import annotations

import sqlite3
from typing import Callable, Optional

HOUR_SECONDS = 3600
OVER_TEMP_THRESHOLD = 8.0  # 超过 8℃ 判定为超温（严格大于）

# 与当前 hours 版本逐小时比对，任一不一致（或小时已被移除）则摘要失效
_VALID_SUMMARIES_SQL = """
SELECT s.id, s.start_ts, s.end_ts, s.total_count, s.over_count
FROM summaries s
WHERE s.start_ts >= ? AND s.end_ts <= ?
  AND NOT EXISTS (
      SELECT 1
      FROM summary_hours sh
      LEFT JOIN hours h ON h.hour_ts = sh.hour_ts
      WHERE sh.summary_id = s.id
        AND (h.version IS NULL OR h.version <> sh.version)
  )
ORDER BY s.id
"""


class HourExistsError(Exception):
    """该小时已有明细，应改用更正接口。"""


class SummaryConflictError(Exception):
    """发布摘要时发现统计所用的小时版本已变化，本次摘要不登记。"""

    def __init__(self, changed_ts: list[int]):
        super().__init__(f"{len(changed_ts)} hour(s) changed since summary stats")
        self.changed_ts = changed_ts


def _rollback_quietly(conn: sqlite3.Connection) -> None:
    try:
        conn.execute("ROLLBACK")
    except sqlite3.OperationalError:
        pass  # 事务已提交或未开启


class MissingHoursError(Exception):
    """区间内存在既无明细、也不被任何有效摘要覆盖的小时。"""

    def __init__(self, missing_ts: list[int]):
        super().__init__(f"missing {len(missing_ts)} hour(s)")
        self.missing_ts = missing_ts


class BudgetExceededError(Exception):
    """最优方案需要读取的明细行数仍超过上限。"""

    def __init__(self, required_rows: int):
        super().__init__(f"requires {required_rows} detail rows")
        self.required_rows = required_rows


def record_hour(conn: sqlite3.Connection, hour_ts: int, temps: list[float], *, overwrite: bool) -> int:
    """录入（overwrite=False）或更正（overwrite=True）某整点的全部明细，返回新版本号。

    更正是整小时替换：旧明细删除、版本号 +1，依赖旧版本生成的摘要随之失效。
    """
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = conn.execute("SELECT version FROM hours WHERE hour_ts = ?", (hour_ts,)).fetchone()
        if row is not None and not overwrite:
            raise HourExistsError()
        version = 1 if row is None else row["version"] + 1
        conn.execute("DELETE FROM readings WHERE hour_ts = ?", (hour_ts,))
        conn.executemany(
            "INSERT INTO readings (hour_ts, temperature) VALUES (?, ?)",
            [(hour_ts, t) for t in temps],
        )
        conn.execute(
            "INSERT INTO hours (hour_ts, version, row_count) VALUES (?, ?, ?) "
            "ON CONFLICT(hour_ts) DO UPDATE SET version = excluded.version, row_count = excluded.row_count",
            (hour_ts, version, len(temps)),
        )
        conn.execute("COMMIT")
        return version
    except Exception:
        _rollback_quietly(conn)
        raise


# 测试/并发自测用屏障：在摘要的只读汇总完成后、发布写事务开始前调用，
# 用于确定性地制造“汇总期间发起更正”的交错。默认空操作，生产路径无额外开销。
summary_publish_barrier: Optional[Callable[[int, int], None]] = None


def make_summary(conn: sqlite3.Connection, start_ts: int, end_ts: int) -> dict:
    """为齐备的整点区间生成摘要；任一小时缺明细则拒绝。

    两阶段提交，避免长汇总持写锁阻塞整点更正：
    阶段一（只读事务）：在同一已提交快照上核对齐备性并逐小时聚合，全程不持写锁；
    阶段二（短写事务）：重新核对版本，全部一致才登记摘要及其小时版本快照，
    否则抛 SummaryConflictError，已做的聚合结果整体丢弃、不落任何记录。
    """
    # ---------- 阶段一：只读快照 + SQL 聚合（不持写锁，更正可随时提交） ----------
    conn.execute("BEGIN")
    try:
        rows = conn.execute(
            "SELECT hour_ts, version, row_count FROM hours WHERE hour_ts >= ? AND hour_ts < ?",
            (start_ts, end_ts),
        ).fetchall()
        present = {r["hour_ts"]: r for r in rows}
        missing = [t for t in range(start_ts, end_ts, HOUR_SECONDS) if t not in present]
        if missing:
            raise MissingHoursError(missing)
        # 聚合下推给 SQLite：每个小时只返回一行计数，应用不加载任何温度明细行
        agg = {
            r["hour_ts"]: (r["cnt"], r["over"])
            for r in conn.execute(
                "SELECT hour_ts, COUNT(*) AS cnt, "
                "SUM(CASE WHEN temperature > ? THEN 1 ELSE 0 END) AS over "
                "FROM readings WHERE hour_ts >= ? AND hour_ts < ? GROUP BY hour_ts",
                (OVER_TEMP_THRESHOLD, start_ts, end_ts),
            ).fetchall()
        }
        total = sum(c for c, _ in agg.values())
        over = sum(o for _, o in agg.values())
        # 快照版本：发布时逐小时核对，保证登记的版本与统计所用明细一致
        snap_versions = {t: present[t]["version"] for t in range(start_ts, end_ts, HOUR_SECONDS)}
        conn.execute("COMMIT")
    except Exception:
        _rollback_quietly(conn)
        raise

    # 只读事务已结束：此刻更正请求可自由提交，不受汇总耗时影响
    if summary_publish_barrier is not None:
        summary_publish_barrier(start_ts, end_ts)

    # ---------- 阶段二：乐观发布，短写事务内重新核对小时版本 ----------
    conn.execute("BEGIN IMMEDIATE")
    try:
        cur_rows = conn.execute(
            "SELECT hour_ts, version FROM hours WHERE hour_ts >= ? AND hour_ts < ?",
            (start_ts, end_ts),
        ).fetchall()
        cur_versions = {r["hour_ts"]: r["version"] for r in cur_rows}
        changed = [
            t
            for t in range(start_ts, end_ts, HOUR_SECONDS)
            if cur_versions.get(t) != snap_versions[t]
        ]
        if changed:
            # 统计基于的小时版本已变化：拒绝登记，由接口层返回 409
            raise SummaryConflictError(changed)
        cur = conn.execute(
            "INSERT INTO summaries (start_ts, end_ts, total_count, over_count, created_at) "
            "VALUES (?, ?, ?, ?, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))",
            (start_ts, end_ts, total, over),
        )
        summary_id = cur.lastrowid
        conn.executemany(
            "INSERT INTO summary_hours (summary_id, hour_ts, version) VALUES (?, ?, ?)",
            [(summary_id, t, snap_versions[t]) for t in range(start_ts, end_ts, HOUR_SECONDS)],
        )
        conn.execute("COMMIT")
        return {"id": summary_id, "total_count": total, "over_count": over}
    except Exception:
        _rollback_quietly(conn)
        raise


def query_range(conn: sqlite3.Connection, start_ts: int, end_ts: int, max_detail_rows: int) -> dict:
    """在单个读事务（同一已提交状态）内规划并执行查询。

    返回 {"segments": [...], "total": int, "over": int, "detail_rows_read": int}。
    缺档抛 MissingHoursError、超预算抛 BudgetExceededError，均在读取明细之前。
    """
    conn.execute("BEGIN")  # 读事务：以下所有 SELECT 看到同一快照
    try:
        hours = {
            r["hour_ts"]: r
            for r in conn.execute(
                "SELECT hour_ts, version, row_count FROM hours WHERE hour_ts >= ? AND hour_ts < ?",
                (start_ts, end_ts),
            ).fetchall()
        }
        summaries = [dict(r) for r in conn.execute(_VALID_SUMMARIES_SQL, (start_ts, end_ts)).fetchall()]

        n = (end_ts - start_ts) // HOUR_SECONDS
        # 按起点归类摘要；同时标记哪些小时被有效摘要覆盖（用于缺档诊断）
        summaries_from: dict[int, list[tuple[int, dict]]] = {}
        covered = [False] * n
        for s in summaries:
            i = (s["start_ts"] - start_ts) // HOUR_SECONDS
            j = (s["end_ts"] - start_ts) // HOUR_SECONDS
            summaries_from.setdefault(i, []).append((j, s))
            for k in range(i, j):
                covered[k] = True

        # DP：dp[i] = 覆盖前 i 小时所需的最少明细行数；并列时取段数更少者
        INF = float("inf")
        dp = [INF] * (n + 1)
        segs = [INF] * (n + 1)
        choice: list[tuple | None] = [None] * (n + 1)
        dp[0], segs[0] = 0, 0
        for i in range(n):
            if dp[i] == INF:
                continue
            h = hours.get(start_ts + i * HOUR_SECONDS)
            if h is not None:  # 该小时按明细读取，成本 = 行数
                cand = (dp[i] + h["row_count"], segs[i] + 1)
                if cand < (dp[i + 1], segs[i + 1]):
                    dp[i + 1], segs[i + 1] = cand
                    choice[i + 1] = ("details", i, None)
            for j, s in summaries_from.get(i, []):  # 摘要段，明细成本为 0
                cand = (dp[i], segs[i] + 1)
                if cand < (dp[j], segs[j]):
                    dp[j], segs[j] = cand
                    choice[j] = ("summary", i, s)

        if dp[n] == INF:
            missing = [
                start_ts + k * HOUR_SECONDS
                for k in range(n)
                if (start_ts + k * HOUR_SECONDS) not in hours and not covered[k]
            ]
            raise MissingHoursError(missing)
        if dp[n] > max_detail_rows:
            raise BudgetExceededError(dp[n])

        # 回溯最优方案，并把相邻的明细小时合并为一段（成本不变）
        plan: list[tuple[int, int, str, dict | None]] = []
        j = n
        while j > 0:
            kind, i, s = choice[j]
            if kind == "details" and plan and plan[-1][2] == "details" and plan[-1][0] == j:
                plan[-1] = (i, plan[-1][1], "details", None)
            else:
                plan.append((i, j, kind, s))
            j = i
        plan.reverse()

        # 通过预算检查后才真正读取明细行
        segments = []
        total = over = rows_read = 0
        for i, j, kind, s in plan:
            seg_start = start_ts + i * HOUR_SECONDS
            seg_end = start_ts + j * HOUR_SECONDS
            if kind == "summary":
                seg_total, seg_over, summary_id = s["total_count"], s["over_count"], s["id"]
            else:
                hour_list = [start_ts + k * HOUR_SECONDS for k in range(i, j)]
                # 聚合下推给 SQLite：只取回各小时的计数，不把温度明细加载进应用内存
                agg_rows = conn.execute(
                    f"SELECT COUNT(*) AS cnt, "
                    f"SUM(CASE WHEN temperature > ? THEN 1 ELSE 0 END) AS over "
                    f"FROM readings WHERE hour_ts IN ({','.join('?' * len(hour_list))})",
                    (OVER_TEMP_THRESHOLD, *hour_list),
                ).fetchone()
                seg_total = agg_rows["cnt"]
                seg_over = agg_rows["over"]
                summary_id = None
                rows_read += seg_total
            total += seg_total
            over += seg_over
            segments.append(
                {
                    "start_ts": seg_start,
                    "end_ts": seg_end,
                    "source": kind,
                    "summary_id": summary_id,
                    "total_readings": seg_total,
                    "over_temp_readings": seg_over,
                }
            )
        assert rows_read == dp[n], "实际读取行数应与规划一致"
        conn.execute("COMMIT")
        return {"segments": segments, "total": total, "over": over, "detail_rows_read": rows_read}
    except Exception:
        _rollback_quietly(conn)
        raise


def list_hours(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute("SELECT hour_ts, version, row_count FROM hours ORDER BY hour_ts").fetchall()


def list_summaries(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT s.id, s.start_ts, s.end_ts, s.total_count, s.over_count, s.created_at,
               CASE WHEN EXISTS (
                   SELECT 1 FROM summary_hours sh
                   LEFT JOIN hours h ON h.hour_ts = sh.hour_ts
                   WHERE sh.summary_id = s.id
                     AND (h.version IS NULL OR h.version <> sh.version)
               ) THEN 0 ELSE 1 END AS valid
        FROM summaries s ORDER BY s.id
        """
    ).fetchall()
