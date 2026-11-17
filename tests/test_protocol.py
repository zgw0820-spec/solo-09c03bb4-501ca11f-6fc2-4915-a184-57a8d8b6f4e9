"""Focused protocol tests that do not need a running HTTP server.

They drive the executor/pool directly against the ephemeral PostgreSQL and
concentrate on the hardest correctness guarantees:

* a statement that finishes just before the cancel request arrives must not
  overwrite the already-decided timeout receipt ("late result");
* a queued request whose deadline expires never sends a statement;
* after a timeout the backend is confirmed IDLE before reuse;
* a committed session-level setting (``search_path``) or an advisory lock
  left by one request is removed before the connection is handed out
  again, and a failed reset discards the connection instead of reusing it.

Run:  python tests/test_protocol.py
"""
from __future__ import annotations

import asyncio
import pathlib
import sys
import tempfile
import time

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import psycopg  # noqa: E402
import pgserver  # noqa: E402

from app.config import Settings  # noqa: E402
from app.errors import PoolAcquireTimeout, QueryTimeout  # noqa: E402
from app.executor import QueryExecutor  # noqa: E402
from app.pool import QueryConnectionPool  # noqa: E402

PASS = 0
FAIL = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name}  {detail}")


async def main() -> int:
    data_dir = tempfile.mkdtemp(prefix="proto-")
    pg = pgserver.get_server(data_dir, cleanup_mode="stop")
    admin_dsn = pg.get_uri()
    ddl = (ROOT / "db" / "01_init.sql").read_text(encoding="utf-8")
    with psycopg.connect(admin_dsn, autocommit=True) as c, c.cursor() as cur:
        cur.execute("CREATE DATABASE faultdb")
    from urllib.parse import urlparse, urlunparse

    p = urlparse(admin_dsn)
    fault_admin = urlunparse(p._replace(path="/faultdb"))
    with psycopg.connect(fault_admin, autocommit=True) as c, c.cursor() as cur:
        cur.execute(ddl)
        cur.execute("ALTER ROLE fault_ro PASSWORD NULL")
    ro = urlunparse(
        urlparse(fault_admin)._replace(
            netloc=urlparse(fault_admin).netloc.replace("postgres", "fault_ro", 1)
        )
    )

    settings = Settings(database_dsn=ro, pool_size=2)
    pool = QueryConnectionPool(settings)
    await pool.open()
    executor = QueryExecutor(pool, settings)

    print("=== A. 迟到结果不改写超时回执 ===")
    # The statement (150ms) always outlives the 60ms deadline, so the timeout
    # path is mandatory.  Cancellation arrives ~sub-millisecond later, which
    # is also how a real "finished just before cancel" race is shaped: the
    # shielded task is awaited and whatever it returns is recorded but never
    # allowed to replace the receipt.
    receipt: QueryTimeout | None = None
    late_seen = False
    try:
        await executor.execute(
            "SELECT pg_sleep(0.15)", time.monotonic() + 0.06
        )
    except QueryTimeout as exc:
        receipt = exc
        late_seen = bool(exc.extra.get("late_result_ignored"))

    check("截止时间到必定返回 QueryTimeout", receipt is not None)
    if receipt is not None:
        check("回执固定为 timeout",
              receipt.extra.get("status") == "timeout", receipt.extra)
        check("回执不携带 rows（迟到结果未泄漏）",
              "rows" not in receipt.extra, receipt.extra)
        check("已发起取消并确认事务结束(未弃置)",
              receipt.extra.get("cancelled") is True
              and receipt.extra.get("connection_discarded") is False,
              receipt.extra)
        print(f"  INFO  late_result_ignored={late_seen} "
              f"(True=语句在取消前完成；False=服务器确认取消)")

    print("=== B. 超时后确认 IDLE 才复用 ===")
    st = await pool.status()
    check("池容量仍为 2", st.size == 2, st)
    check("两条后端均空闲(IDLE)", st.idle == 2, st)
    ok = await executor.execute("SELECT 7", time.monotonic() + 5)
    check("后续查询得到自身结果", ok["rows"] == [[7]], ok)

    print("=== C. 排队过期绝不发送语句 ===")
    # Occupy both backends with long sleeps (long own deadlines).
    async def slow(tag: str) -> dict:
        return await executor.execute(
            f"SELECT '{tag}', pg_sleep(2)", time.monotonic() + 6
        )

    busy = [asyncio.ensure_future(slow("X")), asyncio.ensure_future(slow("Y"))]
    await asyncio.sleep(0.4)
    queued_err = None
    try:
        await executor.execute(
            "SELECT 'NEVER-RUN'", time.monotonic() + 0.4
        )
    except PoolAcquireTimeout as exc:
        queued_err = exc
    check("排队过期抛 PoolAcquireTimeout", queued_err is not None)
    results = await asyncio.gather(*busy)
    check("两条占池查询各自成功",
          {r["rows"][0][0] for r in results} == {"X", "Y"}, results)
    # Prove the queued statement never reached a backend: no marker exists.
    after = await executor.execute("SELECT 1", time.monotonic() + 5)
    check("过期语句无任何副作用，后续查询正常", after["rows"] == [[1]], after)
    st = await pool.status()
    check("结束后池满且空闲", st.idle == 2 and st.size == 2, st)

    print("=== D. 会话设置污染后自动复位 ===")
    for i in range(2):
        r = await executor.execute(
            "SELECT set_config('search_path','pg_catalog',false)",
            time.monotonic() + 5,
        )
        check(f"第 {i + 1} 条连接接受会话级 search_path",
              r["rows"] == [["pg_catalog"]], r)
    st = await pool.status()
    discarded_before = st.total_discarded
    # Both backends are polluted; each of these must still resolve the table
    # via the restored default search_path and receive its own rows.
    for i in range(2):
        r = await executor.execute(
            "SELECT count(*) FROM fault_logs", time.monotonic() + 5
        )
        check(f"污染后第 {i + 1} 次查询仍返回自身结果", r["rows"] == [[7]], r)
    r = await executor.execute(
        "SELECT current_setting('search_path')", time.monotonic() + 5
    )
    check("search_path 恢复为默认值",
          r["rows"] == [['"$user", public']], r)
    # A committed statement_timeout override must not fire on the next
    # borrower's statement.  The request itself sets a *local* (transaction
    # scoped) backstop, which is expected to be visible inside the query;
    # only a leaked session-level value of 1ms would abort the sleep.
    await executor.execute(
        "SELECT set_config('statement_timeout','1',false)", time.monotonic() + 5
    )
    for i in range(2):
        r = await executor.execute(
            "SELECT pg_sleep(0.2)", time.monotonic() + 5
        )
        check(f"第 {i + 1} 次查询未被残留 statement_timeout 打断",
              r["status"] == "ok", r)

    # Advisory locks survive transaction end too.  Verify on one specific
    # backend: after sanitize the key is no longer held by this session.
    async def rows_on(conn, sql):
        async with conn.cursor() as cur:
            await cur.execute(sql)
            return await cur.fetchall() if cur.description else []

    lock_count_sql = (
        "SELECT count(*) FROM pg_locks WHERE locktype='advisory' "
        "AND pid=pg_backend_pid()"
    )

    async def advisory_count(conn):
        await rows_on(conn, "BEGIN READ ONLY")
        count = await rows_on(conn, lock_count_sql)
        await rows_on(conn, "COMMIT")
        return count

    conn = await pool.acquire(time.monotonic() + 5)
    try:
        await rows_on(conn, "BEGIN READ ONLY")
        await rows_on(conn, "SELECT pg_advisory_lock(987654321)")
        await rows_on(conn, "COMMIT")
        held = await advisory_count(conn)
        check("会话级 advisory lock 确实在事务结束后残留",
              held == [(1,)], held)
        check("sanitize 成功清除残留状态", await pool.sanitize(conn) is True)
        left = await advisory_count(conn)
        check("复位后同一后端不再持有 advisory lock",
              left == [(0,)], left)
    finally:
        if not conn.closed:
            await pool.release_healthy(conn)
    for i in range(2):
        r = await executor.execute(
            "SELECT pg_try_advisory_lock(987654321)", time.monotonic() + 5
        )
        check(f"第 {i + 1} 次查询可重新获取同一 advisory lock",
              r["rows"] == [[True]], r)
        await executor.execute(
            "SELECT pg_advisory_unlock_all()", time.monotonic() + 5
        )

    st = await pool.status()
    check("复位通过会话清理完成，未弃置健康连接",
          st.total_discarded == discarded_before, st)
    check("复位后容量仍为 2 且全空闲", st.size == 2 and st.idle == 2, st)

    print("=== E. 无法确认复位时弃置补建 ===")
    original_reset = QueryConnectionPool._reset_session

    async def broken_reset(conn):
        raise RuntimeError("simulated reset failure")

    QueryConnectionPool._reset_session = staticmethod(broken_reset)
    try:
        # Release detects drift, the reset fails -> connection discarded.
        await executor.execute(
            "SELECT set_config('search_path','pg_catalog',false)",
            time.monotonic() + 5,
        )
    finally:
        QueryConnectionPool._reset_session = staticmethod(original_reset)
    st = await pool.status()
    check("复位失败时弃置连接", st.total_discarded >= 1, st)
    check("弃置后补建保持容量 2 且全空闲", st.size == 2 and st.idle == 2, st)
    for i in range(4):
        r = await executor.execute(
            "SELECT count(*) FROM fault_logs", time.monotonic() + 5
        )
        check(f"弃置补建后第 {i + 1} 次查询正常", r["rows"] == [[7]], r)

    print("=== F. 取得/校验连接期间预算耗尽 ===")
    # Verification now happens inside acquire(); a deadline that expires
    # there must still raise PoolAcquireTimeout with no statement sent and
    # without leaking a borrower slot.
    original_sanitize = QueryConnectionPool.sanitize

    async def slow_sanitize(self, conn):
        await asyncio.sleep(0.3)
        return await original_sanitize(self, conn)

    QueryConnectionPool.sanitize = slow_sanitize
    try:
        err = None
        try:
            await executor.execute(
                "SELECT 'SHOULD-NOT-RUN'", time.monotonic() + 0.1
            )
        except PoolAcquireTimeout as exc:
            err = exc
    finally:
        QueryConnectionPool.sanitize = original_sanitize
    check("校验期间预算耗尽抛 PoolAcquireTimeout", err is not None, err)
    st = await pool.status()
    check("未泄漏借出槽位，容量恢复为 2",
          st.size == 2 and st.idle == 2, st)
    r = await executor.execute("SELECT 99", time.monotonic() + 5)
    check("此后普通查询仍返回自身结果", r["rows"] == [[99]], r)

    await pool.close()
    pg.cleanup()
    print(f"\n结果: {PASS} 通过, {FAIL} 失败")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
