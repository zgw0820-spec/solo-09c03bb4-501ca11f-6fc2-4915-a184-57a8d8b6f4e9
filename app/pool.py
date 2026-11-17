"""A fixed-size pool of **two** read-only PostgreSQL connections.

Design goals, mapped to the operational requirements:

* ``POOL_SIZE`` (default 2) physical connections at most.  A bounded
  semaphore serialises requests so a third request *waits* instead of
  opening another backend.
* Waiting for a free connection consumes the same deadline budget as
  execution (:meth:`QueryConnectionPool.acquire` takes an absolute
  monotonic deadline).  If the deadline passes while queued, no statement
  is ever sent on any connection.
* After a timeout the handler must prove the backend is idle again
  (``transaction_status == IDLE`` after a successful ``ROLLBACK``) before
  the connection is returned to the pool.  If that cannot be confirmed the
  connection is closed and discarded, and a replacement is built so the
  pool keeps its capacity of two.
* Session state must not leak between analysts.  ``SELECT`` can still call
  ``set_config('search_path', ...)`` (or ``pg_advisory_lock``), and a
  committed session-level setting survives an idle transaction.  Therefore
  every connection is released only after its session overrides are
  compared with the snapshot taken at connect time; drift is reset with
  ``DISCARD ALL`` and the original pool defaults re-applied.  If the reset
  cannot be verified the connection is discarded and replaced instead of
  being handed to the next borrower.
* Every connection is opened in a read-only, read-committed session.
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from dataclasses import dataclass

from psycopg import AsyncConnection
from psycopg.pq import TransactionStatus

from .config import Settings
from .errors import PoolAcquireTimeout

logger = logging.getLogger("querypool")

# All *session-level* overrides currently in effect.  ``source = 'session'``
# is empty for a freshly connected role (role/database defaults are reported
# as 'role'/'database'), so a connection whose values differ from the
# snapshot taken at connect time carries state left by a previous borrower.
# Value comparisons intentionally ignore the display unit: baseline and
# comparison are read the same way.
_SESSION_STATE_SQL = """
SELECT
    current_user::text AS current_user,
    (SELECT count(*)
       FROM pg_catalog.pg_locks
      WHERE locktype = 'advisory'
        AND pid = pg_catalog.pg_backend_pid()) AS advisory_locks,
    (SELECT pg_catalog.jsonb_object_agg(name, setting)
       FROM pg_catalog.pg_settings
      WHERE source = 'session') AS session_settings
"""

# Session defaults re-applied after ``DISCARD ALL``.  Prepared statements
# are disabled on pooled connections (``prepare_threshold=None``) because
# ``DISCARD ALL`` deallocates them, which would otherwise turn the next
# execution into ``InvalidSqlStatementName``.
_SESSION_DEFAULTS = (
    "SET default_transaction_read_only = on",
    "SET default_transaction_isolation = 'read committed'",
)


@dataclass
class PoolStatus:
    size: int
    idle: int
    in_use: int
    total_created: int
    total_discarded: int
    closed: bool


class QueryConnectionPool:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        # One slot per *borrower*.  Acquiring a slot guarantees the right to
        # use one physical backend, even if that backend must be (re)built.
        self._slots = asyncio.BoundedSemaphore(settings.pool_size)
        self._free: asyncio.Queue[AsyncConnection] = asyncio.Queue()
        # Number of physical backends currently tracked (idle + in use).
        # It can temporarily fall below pool_size when a rebuild fails.
        self._backends = 0
        self._closed = False
        self._lock = asyncio.Lock()
        self._total_created = 0
        self._total_discarded = 0

    # ------------------------------------------------------------------ #
    # Lifecycle
    # ------------------------------------------------------------------ #
    async def open(self) -> None:
        """Eagerly establish the full pool so failures surface at startup."""
        async with self._lock:
            for _ in range(self._settings.pool_size):
                conn = await self._make_connection()
                await self._free.put(conn)
        logger.info(
            "pool opened: size=%d created=%d",
            self._settings.pool_size,
            self._total_created,
        )

    async def close(self) -> None:
        async with self._lock:
            self._closed = True
            while True:
                try:
                    conn = self._free.get_nowait()
                except asyncio.QueueEmpty:
                    break
                self._backends -= 1
                await self._safe_close(conn)
            logger.info("pool closed")

    async def status(self) -> PoolStatus:
        idle = self._free.qsize()
        return PoolStatus(
            size=self._settings.pool_size,
            idle=idle,
            in_use=max(0, self._backends - idle),
            total_created=self._total_created,
            total_discarded=self._total_discarded,
            closed=self._closed,
        )

    # ------------------------------------------------------------------ #
    # Connection factory
    # ------------------------------------------------------------------ #
    async def _make_connection(self) -> AsyncConnection:
        """Open a fresh connection pinned to a read-only session."""
        # Start in autocommit so the session SETs below do not leave an
        # implicit transaction open (psycopg opens one on the first command
        # of a non-autocommit connection).  Switch back afterwards so every
        # request manages explicit BEGIN/COMMIT itself.
        conn = await AsyncConnection.connect(
            self._settings.database_dsn,
            connect_timeout=self._settings.connect_timeout,
            autocommit=True,
            prepare_threshold=None,
        )
        # Session-level safety net: even an accidental COMMIT cannot write.
        await conn.execute("SET default_transaction_read_only = on")
        await conn.execute(
            "SET default_transaction_isolation = 'read committed'"
        )
        conn._pool_session_baseline = await self._read_session_state(conn)
        # Async connections expose a setter coroutine, not a writable property.
        await conn.set_autocommit(False)
        assert conn.info.transaction_status == TransactionStatus.IDLE
        self._backends += 1
        self._total_created += 1
        logger.info(
            "connection created (backends=%d total_created=%d)",
            self._backends,
            self._total_created,
        )
        return conn

    async def _safe_close(self, conn: AsyncConnection) -> None:
        with contextlib.suppress(Exception):
            await conn.close()

    @staticmethod
    def _is_idle(conn: AsyncConnection) -> bool:
        """True only when the backend has no open/in-progress transaction."""
        return (not conn.closed) and (
            conn.info.transaction_status == TransactionStatus.IDLE
        )

    # ------------------------------------------------------------------ #
    # Acquire / release
    # ------------------------------------------------------------------ #
    async def acquire(self, deadline: float) -> AsyncConnection:
        """Reserve one connection, waiting at most until ``deadline``.

        Time spent here counts against the request's total deadline.  On
        timeout the semaphore slot is released and *nothing has been sent
        to the database* for this request.
        """
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise PoolAcquireTimeout("总截止时间已在排队前耗尽，语句未执行")

        try:
            await asyncio.wait_for(self._slots.acquire(), timeout=remaining)
        except TimeoutError as exc:
            raise PoolAcquireTimeout(
                "等待连接超过总截止时间，排队期间已过期，语句未执行"
            ) from exc

        try:
            conn = await self._take_idle_or_build(deadline)
        except Exception:
            # Any failure to obtain a physical backend frees the borrower
            # slot so the pool cannot deadlock after a failed rebuild.
            self._slots.release()
            raise
        if time.monotonic() >= deadline:
            # The budget expired while the connection was being taken or
            # its session verified.  Hand the (clean) connection back and
            # report an acquire timeout: nothing was sent to the database.
            await self._free.put(conn)
            self._slots.release()
            raise PoolAcquireTimeout(
                "总截止时间在取得连接前耗尽，排队期间已过期，语句未执行"
            )
        return conn

    async def _take_idle_or_build(self, deadline: float) -> AsyncConnection:
        # No lock across awaits: all mutations happen on one event loop and
        # each step is atomic between awaits, so no interleaving can corrupt
        # the counters or the queue.
        if self._free.qsize() > 0:
            conn = self._free.get_nowait()
        else:
            # Pool shrank after an earlier discarded connection: rebuild
            # lazily within the caller's remaining deadline budget.
            logger.info("no idle backend; lazily rebuilding within pool")
            conn = await self._make_connection()

        # Defence in depth: a connection handed out must still be IDLE and
        # carry exactly its connect-time session state.  Release paths
        # already validate this; a failure here (e.g. a session killed
        # while idle) means replace it before it reaches a borrower.
        if not self._is_idle(conn) or not await self.sanitize(conn):
            logger.warning("idle connection is unhealthy or polluted; replacing")
            await self._discard_locked(conn)
            conn = await self._make_connection()
        return conn

    @staticmethod
    async def _read_session_state(conn: AsyncConnection) -> tuple[str, int, dict]:
        """Capture current role, advisory-lock count and session overrides."""
        async with conn.cursor() as cur:
            await cur.execute(_SESSION_STATE_SQL)
            row = await cur.fetchone()
            if not conn.autocommit:
                # End the read-only transaction opened by the cursor context
                # so the connection is IDLE, like every other release path.
                await cur.execute("COMMIT")
        current_user, advisory_locks, settings = row
        return current_user, int(advisory_locks), settings or {}

    @classmethod
    def _state_is_clean(cls, baseline: tuple, current: tuple) -> bool:
        return baseline == current

    @staticmethod
    def _describe_state_drift(baseline: tuple, current: tuple) -> dict:
        """Names and before/after values of every GUC that drifted."""
        base_settings = baseline[2]
        settings = current[2]
        return {
            name: (base_settings.get(name), settings.get(name))
            for name in sorted(set(base_settings) | set(settings))
            if base_settings.get(name) != settings.get(name)
        }

    @staticmethod
    async def _ensure_idle(conn: AsyncConnection) -> bool:
        """ROLLBACK any open/failed transaction and confirm IDLE."""
        try:
            if conn.closed:
                return False
            if conn.info.transaction_status != TransactionStatus.IDLE:
                # Also recovers an INERROR transaction left by a cancelled
                # statement, after which autocommit cannot be toggled yet.
                await conn.execute("ROLLBACK")
            return (not conn.closed) and (
                conn.info.transaction_status == TransactionStatus.IDLE
            )
        except Exception:
            logger.exception("could not return connection to IDLE")
            return False

    @classmethod
    async def _reset_session(cls, conn: AsyncConnection) -> None:
        """Drop every session override and re-apply the pool defaults.

        ``DISCARD ALL`` must run outside a transaction block, so the
        connection is first returned to IDLE (a leaked tiny
        ``statement_timeout`` can leave the state probe's transaction
        aborted) and autocommit is enabled for the reset.  ``DISCARD ALL``
        also releases session advisory locks and resets
        ``search_path``/``role``/``statement_timeout`` etc.
        """
        if not await cls._ensure_idle(conn):
            raise RuntimeError("connection not IDLE before session reset")
        previous_autocommit = conn.autocommit
        await conn.set_autocommit(True)
        try:
            await conn.execute("DISCARD ALL")
            for statement in _SESSION_DEFAULTS:
                await conn.execute(statement)
        finally:
            # If anything above failed the connection may be INERROR; try
            # to recover it before restoring autocommit, otherwise leave it
            # for the caller to discard.
            with contextlib.suppress(Exception):
                await conn.execute("ROLLBACK")
            if conn.info.transaction_status == TransactionStatus.IDLE:
                await conn.set_autocommit(previous_autocommit)
        baseline = await cls._read_session_state(conn)
        conn._pool_session_baseline = baseline

    async def sanitize(self, conn: AsyncConnection) -> bool:
        """Restore the connect-time session state before reusing ``conn``.

        Returns ``True`` when the session provably matches its baseline and
        the backend is IDLE.  ``False`` means the connection must not be
        reused; the caller discards and replaces it.

        A request may legally run ``SELECT set_config('search_path',
        'pg_catalog', false)``; a ``false`` (session-level) change survives
        the request transaction and would make the next analyst's
        ``fault_logs`` query fail with ``UndefinedTable``.  This method
        detects any such drift plus leftover advisory locks, resets the
        session and re-verifies it.
        """
        baseline = getattr(conn, "_pool_session_baseline", None)
        if baseline is None:
            logger.warning("connection has no session baseline; cannot reuse")
            return False
        if conn.closed:
            return False
        try:
            if conn.info.transaction_status != TransactionStatus.IDLE:
                return False
            current = await self._read_session_state(conn)
            if self._state_is_clean(baseline, current):
                return True
            logger.warning(
                "session state drift detected (user=%s changed=%s "
                "advisory_locks=%s); resetting before reuse",
                current[0],
                self._describe_state_drift(baseline, current),
                current[1] - baseline[1],
            )
        except Exception:
            # The probe itself can be aborted by the very state we are
            # trying to detect (e.g. a leaked statement_timeout=1ms).
            # Treat it as drift and attempt a full reset.
            logger.warning("session state probe failed; attempting reset",
                           exc_info=True)
            if not await self._ensure_idle(conn):
                return False
        try:
            await self._reset_session(conn)
            if not await self._ensure_idle(conn):
                return False
            return self._state_is_clean(
                baseline, await self._read_session_state(conn)
            )
        except Exception:
            logger.exception("could not sanitize session state")
            return False

    async def release_healthy(self, conn: AsyncConnection) -> bool:
        """Return a connection that is IDLE *and* sanitized to the pool.

        Returns ``True`` when the connection was reused, ``False`` when it
        had to be discarded and replaced.
        """
        if not self._is_idle(conn) or not await self.sanitize(conn):
            # Never put an unverified or polluted connection back; rebuild.
            await self.discard_and_replace(conn)
            return False
        await self._free.put(conn)
        self._slots.release()
        return True

    async def discard_and_replace(self, conn: AsyncConnection) -> None:
        """Drop an unverifiable connection and build its replacement.

        The replacement is built *before* the slot is released so the pool
        usually offers the full capacity of two healthy backends afterwards.
        If the replacement cannot be built the slot is released anyway and
        the pool temporarily shrinks; the next acquire lazily restores it.
        """
        logger.warning(
            "discarding connection (closed=%s tx=%s) and rebuilding",
            conn.closed,
            conn.info.transaction_status,
        )
        await self._discard_locked(conn)
        try:
            fresh = await self._make_connection()
            await self._free.put(fresh)
        except Exception:
            logger.exception("failed to rebuild discarded connection; "
                             "pool will lazily rebuild on next acquire")
        finally:
            self._slots.release()

    async def _discard_locked(self, conn: AsyncConnection) -> None:
        await self._safe_close(conn)
        self._backends -= 1
        self._total_discarded += 1

    # ------------------------------------------------------------------ #
    # Timeout recovery primitives used by the request handler
    # ------------------------------------------------------------------ #
    async def confirm_idle_or_discard(self, conn: AsyncConnection) -> bool:
        """Best-effort proof that no statement is running anymore.

        Returns ``True`` only when the backend reports the IDLE transaction
        state.  Any failure means the connection must be discarded.
        """
        try:
            if conn.closed:
                return False
            status = conn.info.transaction_status
            if status != TransactionStatus.IDLE:
                # A cancelled statement leaves an aborted transaction;
                # ROLLBACK both ends it and proves the backend responds.
                await conn.execute("ROLLBACK")
                status = conn.info.transaction_status
            return (not conn.closed) and status == TransactionStatus.IDLE
        except Exception:
            logger.exception("could not confirm connection idle")
            return False
