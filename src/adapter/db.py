from __future__ import annotations

import contextlib
import threading

import loguru
import psycopg2.pool
from psycopg2._psycopg import connection

from src import data

__all__ = ("create_batch", "create_pool", "Db", "open_db")


def create_pool(
    *,
    connection_str: str,
    max_size: int,
) -> psycopg2.pool.ThreadedConnectionPool:
    return psycopg2.pool.ThreadedConnectionPool(3, max_size, dsn=connection_str)


# noinspection PyBroadException
@contextlib.contextmanager
def _connect(*, pool: psycopg2.pool.ThreadedConnectionPool) -> connection:
    con: connection = pool.getconn()
    try:
        yield con
    except BaseException:
        if con.closed == 0:
            con.rollback()
        raise
    else:
        if con.closed == 0:
            con.commit()
    finally:
        if con.closed == 0:
            pool.putconn(con)


def open_db(*, batch_id: int, pool: psycopg2.pool.ThreadedConnectionPool, days_logs_to_keep: int) -> data.Db:
    loguru.logger.info("Opening database...")

    return Pg(batch_id=batch_id, pool=pool, days_logs_to_keep=days_logs_to_keep)


# noinspection SqlDialectInspection
def create_batch(*, pool: psycopg2.pool.ThreadedConnectionPool) -> int:
    with _connect(pool=pool) as con:
        with con.cursor() as cur:
            cur.execute("SELECT * FROM ppe.create_batch();")
            if row := cur.fetchone():
                return row[0]
            raise Exception(f"ppe.create_batch should have returned an int, but returned {row!r}.")


# noinspection SqlDialectInspection
class Pg(data.Db):
    def __init__(
        self,
        *,
        batch_id: int,
        pool: psycopg2.pool.ThreadedConnectionPool,
        days_logs_to_keep: int,
    ):
        self._batch_id = batch_id
        self._pool = pool
        self._days_logs_to_keep = days_logs_to_keep

        self._lock = threading.Lock()

    def cancel_running_jobs(self, *, reason: str) -> None:
        with self._lock:
            with self._pool.getconn() as con:
                with con.cursor() as cur:
                    cur.execute(
                        "CALL ppe.cancel_running_jobs(p_reason := %(reason)s);",
                        {"reason": reason},
                    )

    def delete_old_logs(self) -> None:
        loguru.logger.debug("Deleting old logs...")
        with self._lock:
            with _connect(pool=self._pool) as con:
                with con.cursor() as cur:
                    cur.execute(
                        "CALL ppe.delete_old_log_entries(p_current_batch_id := %(batch_id)s, p_days_to_keep := %(days_to_keep)s)",
                        {"batch_id": self._batch_id, "days_to_keep": self._days_logs_to_keep},
                    )
        loguru.logger.debug("Finished deleting old logs.")

    def get_ready_job(self) -> data.Job | None:
        with self._lock:
            with _connect(pool=self._pool) as con:
                with con.cursor() as cur:
                    cur.execute("""
                        SELECT
                            t.task_id
                        ,   t.task_name
                        ,   t.tool
                        ,   t.tool_args
                        ,   t.task_sql
                        ,   t.retries
                        ,   t.timeout_seconds
                        FROM ppe.get_ready_task() AS t;
                    """)
                    if row := cur.fetchone():
                        task = data.Task(
                            task_id=row[0],
                            name=row[1],
                            tool=row[2],
                            tool_args=row[3],
                            sql=row[4],
                            retries=row[5],
                            timeout_seconds=row[6],
                        )
                        cur.execute(
                            "SELECT * FROM ppe.create_job(p_batch_id := %(batch_id)s, p_task_id := %(task_id)s);",
                            {"batch_id": self._batch_id, "task_id": task.task_id},
                        )
                        if row := cur.fetchone():
                            job_id = row[0]
                        else:
                            raise Exception(f"ppe.create_job should have returned an int, but returned {row!r}.")
                        return data.Job(job_id=job_id, batch_id=self._batch_id, task=task)
        return None

    def log_batch_info(self, *, message: str) -> None:
        with _connect(pool=self._pool) as con:
            with con.cursor() as cur:
                cur.execute(
                    "CALL ppe.log_batch_info(p_batch_id := %(batch_id)s, p_message := %(message)s);",
                    {"batch_id": self._batch_id, "message": message},
                )

    def log_batch_error(self, *, error_message: str) -> None:
        with _connect(pool=self._pool) as con:
            with con.cursor() as cur:
                cur.execute(
                    "CALL ppe.log_batch_error(p_batch_id := %(batch_id)s, p_message := %(error_message)s);",
                    {"batch_id": self._batch_id, "error_message": error_message},
                )

    def log_job_error(self, *, job_id: int, return_code: int, error_message: str) -> None:
        with _connect(pool=self._pool) as con:
            with con.cursor() as cur:
                cur.execute(
                    "CALL ppe.job_failed(p_job_id := %(job_id)s, p_message := %(error_message)s);",
                    {"job_id": job_id, "error_message": error_message},
                )

    def log_job_success(self, *, job_id: int, execution_millis: int) -> None:
        with _connect(pool=self._pool) as con:
            with con.cursor() as cur:
                cur.execute(
                    "CALL ppe.job_completed_successfully(p_job_id := %(job_id)s, p_execution_millis := %(execution_millis)s);",
                    {"job_id": job_id, "execution_millis": execution_millis},
                )

    def update_queue(self) -> None:
        loguru.logger.debug("Updating queue...")
        with self._lock:
            with _connect(pool=self._pool) as con:
                cur: psycopg2.cursor
                with con.cursor() as cur:
                    cur.execute("CALL ppe.update_queue();")
        loguru.logger.debug("Finished updating queue.")

    def update_task_issues(self) -> None:
        loguru.logger.debug("Updating task issues...")
        with self._lock:
            with _connect(pool=self._pool) as con:
                cur: psycopg2.cursor
                with con.cursor() as cur:
                    cur.execute("CALL ppe.update_task_issues();")
        loguru.logger.debug("Finished updating task issues.")
