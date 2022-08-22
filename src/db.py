from __future__ import annotations

import abc
import functools
import logging
import textwrap

import psycopg_pool
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from src import config, data
from src.data import Job, Task

__all__ = ("Db", "open_db")

logger = logging.getLogger()


@functools.lru_cache(maxsize=1)
def _create_pool(
    *,
    connection_string: str = config.connection_string(),
    max_size: int = 10,
    max_lifetime: int = 3600,
) -> ConnectionPool:
    return ConnectionPool(
        connection_string,
        max_size=max_size,
        max_lifetime=max_lifetime,
    )


@functools.lru_cache(maxsize=1)
def open_db(
    *,
    pool: psycopg_pool.ConnectionPool = _create_pool(),
    connection_timeout_seconds: int = 10,
) -> Db:
    logger.info("Opening database...")

    return _Db(pool=pool, connection_timeout_seconds=connection_timeout_seconds)


class Db(abc.ABC):
    @abc.abstractmethod
    def cancel_running_jobs(self, *, reason: str) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def create_batch(self) -> int:
        raise NotImplementedError

    @abc.abstractmethod
    def create_job(self, *, task: data.Task) -> data.Job:
        raise NotImplementedError

    @abc.abstractmethod
    def get_ready_tasks(self, *, n: int) -> list[data.Task]:
        raise NotImplementedError

    @abc.abstractmethod
    def log_batch_error(self, *, error_message: str) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def log_job_error(self, *, job_id: int, return_code: int, error_message: str) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def log_job_success(self, *, job_id: int, execution_millis: int) -> None:
        raise NotImplementedError


# noinspection SqlDialectInspection
class _Db(Db):
    def __init__(self, *, pool: psycopg_pool.ConnectionPool, connection_timeout_seconds: int):
        self._pool = pool
        self._connection_timeout_seconds = connection_timeout_seconds

        self._batch_id = self._create_batch()

    def cancel_running_jobs(self, *, reason: str) -> None:
        sql = "CALL ppe.cancel_running_jobs(p_reason := %(reason)s);"
        with self._pool.connection() as con:
            with con.cursor() as cur:
                cur.execute(sql, {"reason": reason})

    def create_batch(self) -> int:
        sql = "SELECT * FROM ppe.create_job();"
        with self._pool.connection() as con:
            with con.cursor() as cur:
                cur.execute(sql)
                if row := cur.fetchone():
                    return row[0]
                raise Exception(f"ppe.create_job should have returned an int, but returned {row!r}.")

    def create_job(self, *, task: data.Task) -> data.Job:
        sql = "SELECT * FROM ppe.create_job(p_batch_id := %(batch_id)s, p_task_id := %(task_id)s);"
        with self._pool.connection() as con:
            with con.cursor() as cur:
                cur.execute(sql, {"batch_id": self._batch_id, "task_id": task.task_id})
                if row := cur.fetchone():
                    job_id = row[0]
                else:
                    raise Exception(f"ppe.create_job should have returned an int, but returned {row!r}.")
        return Job(job_id=job_id, batch_id=self._batch_id, task=task)

    def get_ready_tasks(self, /, n: int) -> list[data.Task]:
        # array_to_string ~~~ ... split("$$$") is used to get around a bug with psycopg3 where only the first element of the array is returned
        sql = textwrap.dedent("""
            SELECT
                t.task_id
            ,   t.task_name
            ,   array_to_string(t.cmd, '~~~') AS cmd
            ,   t.task_sql
            ,   t.retries
            ,   t.timeout_seconds
            FROM ppe.get_ready_tasks(p_max_jobs := 5) AS t;
        """)
        with self._pool.connection() as con:
            with con.cursor(row_factory=dict_row) as cur:
                cur.execute(sql, {"n": n})
                return [
                    Task(
                        task_id=row["task_id"],
                        name=row["task_name"],
                        cmd=None if not row["cmd"] else row["cmd"].split("~~~"),
                        sql=row["task_sql"],
                        retries=row["retries"],
                        timeout_seconds=row["timeout_seconds"],
                    )
                    for row in cur.fetchall()
                ]

    def log_batch_error(self, *, error_message: str) -> None:
        sql = "CALL ppe.log_batch_error(p_batch_id := %(batch_id)s, p_message := %(error_message)s);"
        with self._pool.connection() as con:
            with con.cursor() as cur:
                cur.execute(sql, {"batch_id": self._batch_id, "error_message": error_message})

    def log_job_error(self, *, job_id: int, return_code: int, error_message: str) -> None:
        sql = "CALL ppe.job_failed(p_job_id := %(job_id)s, p_message := %(error_message)s);"
        with self._pool.connection() as con:
            with con.cursor() as cur:
                cur.execute(sql, {"job_id": job_id, "error_message": error_message})

    def log_job_success(self, *, job_id: int, execution_millis: int) -> None:
        sql = "CALL ppe.job_completed_successfully(p_job_id := %(job_id)s, p_execution_millis := %(execution_millis)s);"
        with self._pool.connection() as con:
            with con.cursor() as cur:
                cur.execute(sql, {"job_id": job_id, "execution_millis": execution_millis})

    def _create_batch(self) -> int:
        sql = "SELECT * FROM ppe.create_batch();"
        with self._pool.connection() as con:
            with con.cursor() as cur:
                cur.execute(sql)
                if row := cur.fetchone():
                    return row[0]
                raise Exception(f"ppe.create_batch should have returned an int, but returned {row!r}.")
