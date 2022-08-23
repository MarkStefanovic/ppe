from __future__ import annotations

import abc
import contextlib
import textwrap

import loguru
import psycopg2.pool
from psycopg2._psycopg import connection

from src import data
from src.adapter import config

__all__ = ("create_pool", "Db", "open_db")


def create_pool(
    *,
    connection_string: str = config.get_connection_string(),
    max_size: int = config.get_max_connections(),
) -> psycopg2.pool.ThreadedConnectionPool:
    return psycopg2.pool.ThreadedConnectionPool(
        3,
        max_size,
        dsn=connection_string,
    )


# noinspection PyBroadException
@contextlib.contextmanager
def _connect(*, pool: psycopg2.pool.ThreadedConnectionPool) -> connection:
    con = pool.getconn()
    try:
        yield con
    except BaseException:
        con.rollback()
        raise
    else:
        con.commit()
    finally:
        pool.putconn(con)


def open_db(*, pool: psycopg2.pool.ThreadedConnectionPool) -> Db:
    loguru.logger.info("Opening database...")

    return _Db(pool=pool)


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
    def __init__(self, *, pool: psycopg2.pool.ThreadedConnectionPool):
        self._pool = pool

        self._batch_id = self._create_batch()

    def cancel_running_jobs(self, *, reason: str) -> None:
        sql = "CALL ppe.cancel_running_jobs(p_reason := %(reason)s);"
        with self._pool.getconn() as con:
            with con.cursor() as cur:
                cur.execute(sql, {"reason": reason})

    def create_batch(self) -> int:
        sql = "SELECT * FROM ppe.create_batch();"
        with self._pool.getconn() as con:
            with con.cursor() as cur:
                cur.execute(sql)
                if row := cur.fetchone():
                    return row[0]
                raise Exception(f"ppe.create_job should have returned an int, but returned {row!r}.")

    def create_job(self, *, task: data.Task) -> data.Job:
        sql = "SELECT * FROM ppe.create_job(p_batch_id := %(batch_id)s, p_task_id := %(task_id)s);"
        with _connect(pool=self._pool) as con:
            with con.cursor() as cur:
                cur.execute(sql, {"batch_id": self._batch_id, "task_id": task.task_id})
                if row := cur.fetchone():
                    job_id = row[0]
                else:
                    raise Exception(f"ppe.create_job should have returned an int, but returned {row!r}.")
        return data.Job(job_id=job_id, batch_id=self._batch_id, task=task)

    def get_ready_tasks(self, /, n: int) -> list[data.Task]:
        # sql = textwrap.dedent("""
        #     SELECT
        #         t.task_id
        #     ,   t.task_name
        #     ,   t.cmd
        #     ,   t.task_sql
        #     ,   t.retries
        #     ,   t.timeout_seconds
        #     FROM ppe.get_ready_tasks(p_max_jobs := %(n)s) AS t;
        # """)
        sql = """
            SELECT
                t.task_id
            ,   t.task_name
            ,   t.cmd
            ,   t.task_sql
            ,   t.retries
            ,   t.timeout_seconds
            FROM ppe.task_queue AS t
            LIMIT %(n)s
        """
        with _connect(pool=self._pool) as con:
            with con.cursor() as cur:
                cur.execute(sql, {"n": n})
                result = cur.fetchall()
                return [
                    data.Task(
                        task_id=row[0],
                        name=row[1],
                        cmd=row[2],
                        sql=row[3],
                        retries=row[4],
                        timeout_seconds=row[5],
                    )
                    for row in result
                ]

    def log_batch_error(self, *, error_message: str) -> None:
        sql = "CALL ppe.log_batch_error(p_batch_id := %(batch_id)s, p_message := %(error_message)s);"
        with _connect(pool=self._pool) as con:
            with con.cursor() as cur:
                cur.execute(sql, {"batch_id": self._batch_id, "error_message": error_message})

    def log_job_error(self, *, job_id: int, return_code: int, error_message: str) -> None:
        sql = "CALL ppe.job_failed(p_job_id := %(job_id)s, p_message := %(error_message)s);"
        with _connect(pool=self._pool) as con:
            with con.cursor() as cur:
                cur.execute(sql, {"job_id": job_id, "error_message": error_message})

    def log_job_success(self, *, job_id: int, execution_millis: int) -> None:
        sql = "CALL ppe.job_completed_successfully(p_job_id := %(job_id)s, p_execution_millis := %(execution_millis)s);"
        with _connect(pool=self._pool) as con:
            with con.cursor() as cur:
                cur.execute(sql, {"job_id": job_id, "execution_millis": execution_millis})

    def _create_batch(self) -> int:
        sql = "SELECT * FROM ppe.create_batch();"
        with _connect(pool=self._pool) as con:
            with con.cursor() as cur:
                cur.execute(sql)
                if row := cur.fetchone():
                    return row[0]
                raise Exception(f"ppe.create_batch should have returned an int, but returned {row!r}.")


if __name__ == '__main__':
    d = open_db(pool=create_pool())
    for t in d.get_ready_tasks(n=5):
        print(t)
