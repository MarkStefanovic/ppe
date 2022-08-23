import datetime
import multiprocessing as mp
import queue
import subprocess
import threading
import time
import typing

import loguru
import psycopg

from src import adapter, data

__all__ = ("Runner", "start")


def start(
    *,
    job_queue: queue.Queue[data.Task],
    db: adapter.db.Db,
    max_simultaneous_jobs: int = adapter.config.get_max_simultaneous_jobs(),
    connection_str: str = adapter.config.get_connection_string(),
) -> threading.Thread:
    loguru.logger.info("Starting job runner...")

    job_runner = Runner(
        db=db,
        task_queue=job_queue,
        max_simultaneous_jobs=max_simultaneous_jobs,
        connection_str=connection_str,
    )
    th = threading.Thread(name="runner", target=job_runner.run, daemon=True)
    th.start()
    return th


class Runner:
    def __init__(
        self,
        *,
        db: adapter.db.Db,
        task_queue: queue.Queue[data.Task],
        max_simultaneous_jobs: int = adapter.config.get_max_simultaneous_jobs(),
        connection_str: str = adapter.config.get_connection_string(),
    ):
        self._db = db
        self._task_queue = task_queue
        self._max_simultaneous_jobs = max_simultaneous_jobs
        self._connection_str = connection_str

    def run(self) -> None:
        while True:
            try:
                task = self._task_queue.get_nowait()
                job = self._db.create_job(task=task)
                loguru.logger.info(f"Starting [{job.task.name}]...")
                result = _run_job_with_retry(
                    connection_str=self._connection_str,
                    job=job,
                    retries_so_far=0,
                )
                if result.is_err:
                    loguru.logger.info(f"[{job.task.name}] failed: {result.error_message}.")
                else:
                    loguru.logger.info(f"[{job.task.name}] completed successfully.")
                self._add_result(result=result)
            except queue.Empty:
                loguru.logger.debug("Queue is empty")
            except Exception as e:
                loguru.logger.exception(e)
                self._db.log_batch_error(error_message=str(e))

            time.sleep(1)

    def _add_result(self, *, result: data.JobResult) -> None:
        if result.is_err:
            self._db.log_job_error(
                job_id=result.job.job_id,
                return_code=result.return_code or -1,
                error_message=result.error_message or "No error message was provided.",
            )
        else:
            self._db.log_job_success(
                job_id=result.job.job_id,
                execution_millis=typing.cast(int, result.execution_millis),
            )


def _run_job_with_retry(*, connection_str: str, job: data.Job, retries_so_far: int = 0) -> data.JobResult:
    loguru.logger.debug(f"Running [{job.task.name}]")
    try:
        result = _run_job_in_process(connection_str=connection_str, job=job, retries=retries_so_far)
        if result.is_err:
            if job.task.retries > retries_so_far:
                loguru.logger.info(f"Retrying [{job.task.name}] ({retries_so_far + 1}/{job.task.retries})...")
                return _run_job_with_retry(connection_str=connection_str, job=job, retries_so_far=retries_so_far + 1)
            return result
        return result
    except Exception as e:
        if job.task.retries > retries_so_far:
            loguru.logger.info(f"Retrying [{job.task.name}] ({retries_so_far + 1}/{job.task.retries})...")
            return _run_job_with_retry(connection_str=connection_str, job=job, retries_so_far=retries_so_far + 1)
        return data.JobResult.error(job=job, code=-1, message=str(e), retries=retries_so_far)
    finally:
        loguru.logger.debug(f"[{job.task.name}] finished.")


def _run_job_in_process(*, connection_str: str, job: data.Job, retries: int) -> data.JobResult:
    result_queue: "mp.Queue[data.JobResult]" = mp.Queue()
    try:
        p = mp.Process(target=_run, args=(job, connection_str, result_queue, retries))
        p.start()
        result = result_queue.get(block=True, timeout=job.task.timeout_seconds)
        p.join()
        return result
    except queue.Empty:
        loguru.logger.error(f"[{job.task.name}] timed out after {job.task.timeout_seconds} seconds.")
        return data.JobResult.error(job=job, code=-1, message=f"[{job.task.name}] timed out after {job.task.timeout_seconds} seconds.", retries=retries)
    except Exception as e:
        loguru.logger.exception(e)
        return data.JobResult.error(job=job, code=-1, message=str(e), retries=retries)
    finally:
        result_queue.close()


def _run(job: data.Job, connection_str: str, result_queue: "mp.Queue[data.JobResult]", retries: int, /) -> None:
    if job.task.cmd:
        _run_cmd(job=job, result_queue=result_queue, retries=retries)
    else:
        _run_sql(job=job, connection_str=connection_str, result_queue=result_queue, retries=retries)


def _run_cmd(*, job: data.Job, result_queue: "mp.Queue[data.JobResult]", retries: int) -> None:
    start = datetime.datetime.now()

    result = data.JobResult.error(job=job, code=-1, message=f"[{job.task.name}] never ran.", retries=retries)
    try:
        proc_result = subprocess.run(typing.cast(list[str], job.task.cmd), capture_output=True, timeout=job.task.timeout_seconds)
        execution_millis = int((datetime.datetime.now() - start).total_seconds() * 1000)
        if proc_result.returncode:
            result = data.JobResult.error(job=job, code=proc_result.returncode, message=str(proc_result.stderr), retries=retries)
        else:
            result = data.JobResult.success(job=job, execution_millis=execution_millis, retries=retries)
    except Exception as e:
        result = data.JobResult.error(job=job, code=-1, message=str(e), retries=retries)
    finally:
        loguru.logger.debug(f"Finished running {job}.")
        result_queue.put(result)


def _run_sql(*, job: data.Job, connection_str: str, result_queue: "mp.Queue[data.JobResult]", retries: int) -> None:
    start = datetime.datetime.now()

    result = data.JobResult.error(job=job, code=-1, message=f"[{job.task.name}] never ran.", retries=retries)
    try:
        with psycopg.connect(connection_str, autocommit=True) as con:
            con.execute(typing.cast(str, job.task.sql))

        result = data.JobResult.success(
            job=job,
            execution_millis=int((datetime.datetime.now() - start).total_seconds() * 1000),
            retries=retries,
        )
    except Exception as e:
        result = data.JobResult.error(job=job, code=-1, message=str(e), retries=retries)
    finally:
        result_queue.put(result)
