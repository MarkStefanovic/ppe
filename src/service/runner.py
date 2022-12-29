from __future__ import annotations

import datetime
import multiprocessing as mp
import pathlib
import queue
import subprocess
import threading
import time
import typing

import psycopg2
from loguru import logger

from src import data

__all__ = ("Runner",)


class Runner(threading.Thread):
    def __init__(
        self,
        *,
        db: data.Db,
        connection_str: str,
        tool_dir: pathlib.Path,
        cancel: threading.Event,
    ):
        super().__init__()

        self._db = db
        self._connection_str = connection_str
        self._tool_dir = tool_dir
        self._cancel = cancel

        self._e: Exception | None = None

    def error(self) -> Exception | None:
        return self._e

    def join(self, timeout: float | None = None) -> None:
        super().join()

        logger.info("Runner stopped.")

        # reraise exception in main thread
        if self._e is not None:
            raise self._e

    def run(self) -> None:
        while not self._cancel.is_set():
            try:
                job = self._db.get_ready_job()
                if job is not None:
                    logger.info(f"Starting [{job.task.name}]...")

                    result = _run_job_with_retry(
                        connection_str=self._connection_str,
                        tool_dir=self._tool_dir,
                        job=job,
                        retries_so_far=0,
                    )

                    _add_result(db=self._db, result=result)
            except queue.Empty:
                logger.debug("Queue is empty")
            except Exception as e:
                self._e = e
                logger.exception(e)
                self._db.log_batch_error(error_message=str(e))
                self._cancel.set()

            time.sleep(1)


def _add_result(*, db: data.Db, result: data.JobResult) -> None:
    if result.is_err:
        logger.info(f"[{result.job.task.name}] failed with the following error message: {result.error_message}.")

        db.log_job_error(
            job_id=result.job.job_id,
            return_code=result.return_code or -1,
            error_message=result.error_message or "No error message was provided.",
        )
    else:
        logger.info(f"[{result.job.task.name}] completed successfully in {result.execution_millis/1000:.0f} seconds.")

        db.log_job_success(
            job_id=result.job.job_id,
            execution_millis=typing.cast(int, result.execution_millis),
        )


def _run_job_with_retry(
    *,
    connection_str: str,
    tool_dir: pathlib.Path,
    job: data.Job,
    retries_so_far: int = 0,
) -> data.JobResult:
    try:
        result = _run_job_in_process(
            connection_str=connection_str,
            tool_dir=tool_dir,
            job=job,
            retries=retries_so_far,
        )
        if result.is_err:
            if job.task.retries > retries_so_far:
                logger.info(f"Retrying [{job.task.name}] ({retries_so_far + 1}/{job.task.retries})...")
                return _run_job_with_retry(
                    connection_str=connection_str,
                    tool_dir=tool_dir,
                    job=job,
                    retries_so_far=retries_so_far + 1,
                )
            return result
        return result
    except Exception as e:
        if job.task.retries > retries_so_far:
            logger.info(f"Retrying [{job.task.name}] ({retries_so_far + 1}/{job.task.retries})...")
            return _run_job_with_retry(
                connection_str=connection_str,
                tool_dir=tool_dir,
                job=job,
                retries_so_far=retries_so_far + 1,
            )
        return data.JobResult.error(job=job, code=-1, message=str(e), retries=retries_so_far)


def _run_job_in_process(
    *,
    connection_str: str,
    tool_dir: pathlib.Path,
    job: data.Job,
    retries: int,
) -> data.JobResult:
    result_queue: "mp.Queue[data.JobResult]" = mp.Queue()
    try:
        p = mp.Process(target=_run_job, args=(job, connection_str, tool_dir, result_queue, retries))
        p.start()
        result = result_queue.get(block=True, timeout=job.task.timeout_seconds)
        p.join()
        return result
    except queue.Empty:
        logger.error(f"[{job.task.name}] timed out after {job.task.timeout_seconds} seconds.")
        return data.JobResult.error(
            job=job,
            code=-1,
            message=f"[{job.task.name}] timed out after {job.task.timeout_seconds} seconds.",
            retries=retries,
        )
    except Exception as e:
        logger.exception(e)
        return data.JobResult.error(job=job, code=-1, message=str(e), retries=retries)
    finally:
        result_queue.close()


def _run_job(
    job: data.Job,
    connection_str: str,
    tool_dir: pathlib.Path,
    result_queue: "mp.Queue[data.JobResult]",
    retries: int,
    /,
) -> None:
    if isinstance(job.task, data.CmdLineUtilityTask):
        _run_cmd_line_utility_task(job=job, tool_dir=tool_dir, result_queue=result_queue, retries=retries)
    elif isinstance(job.task, data.CondaProjectTask):
        _run_conda_project_task(job=job, tool_dir=tool_dir, result_queue=result_queue, retries=retries)
    elif isinstance(job.task, data.SQLTask):
        _run_sql_task(job=job, connection_str=connection_str, result_queue=result_queue, retries=retries)
    else:
        raise Exception(f"Unrecognized job task, {job.task.__class__.__name__}.")  # todo create custom exception


def _run_cmd_line_utility_task(
    *,
    job: data.Job,
    tool_dir: pathlib.Path,
    result_queue: "mp.Queue[data.JobResult]",
    retries: int,
) -> None:
    assert isinstance(job.task, data.CmdLineUtilityTask)

    result = data.JobResult.error(
        job=job,
        code=-1,
        message=f"[{job.task.name}] never ran.",
        retries=retries,
    )
    try:
        if (fp := (tool_dir / job.task.tool)).exists():
            tool_path = fp
        elif (nested_fp := tool_dir / pathlib.Path(job.task.tool).with_suffix("").name / job.task.tool).exists():
            tool_path = nested_fp
        else:
            raise Exception(
                f"The tool specified, {job.task.tool!r}, was not found in the tools directory.  "
                f"The following paths were checked: {fp.resolve()!s}, {nested_fp.resolve()!s}"
            )

        start = datetime.datetime.now()

        executable_arg = str(tool_path.resolve())
        if job.task.tool_args:
            cmd = [executable_arg] + job.task.tool_args
        else:
            cmd = [executable_arg]

        proc_result = subprocess.run(
            cmd,
            capture_output=True,
            timeout=job.task.timeout_seconds,
            cwd=tool_path.parent,
        )

        execution_millis = int((datetime.datetime.now() - start).total_seconds() * 1000)

        if proc_result.returncode:
            result = data.JobResult.error(
                job=job,
                code=proc_result.returncode,
                message=str(proc_result.stderr),
                retries=retries,
            )
        else:
            result = data.JobResult.success(
                job=job,
                execution_millis=execution_millis,
                retries=retries,
            )
    except subprocess.TimeoutExpired:
        result = data.JobResult.error(job=job, code=-1, message="Job timed out.", retries=retries)
    except Exception as e:
        result = data.JobResult.error(job=job, code=-1, message=str(e), retries=retries)
    finally:
        result_queue.put(result)


def _run_sql_task(
    *,
    job: data.Job,
    connection_str: str,
    result_queue: "mp.Queue[data.JobResult]",
    retries: int,
) -> None:
    assert isinstance(job.task, data.SQLTask)

    result = data.JobResult.error(
        job=job,
        code=-1,
        message=f"[{job.task.name}] never ran.",
        retries=retries,
    )
    try:
        start = datetime.datetime.now()

        with psycopg2.connect(connection_str) as con:
            with con.cursor() as cur:
                cur.execute(typing.cast(str, job.task.sql))

        result = data.JobResult.success(
            job=job,
            execution_millis=int((datetime.datetime.now() - start).total_seconds() * 1000),
            retries=retries,
        )
    except Exception as e:
        result = data.JobResult.error(
            job=job,
            code=-1,
            message=str(e),
            retries=retries,
        )
    finally:
        result_queue.put(result)
