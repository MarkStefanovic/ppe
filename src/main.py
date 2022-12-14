import multiprocessing
import os
import sys
import threading
import time
import traceback

import loguru

from src import adapter, service


def run() -> None:
    loguru.logger.info("Starting ppe...")

    config_file = adapter.fs.get_config_path()

    seconds_between_retries = adapter.config.get_seconds_between_retries(config_file=config_file)

    while True:
        try:
            _run(
                connection_str=adapter.config.get_connection_str(config_file=config_file),
                max_connections=adapter.config.get_max_connections(config_file=config_file),
                max_jobs=adapter.config.get_max_simultaneous_jobs(config_file=config_file),
                seconds_between_updates=adapter.config.get_seconds_between_updates(config_file=config_file),
                seconds_between_cleanups=adapter.config.get_seconds_between_cleanups(config_file=config_file),
                seconds_between_task_issue_updates=adapter.config.get_seconds_between_task_issue_updates(config_file=config_file),
                days_logs_to_keep=adapter.config.get_days_logs_to_keep(config_file=config_file),
            )
        except Exception:  # noqa
            loguru.logger.error(f"ppe exited abnormally, restarting in {seconds_between_retries} seconds...")
            time.sleep(seconds_between_retries)


def _run(
    *,
    connection_str: str,
    max_connections: int,
    max_jobs: int,
    seconds_between_updates: int,
    seconds_between_cleanups: int,
    seconds_between_task_issue_updates: int,
    days_logs_to_keep: int,
) -> None:

    with adapter.db.create_pool(connection_str=connection_str, max_size=max_connections) as pool:
        batch_id = adapter.db.create_batch(pool=pool)

        loguru.logger.info(f"Starting batch {batch_id}...")

        db = adapter.db.open_db(batch_id=batch_id, pool=pool, days_logs_to_keep=days_logs_to_keep)

        loguru.logger.info("Database connection open.")

        cancel = threading.Event()
        try:
            db.log_batch_info(message="batch started")

            db.cancel_running_jobs(reason="A new batch was started.")

            scheduler = service.scheduler.Scheduler(
                db=db,
                seconds_between_updates=seconds_between_updates,
                seconds_between_cleanups=seconds_between_cleanups,
                seconds_between_task_issue_updates=seconds_between_task_issue_updates,
                cancel=cancel,
            )

            job_runners = [
                service.runner.Runner(db=db, connection_str=connection_str, tool_dir=adapter.fs.get_tool_dir(), cancel=cancel)
                for _ in range(max_jobs)
            ]

            scheduler.start()

            for job_runner in job_runners:
                job_runner.start()

            loguru.logger.info(f"{max_jobs} job runners started.")

            while not cancel.is_set():
                time.sleep(1)

            scheduler.join()
            for job_runner in job_runners:
                job_runner.join()
        except (KeyboardInterrupt, SystemExit):
            loguru.logger.info(f"Service shutdown triggered.")
            db.log_batch_info(message=f"ppe exited at the request of the user, {os.environ.get('USERNAME', 'Unknown')}.")
            sys.exit()
        except Exception as e2:
            loguru.logger.exception(e2)
            db.log_batch_error(error_message=f"ppe ran into an error: {e2!s}\n{traceback.format_exc()}")
            raise
        finally:
            cancel.set()


if __name__ == '__main__':
    multiprocessing.freeze_support()

    adapter.fs.get_log_folder().mkdir(exist_ok=True)

    loguru.logger.remove()
    loguru.logger.add(adapter.fs.get_log_folder() / "error.log", rotation="5 MB", retention="7 days", level="ERROR")
    loguru.logger.add(sys.stderr, level="INFO")

    try:
        run()
        sys.exit(0)
    except Exception as e:
        loguru.logger.exception(e)
        sys.exit(1)
