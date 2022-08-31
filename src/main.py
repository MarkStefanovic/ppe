import logging
import os
import sys
import threading
import time

import loguru

from src import adapter, service


def main(
    *,
    connection_str: str,
    max_connections: int,
    max_jobs: int,
    seconds_between_updates: int,
    seconds_between_cleanups: int,
    days_logs_to_keep: int,
) -> None:
    cancel = threading.Event()

    pool = adapter.db.create_pool(connection_str=connection_str, max_size=max_connections)

    batch_id = adapter.db.create_batch(pool=pool)

    db = adapter.db.open_db(batch_id=batch_id, pool=pool, days_logs_to_keep=days_logs_to_keep)

    try:
        db.log_batch_info(message="batch started")

        db.cancel_running_jobs(reason="A new batch was started.")

        scheduler = service.scheduler.Scheduler(
            db=db,
            seconds_between_updates=seconds_between_updates,
            seconds_between_cleanups=seconds_between_cleanups,
            cancel=cancel,
        )

        job_runners = [
            service.runner.Runner(db=db, connection_str=connection_str, tool_dir=adapter.fs.get_tool_dir(), cancel=cancel)
            for _ in range(max_jobs)
        ]

        scheduler.start()
        for job_runner in job_runners:
            job_runner.start()

        while not cancel.is_set():
            time.sleep(1)

        scheduler.join()
        for job_runner in job_runners:
            job_runner.join()
    except (KeyboardInterrupt, SystemExit):
        loguru.logger.info(f"Service shutdown triggered.")
        db.log_batch_info(message=f"ppe exited at the request of the user, {os.environ.get('USERNAME', 'Unknown')}.")
        sys.exit()
    except Exception as e:
        loguru.logger.exception(e)
        db.log_batch_error(error_message=f"ppe closed as a result of the following error: {e}")
        sys.exit(-1)
    finally:
        cancel.set()


if __name__ == '__main__':
    adapter.fs.get_log_folder().mkdir(exist_ok=True)

    loguru.logger.add(adapter.fs.get_log_folder() / "error.log", rotation="5 MB", retention="7 days", level="ERROR")

    if getattr(sys, "frozen", False):
        loguru.logger.add(sys.stderr, format="{time} {level} {message}", level=logging.DEBUG)

    loguru.logger.info("Starting ppe...")

    config_file = adapter.fs.get_config_path()

    main(
        connection_str=adapter.config.get_connection_str(config_file=config_file),
        max_connections=adapter.config.get_max_connections(config_file=config_file),
        max_jobs=adapter.config.get_max_simultaneous_jobs(config_file=config_file),
        seconds_between_updates=adapter.config.get_seconds_between_updates(config_file=config_file),
        seconds_between_cleanups=adapter.config.get_seconds_between_cleanups(config_file=config_file),
        days_logs_to_keep=adapter.config.get_days_logs_to_keep(config_file=config_file),
    )
