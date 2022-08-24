import logging
import sys

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
    try:
        pool = adapter.db.create_pool(connection_str=connection_str, max_size=max_connections)

        batch_id = adapter.db.create_batch(pool=pool)

        db = adapter.db.open_db(batch_id=batch_id, pool=pool, days_logs_to_keep=days_logs_to_keep)

        db.cancel_running_jobs(reason="A new batch was started.")

        scheduler_thread = service.scheduler.start(
            db=db,
            seconds_between_updates=seconds_between_updates,
            seconds_between_cleanups=seconds_between_cleanups,
        )

        job_runners = [
            service.runner.start(db=db, connection_str=connection_str)
            for _ in range(max_jobs)
        ]

        scheduler_thread.join()
        for job_runner in job_runners:
            job_runner.join()
    except Exception as e:
        loguru.logger.exception(e)
        raise


if __name__ == '__main__':
    adapter.fs.log_folder().mkdir(exist_ok=True)

    loguru.logger.add(adapter.fs.log_folder() / "error.log", rotation="5 MB", retention="7 days", level="ERROR")

    if getattr(sys, "frozen", False):
        loguru.logger.add(sys.stderr, format="{time} {level} {message}", level=logging.DEBUG)

    loguru.logger.info("Starting ppe...")

    config_file = adapter.fs.config_path()
    main(
        connection_str=adapter.config.get_connection_str(config_file=config_file),
        max_connections=adapter.config.get_max_connections(config_file=config_file),
        max_jobs=adapter.config.get_max_simultaneous_jobs(config_file=config_file),
        seconds_between_updates=adapter.config.get_seconds_between_updates(config_file=config_file),
        seconds_between_cleanups=adapter.config.get_seconds_between_cleanups(config_file=config_file),
        days_logs_to_keep=adapter.config.get_days_logs_to_keep(config_file=config_file),
    )

    loguru.logger.error("ppe stopped somehow.")

    sys.exit(-1)
