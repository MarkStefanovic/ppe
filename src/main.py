import logging
import queue
import sys

import loguru

from src import adapter, data, service


def main(
    *,
    connection_string = adapter.config.get_connection_string(),
    max_connections: int = adapter.config.get_max_connections(),
    max_jobs: int = adapter.config.get_max_simultaneous_jobs(),
) -> None:
    try:
        job_queue: queue.Queue[data.Task] = queue.Queue(max_jobs)

        pool = adapter.db.create_pool(connection_string=connection_string, max_size=max_connections)

        db = adapter.db.open_db(pool=pool)

        scheduler_thread = service.scheduler.start(job_queue=job_queue, db=db)

        job_runners = [
            service.runner.start(db=db, job_queue=job_queue, max_simultaneous_jobs=max_jobs)
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
    logging.basicConfig(level=logging.INFO)

    loguru.logger.add(adapter.fs.log_folder() / "error.log", rotation="5 MB", retention="7 days", level="ERROR")

    if getattr(sys, "frozen", False):
        loguru.logger.add(sys.stderr, format="{time} {level} {message}", level=logging.DEBUG)

    loguru.logger.info("Starting ppe...")
    main(max_jobs=adapter.config.get_max_simultaneous_jobs())
    loguru.logger.error("ppe stopped somehow.")

    sys.exit(-1)
