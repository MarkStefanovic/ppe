import logging
import queue
import sys

from src import adapter, data, service

logger = logging.getLogger()


def main(*, max_jobs: int = adapter.config.max_simultaneous_jobs()) -> None:
    try:
        job_queue: queue.Queue[data.Task] = queue.Queue(max_jobs)

        scheduler_thread = service.scheduler.start(job_queue=job_queue)

        db = adapter.db.open_db()

        job_runners = [
            service.runner.start(db=db, job_queue=job_queue, max_simultaneous_jobs=max_jobs)
            for _ in range(max_jobs)
        ]

        scheduler_thread.join()
        for job_runner in job_runners:
            job_runner.join()
    except Exception as e:
        logger.exception(e)
        raise


if __name__ == '__main__':
    adapter.fs.log_folder().mkdir(exist_ok=True)
    logging.basicConfig(level=logging.INFO)

    logger.info("Starting ppe...")
    main(max_jobs=adapter.config.max_simultaneous_jobs())
    logger.error("ppe stopped somehow.")

    sys.exit(-1)
