import logging
import queue
import sys

from src import data, fs, runner, scheduler
from src.db import open_db

logger = logging.getLogger()


def main(*, max_jobs: int) -> None:
    try:
        job_queue: queue.Queue[data.Task] = queue.Queue(max_jobs)

        scheduler_thread = scheduler.start(job_queue=job_queue)

        db = open_db()

        job_runners = [
            runner.start(db=db, job_queue=job_queue, max_simultaneous_jobs=max_jobs)
            for _ in range(max_jobs)
        ]

        scheduler_thread.join()
        for job_runner in job_runners:
            job_runner.join()
    except Exception as e:
        logger.exception(e)
        raise


if __name__ == '__main__':
    fs.log_folder().mkdir(exist_ok=True)
    logging.basicConfig(level=logging.INFO)

    logger.info("Starting ppe...")
    main(max_jobs=5)
    logger.error("ppe stopped somehow.")

    sys.exit(-1)
