import queue
import threading
import time

import loguru

from src import adapter, data

__all__ = ("Scheduler", "start")


def start(
    *,
    job_queue: queue.Queue[data.Task],
    db: adapter.db.Db,
) -> threading.Thread:
    loguru.logger.info("Starting scheduler...")

    assert job_queue.maxsize > 0, f"job_queue maxsize must be > 0, but the job_queue provided to scheduler.start was maxsize {job_queue.maxsize}."

    scheduler = Scheduler(db=db, job_queue=job_queue)
    th = threading.Thread(name="scheduler", target=scheduler.run, daemon=True)
    th.start()

    return th


class Scheduler:
    def __init__(
        self,
        *,
        job_queue: queue.Queue[data.Task],
        db: adapter.db.Db,
    ):
        self._job_queue = job_queue
        self._db = db

    def run(self) -> None:
        while True:
            if self._job_queue.full():
                loguru.logger.debug("The job_queue is full.")
            else:
                jobs_to_fetch = self._job_queue.maxsize - self._job_queue.qsize()
                loguru.logger.debug(f"Fetching {jobs_to_fetch} jobs...")
                jobs_to_add: list[data.Task] = self._db.get_ready_tasks(n=jobs_to_fetch)
                for job in jobs_to_add:
                    try:
                        self._job_queue.put_nowait(job)
                        loguru.logger.debug(f"Added [{job.name}] to the queue.")
                    except queue.Full:
                        loguru.logger.exception(f"The queue was full, so [{job.name}] was not added to the queue.")
            time.sleep(1)
