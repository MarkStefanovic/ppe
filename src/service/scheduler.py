import datetime
import threading
import time

import loguru

from src import adapter

__all__ = ("start",)


def start(
    *,
    db: adapter.db.Db,
    seconds_between_updates: int,
    seconds_between_cleanups: int,
) -> threading.Thread:
    loguru.logger.info("Starting scheduler...")

    th = threading.Thread(
        name="scheduler",
        target=run,
        kwargs={
            "db": db,
            "seconds_between_updates": seconds_between_updates,
            "seconds_between_cleanups": seconds_between_cleanups,
        },
        daemon=True,
    )
    th.start()

    return th


def run(*, db: adapter.db.Db, seconds_between_updates: int, seconds_between_cleanups: int) -> None:
    start = datetime.datetime.now()
    last_cleanup = start
    db.delete_old_logs()

    while True:
        if (datetime.datetime.now() - last_cleanup).total_seconds() > seconds_between_cleanups:
            loguru.logger.debug("Cleaning up old logs...")
            last_cleanup = datetime.datetime.now()
            db.delete_old_logs()

        loguru.logger.debug("Updating queue...")
        try:
            db.update_queue()
        except Exception as e:
            loguru.logger.exception(e)
            raise

        time.sleep(seconds_between_updates)
