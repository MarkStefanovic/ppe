from __future__ import annotations

import datetime
import threading
import time

import loguru

from src import adapter

__all__ = ("Scheduler",)


class Scheduler(threading.Thread):
    def __init__(
        self,
        *,
        db: adapter.db.Db,
        seconds_between_updates: int,
        seconds_between_cleanups: int,
        cancel: threading.Event,
    ):
        super().__init__()

        self._db = db
        self._seconds_between_updates = seconds_between_updates
        self._seconds_between_cleanups = seconds_between_cleanups
        self._cancel = cancel

        self._e: Exception | None = None

    def error(self) -> Exception | None:
        return self._e

    def join(self, timeout: float | None = None) -> None:
        super().join()

        loguru.logger.info("Scheduler stopped.")

        # reraise exception in main thread
        if self._e is not None:
            raise self._e

    def run(self) -> None:
        try:
            self._db.delete_old_logs()
            last_cleanup = datetime.datetime.now()

            self._db.update_queue()
            last_queue_update = datetime.datetime.now()

            while not self._cancel.is_set():
                if (datetime.datetime.now() - last_cleanup).total_seconds() > self._seconds_between_cleanups:
                    loguru.logger.debug("Cleaning up old logs...")
                    self._db.delete_old_logs()
                    last_cleanup = datetime.datetime.now()
                    loguru.logger.debug("Finished cleaning up logs.")

                if (datetime.datetime.now() - last_queue_update).total_seconds() > self._seconds_between_updates:
                    loguru.logger.debug("Updating queue...")
                    self._db.update_queue()
                    last_queue_update = datetime.datetime.now()
                    loguru.logger.debug("Finished updating queue.")

                time.sleep(1)
        except Exception as e:
            self._e = e
            loguru.logger.exception(e)
            self._db.log_batch_error(error_message=str(e))
            self._cancel.set()
