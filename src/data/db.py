from __future__ import annotations

import abc

from src.data.job import Job

__all__ = ("Db",)


class Db(abc.ABC):
    @abc.abstractmethod
    def cancel_running_jobs(self, *, reason: str) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def delete_old_logs(self) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def get_ready_job(self) -> Job | None:
        raise NotImplementedError

    @abc.abstractmethod
    def log_batch_info(self, *, message: str) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def log_batch_error(self, *, error_message: str) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def log_job_error(self, *, job_id: int, return_code: int, error_message: str) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def log_job_success(self, *, job_id: int, execution_millis: int) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def update_queue(self) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def update_task_issues(self) -> None:
        raise NotImplementedError
