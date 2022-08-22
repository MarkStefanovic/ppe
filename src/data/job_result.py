from __future__ import annotations

import dataclasses
import typing

from src.data import Job

__all__ = ("JobResult",)


@dataclasses.dataclass(frozen=True)
class JobResult:
    job: Job
    status: typing.Literal["success"] | typing.Literal["error"]
    return_code: int | None
    error_message: str | None
    execution_millis: int | None
    retries: int | None

    @property
    def is_err(self) -> bool:
        return self.status == "error"

    @staticmethod
    def error(*, job: Job, code: int, message: str, retries: int) -> JobResult:
        return JobResult(job=job, status="error", return_code=code, error_message=message, execution_millis=None, retries=retries)

    @staticmethod
    def success(*, job: Job, execution_millis: int, retries: int) -> JobResult:
        return JobResult(job=job, status="success", return_code=0, error_message=None, execution_millis=execution_millis, retries=retries)

    @staticmethod
    def timeout(*, job: Job, retries: int) -> JobResult:
        return JobResult(job=job, status="error", return_code=-1, error_message="Job timed out.", execution_millis=None, retries=retries)
