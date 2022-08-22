from __future__ import annotations

import dataclasses
import textwrap

__all__ = ("Task",)


@dataclasses.dataclass(frozen=True, eq=True)
class Task:
    task_id: int
    name: str
    cmd: list[str] | None
    sql: str | None
    timeout_seconds: int | None
    retries: int

    def __post_init__(self) -> None:
        assert self.task_id > 0, "task_id must be > 0."
        assert len(self.name) > 0, "task_name cannot be blank."
        assert self.cmd is not None or self.sql is not None, "Either cmd or sql must be provided."
        assert self.cmd is None or self.sql is None, "cmd or task_sql must be provided, but not both."
        assert self.cmd is None or len(self.cmd) > 0, "If cmd is provided, then it cannot be blank."
        assert self.sql is None or len(self.sql) > 0, "If task_sql is provided, then it cannot be blank."
        assert self.timeout_seconds is None or self.timeout_seconds >= 0, "If timeout_seconds is provided, then it must be positive."
        assert self.retries >= 0, "retries must be positive."

    def __repr__(self) -> str:
        return textwrap.dedent(
            f"""
            Task [
                task_id:         {self.task_id}
                name:            {self.name}
                cmd:             {self.cmd!r}
                task_sql:        {self.sql!r}
                timeout_seconds: {self.timeout_seconds}
                retries:         {self.retries}
            ]
            """
        )
