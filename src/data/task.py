from __future__ import annotations

import dataclasses
import textwrap

__all__ = ("Task",)


@dataclasses.dataclass(frozen=True, eq=True)
class Task:
    task_id: int
    name: str
    tool: str | None
    tool_args: list[str] | None
    sql: str | None
    timeout_seconds: int | None
    retries: int

    def __post_init__(self) -> None:
        assert self.task_id > 0, "task_id must be > 0."
        assert len(self.name) > 0, "task_name cannot be blank."
        assert self.tool is not None or self.sql is not None, "Either cmd or sql must be provided."
        assert self.tool is None or self.sql is None, "Either sql or tool must be provided, but not both."
        assert self.tool is None or len(self.tool) > 0, "If tool is provided, then it cannot be blank."
        assert self.tool_args is None or len(self.tool_args) > 0, "If tool_args is provided, then it must have at least 1 item."
        assert self.sql is None or len(self.sql) > 0, "If task_sql is provided, then it cannot be blank."
        assert self.timeout_seconds is None or self.timeout_seconds >= 0, "If timeout_seconds is provided, then it must be positive."
        assert self.retries >= 0, "retries must be positive."

    def __repr__(self) -> str:
        return textwrap.dedent(
            f"""
            Task [
                task_id:         {self.task_id}
                name:            {self.name}
                tool:            {self.tool!r}
                tool_args:       [{', '.join(repr(arg) for arg in self.tool_args or [])}]
                task_sql:        {self.sql!r}
                timeout_seconds: {self.timeout_seconds}
                retries:         {self.retries}
            ]
            """
        ).strip()
