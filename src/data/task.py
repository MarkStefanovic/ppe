from __future__ import annotations

import dataclasses
import textwrap
import typing

__all__ = ("CmdLineUtilityTask", "CondaProjectTask", "SQLTask", "Task")


@typing.runtime_checkable
class Task(typing.Protocol):
    task_id: int
    name: str
    timeout_seconds: int | None
    retries: int


@dataclasses.dataclass(frozen=True, eq=True, kw_only=True)
class CmdLineUtilityTask:
    task_id: int
    name: str
    timeout_seconds: int | None
    retries: int
    tool: str
    tool_args: list[str] | None

    def __post_init__(self) -> None:
        assert self.task_id > 0, "task_id must be > 0."
        assert len(self.name) > 0, "task_name cannot be blank."
        assert len(self.tool) > 0, "tool cannot be blank."
        assert self.tool_args is None or len(self.tool_args) > 0, \
            "If tool_args is provided, then it must have at least 1 item."
        assert self.timeout_seconds is None or self.timeout_seconds >= 0, \
            "If timeout_seconds is provided, then it must be positive."
        assert self.retries >= 0, "retries must be positive."

    def __repr__(self) -> str:
        return textwrap.dedent(
            f"""
            {self.__class__.__name__} [
                task_id:         {self.task_id}
                name:            {self.name}
                tool:            {self.tool!r}
                tool_args:       [{', '.join(repr(arg) for arg in self.tool_args or [])}]
                timeout_seconds: {self.timeout_seconds}
                retries:         {self.retries}
            ]
            """
        ).strip()


@dataclasses.dataclass(frozen=True, eq=True, kw_only=True)
class CondaProjectTask:
    task_id: int
    name: str
    timeout_seconds: int | None
    retries: int
    env: str
    project_name: str
    fn: str = "src.main"
    fn_args: frozenset[tuple[str, typing.Hashable]] = frozenset()

    def __post_init__(self) -> None:
        assert self.task_id > 0, "task_id must be > 0."
        assert len(self.name) > 0, "task_name cannot be blank."
        assert self.timeout_seconds is None or self.timeout_seconds >= 0, \
            "If timeout_seconds is provided, then it must be positive."
        assert self.retries >= 0, "retries must be positive."
        assert len(self.env) > 0, "env cannot be blank."
        assert len(self.project_name) > 0, "project_name cannot be blank."
        assert len(self.fn) > 0, "fn cannot be blank."

    def __repr__(self) -> str:
        return textwrap.dedent(
            f"""
            {self.__class__.__name__} [
                task_id:         {self.task_id}
                name:            {self.name}
                timeout_seconds: {self.timeout_seconds}
                retries:         {self.retries}
                env:             {self.env!r}
                project:         {self.project_name!r}
                fn:              {self.fn!r}
                fn_args:         {self.fn_args!r}
            ]
            """
        ).strip()


@dataclasses.dataclass(frozen=True, eq=True, kw_only=True)
class SQLTask:
    task_id: int
    name: str
    timeout_seconds: int | None
    retries: int
    sql: str

    def __post_init__(self) -> None:
        assert self.task_id > 0, "task_id must be > 0."
        assert len(self.name) > 0, "task_name cannot be blank."
        assert self.timeout_seconds is None or self.timeout_seconds >= 0, \
            "If timeout_seconds is provided, then it must be positive."
        assert self.retries >= 0, "retries must be positive."
        assert len(self.sql) > 0, "sql cannot be blank."

    def __repr__(self) -> str:
        return textwrap.dedent(
            f"""
            {self.__class__.__name__} [
                task_id:         {self.task_id}
                name:            {self.name}
                timeout_seconds: {self.timeout_seconds}
                retries:         {self.retries}
                sql:             {self.sql!r}
            ]
            """
        ).strip()
