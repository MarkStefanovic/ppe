from __future__ import annotations

import functools
import json
import pathlib
import typing

import loguru

__all__ = (
    "get_conda_project_root",
    "get_connection_str",
    "get_days_logs_to_keep",
    "get_max_connections",
    "get_max_simultaneous_jobs",
    "get_seconds_between_cleanups",
    "get_seconds_between_retries",
    "get_seconds_between_updates",
)


@functools.lru_cache
def get_conda_project_root(*, config_file: pathlib.Path) -> pathlib.Path:
    folder = pathlib.Path(str(_load(config_file=config_file)["conda-project-root"]))
    assert folder.exists(), f"The conda-project-root config setting, {folder.resolve()!s}, does not exist."
    return folder


@functools.lru_cache
def get_connection_str(*, config_file: pathlib.Path) -> str:
    return str(_load(config_file=config_file)["connection-string"])


@functools.lru_cache
def get_days_logs_to_keep(*, config_file: pathlib.Path) -> int:
    return typing.cast(int, _load(config_file=config_file)["days-logs-to-keep"])


@functools.lru_cache
def get_max_connections(*, config_file: pathlib.Path) -> int:
    return typing.cast(int, _load(config_file=config_file)["max-connections"])


@functools.lru_cache
def get_max_simultaneous_jobs(*, config_file: pathlib.Path) -> int:
    return typing.cast(int, _load(config_file=config_file)["max-simultaneous-jobs"])


@functools.lru_cache
def get_seconds_between_cleanups(*, config_file: pathlib.Path) -> int:
    return typing.cast(int, _load(config_file=config_file)["seconds-between-cleanups"])


@functools.lru_cache
def get_seconds_between_retries(*, config_file: pathlib.Path) -> int:
    return typing.cast(int, _load(config_file=config_file)["seconds-between-retries"])


@functools.lru_cache
def get_seconds_between_updates(*, config_file: pathlib.Path) -> int:
    return typing.cast(int, _load(config_file=config_file)["seconds-between-updates"])


@functools.lru_cache
def get_seconds_between_task_issue_updates(*, config_file: pathlib.Path) -> int:
    return typing.cast(int, _load(config_file=config_file)["seconds-between-task-issue-updates"])


@functools.lru_cache
def _load(*, config_file: pathlib.Path) -> dict[str, typing.Hashable]:
    loguru.logger.info(f"Loading config file at {config_file.resolve()!s}...")

    assert config_file.exists(), f"The config file specified, {config_file.resolve()!s}, does not exist."

    with config_file.open("r") as fh:
        return json.load(fh)
