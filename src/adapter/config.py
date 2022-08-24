from __future__ import annotations

import functools
import json
import pathlib
import typing

import loguru

__all__ = (
    "get_connection_str",
    "get_days_logs_to_keep",
    "get_max_connections",
    "get_max_simultaneous_jobs",
    "get_seconds_between_cleanups",
    "get_seconds_between_updates",
)


@functools.lru_cache(maxsize=1)
def get_connection_str(*, config_file: pathlib.Path) -> str:
    return str(_load(config_file=config_file)["connection-string"])


def get_days_logs_to_keep(*, config_file: pathlib.Path) -> int:
    return typing.cast(int, _load(config_file=config_file)["days-logs-to-keep"])


@functools.lru_cache(maxsize=1)
def get_max_connections(*, config_file: pathlib.Path) -> int:
    return typing.cast(int, _load(config_file=config_file)["max-connections"])


@functools.lru_cache(maxsize=1)
def get_max_simultaneous_jobs(*, config_file: pathlib.Path) -> int:
    return typing.cast(int, _load(config_file=config_file)["max-simultaneous-jobs"])


@functools.lru_cache(maxsize=1)
def get_seconds_between_cleanups(*, config_file: pathlib.Path) -> int:
    return typing.cast(int, _load(config_file=config_file)["get-seconds-between-cleanups"])


@functools.lru_cache(maxsize=1)
def get_seconds_between_updates(*, config_file: pathlib.Path) -> int:
    return typing.cast(int, _load(config_file=config_file)["get-seconds-between-updates"])


@functools.lru_cache(maxsize=1)
def _load(*, config_file: pathlib.Path) -> dict[str, typing.Hashable]:
    loguru.logger.info(f"Loading config file at {config_file.resolve()!s}...")

    assert config_file.exists(), f"The config file specified, {config_file.resolve()!s}, does not exist."

    with config_file.open("r") as fh:
        return json.load(fh)
