from __future__ import annotations

import functools
import json
import pathlib
import typing

import loguru

from src.adapter import fs

__all__ = ("get_connection_string", "get_max_connections", "get_max_simultaneous_jobs")


@functools.lru_cache(maxsize=1)
def get_connection_string(*, config_file: pathlib.Path = fs.config_path()) -> str:
    return str(_load(config_file=config_file)["connection-string"])


@functools.lru_cache(maxsize=1)
def get_max_connections(*, config_file: pathlib.Path = fs.config_path()) -> int:
    return typing.cast(int, _load(config_file=config_file)["max-connections"])


@functools.lru_cache(maxsize=1)
def get_max_simultaneous_jobs(*, config_file: pathlib.Path = fs.config_path()) -> int:
    return typing.cast(int, _load(config_file=config_file)["max-simultaneous-jobs"])


@functools.lru_cache(maxsize=1)
def _load(*, config_file: pathlib.Path = fs.config_path()) -> dict[str, typing.Hashable]:
    loguru.logger.info(f"Loading config file at {config_file.resolve()!s}...")

    assert config_file.exists(), f"The config file specified, {config_file.resolve()!s}, does not exist."

    with config_file.open("r") as fh:
        return json.load(fh)
