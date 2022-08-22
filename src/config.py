from __future__ import annotations

import functools
import json
import pathlib
import typing

from src import fs


@functools.lru_cache(maxsize=1)
def connection_string(*, config_file: pathlib.Path = fs.config_path()) -> str:
    return str(_load(config_file=config_file)["connection_string"])


@functools.lru_cache(maxsize=1)
def max_simultaneous_jobs(*, config_file: pathlib.Path = fs.config_path()) -> int:
    return typing.cast(int, _load(config_file=config_file)["max_simultaneous_jobs"])


@functools.lru_cache(maxsize=1)
def _load(*, config_file: pathlib.Path = fs.config_path()) -> dict[str, typing.Hashable]:
    assert config_file.exists(), f"The config file specified, {config_file.resolve()!s}, does not exist."

    with config_file.open("r") as fh:
        return json.load(fh)


if __name__ == '__main__':
    print(f"{connection_string()=}")
    print(f"{max_simultaneous_jobs()=}")
