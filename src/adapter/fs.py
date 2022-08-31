import functools
import os
import pathlib
import sys

__all__ = ("get_config_path", "get_log_folder", "get_tool_dir")


@functools.lru_cache(maxsize=1)
def _root_dir() -> pathlib.Path:
    if getattr(sys, "frozen", False):
        path = pathlib.Path(os.path.dirname(sys.executable))
        assert path is not None
        return path
    else:
        try:
            return next(p for p in pathlib.Path(__file__).parents if p.name == "ppe")
        except StopIteration:
            raise Exception(f"ppe not found in path, {__file__}.")


@functools.lru_cache(maxsize=1)
def get_config_path() -> pathlib.Path:
    return _root_dir() / "assets" / "config.json"


@functools.lru_cache(maxsize=1)
def get_log_folder() -> pathlib.Path:
    d = _root_dir() / "logs"
    d.mkdir(exist_ok=True)
    return d


@functools.lru_cache(maxsize=1)
def get_tool_dir() -> pathlib.Path:
    d = _root_dir() / "tools"
    d.mkdir(exist_ok=True)
    return d
