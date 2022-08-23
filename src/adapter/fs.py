import functools
import os
import pathlib
import sys

__all__ = ("config_path", "log_folder")


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
def config_path() -> pathlib.Path:
    return _root_dir() / "assets" / "config.json"


@functools.lru_cache(maxsize=1)
def log_folder() -> pathlib.Path:
    return _root_dir() / "logs"
