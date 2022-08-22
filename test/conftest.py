import json
import pathlib
import typing

import psycopg_pool
import pytest

from src.db import Db, open_db


@pytest.fixture(scope="function")
def _root_dir_fixture(request) -> pathlib.Path:
    return pathlib.Path(request.fspath).parent


@pytest.fixture(scope="function")
def _config_fixture(_root_dir_fixture: pathlib.Path) -> dict[str, typing.Hashable]:
    with (_root_dir_fixture / "test-config.json").open("r") as fh:
        return json.load(fh)


@pytest.fixture(scope="function")
def connection_str_fixture(_config_fixture: dict[str, typing.Hashable]) -> str:
    return typing.cast(str, _config_fixture["test-db-connection-str"])


@pytest.fixture(scope="function")
def pool_fixture(_root_dir_fixture: pathlib.Path, connection_str_fixture: str) -> psycopg_pool.ConnectionPool:
    pool = psycopg_pool.ConnectionPool(connection_str_fixture)
    with pool.connection(5) as con:
        con.execute("DROP SCHEMA IF EXISTS ppe CASCADE;")
        sql_path = _root_dir_fixture.parent / "setup.sql"
        with sql_path.open("r") as fh:
            sql = "\n".join(fh.readlines())
        con.execute(sql)
    return pool


@pytest.fixture(scope="function")
def db_fixture(pool_fixture: psycopg_pool.ConnectionPool) -> Db:
    return open_db(pool=pool_fixture)

