import json
import pathlib
import typing

import pytest
from psycopg2.pool import ThreadedConnectionPool


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
def pool_fixture(_root_dir_fixture: pathlib.Path, connection_str_fixture: str) -> ThreadedConnectionPool:
    pool = ThreadedConnectionPool(1, 5, dsn=connection_str_fixture)
    with pool.getconn() as con:
        with con.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS ppe CASCADE;")
            sql_path = _root_dir_fixture.parent / "setup.sql"
            with sql_path.open("r") as fh:
                sql = "\n".join(fh.readlines())
            cur.execute(sql)
        con.commit()
        pool.putconn(con)
    return pool
