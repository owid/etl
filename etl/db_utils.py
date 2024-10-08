import functools
import os
import warnings
from typing import Any, Dict, Optional
from urllib.parse import quote

import pandas as pd
import pymysql
import structlog
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from etl import config

log = structlog.get_logger()


def can_connect(conf: Optional[Dict[str, Any]] = None) -> bool:
    try:
        get_connection(conf=conf)
        return True
    except pymysql.OperationalError:
        return False


def get_connection(conf: Optional[Dict[str, Any]] = None) -> pymysql.Connection:
    "Connect to the Grapher database."
    cf: Any = dict_to_object(conf) if conf else config
    return pymysql.connect(
        db=cf.DB_NAME,
        host=cf.DB_HOST,
        port=cf.DB_PORT,
        user=cf.DB_USER,
        password=cf.DB_PASS,
        charset="utf8mb4",
        autocommit=True,
    )


def get_session(**kwargs) -> Session:
    """Get session with defaults."""
    return Session(get_engine(**kwargs))


@functools.cache
def _get_engine_cached(cf: Any, pid: int) -> Engine:
    return create_engine(
        f"mysql+pymysql://{cf.DB_USER}:{quote(cf.DB_PASS)}@{cf.DB_HOST}:{cf.DB_PORT}/{cf.DB_NAME}",
        pool_size=30,  # Increase the pool size to allow higher GRAPHER_WORKERS
        max_overflow=30,  # Increase the max overflow limit to allow higher GRAPHER_WORKERS
    )


def get_engine(conf: Optional[Dict[str, Any]] = None) -> Engine:
    cf: Any = dict_to_object(conf) if conf else config
    # pid in memoization makes sure every process gets its own Engine
    pid = os.getpid()
    return _get_engine_cached(cf, pid)


def dict_to_object(d):
    return type("DynamicObject", (object,), d)()


def read_sql(sql: str, engine: Optional[Engine | Session] = None, *args, **kwargs) -> pd.DataFrame:
    """Wrapper around pd.read_sql that creates a connection and closes it after reading the data.
    This adds overhead, so if you need performance, reuse the same connection and cursor.
    """
    engine = engine or get_engine()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        if isinstance(engine, Engine):
            with engine.connect() as con:
                return pd.read_sql(sql, con, *args, **kwargs)
        elif isinstance(engine, Session):
            return pd.read_sql(sql, engine.bind, *args, **kwargs)
        else:
            raise ValueError(f"Unsupported engine type {type(engine)}")
