import traceback
from collections.abc import Generator
from contextlib import contextmanager
from urllib.parse import quote

import MySQLdb
import structlog
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from etl import config
from etl.db_utils import DBUtils

log = structlog.get_logger()


def get_connection() -> MySQLdb.Connection:
    "Connect to the Grapher database."
    return MySQLdb.connect(
        db=config.DB_NAME,
        host=config.DB_HOST,
        port=config.DB_PORT,
        user=config.DB_USER,
        password=config.DB_PASS,
        charset="utf8mb4",
        autocommit=True,
    )


def get_engine() -> Engine:
    return create_engine(
        f"mysql://{config.DB_USER}:{quote(config.DB_PASS)}@{config.DB_HOST}:{config.DB_PORT}/{config.DB_NAME}"
    )


@contextmanager
def open_db() -> Generator[DBUtils, None, None]:
    connection = None
    cursor = None
    try:
        connection = get_connection()
        connection.autocommit(False)
        cursor = connection.cursor()
        yield DBUtils(cursor)
        connection.commit()
    except Exception as e:
        log.error(f"Error encountered during import: {e}")
        log.error("Rolling back changes...")
        if connection:
            connection.rollback()
        if config.DEBUG:
            traceback.print_exc()
        raise e
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
