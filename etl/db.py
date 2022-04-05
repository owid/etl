import MySQLdb
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from urllib.parse import quote

from etl import config


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
