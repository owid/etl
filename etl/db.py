import MySQLdb

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
