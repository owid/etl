import os
import MySQLdb
from dotenv import load_dotenv

load_dotenv()


def get_connection() -> MySQLdb.Connection:
    "Connect to the Grapher database."
    port_s = os.getenv("DB_PORT")
    port = int(port_s) if port_s else 3306
    return MySQLdb.connect(
        db=os.getenv("DB_NAME"),
        host=os.getenv("DB_HOST"),
        port=port,
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        charset="utf8mb4",
        autocommit=True,
    )
