import duckdb
import time
import functools
from fastapi import FastAPI
from pathlib import Path
import structlog
import threading
from fastapi.middleware.gzip import GZipMiddleware

from fastapi import FastAPI, Response
import io

import pyarrow as pa

from owid.catalog import utils

log = structlog.get_logger()

DIR_PATH = Path(__file__).parent
DUCKDB_PATH = DIR_PATH / "duck.db"

app = FastAPI()

# TODO: compression should be handled by nginx, this is only for testing purposes
# NOTE: compression ratio of gzip is around 90%, but gzipping itself takes about 10x
#  longer than querying the database
app.add_middleware(GZipMiddleware, minimum_size=1000)


@functools.cache
def duckdb_connection(thread_id):
    # duckdb connection is not threadsafe, we have to create one connection per thread
    log.info("duckdb.new_connection", thread_id=thread_id)
    return duckdb.connect(database=DUCKDB_PATH.as_posix(), read_only=True)


@app.get("/health")
def health() -> str:
    return str(threading.get_ident())


@app.get("/table/{table_name}.csv")
def table_csv(table_name: str, limit: int = 1000000000):
    cur = duckdb_connection(threading.get_ident())
    # TODO: DuckDB / SQLite doesn't allow parameterized table names, how do we escape it properly?
    q = f"""
    select * from {utils.underscore(table_name)}
    limit ?
    """
    return cur.execute(q, parameters=(limit,)).fetch_df().to_csv(index=False)


def _read_sql_bytes(cur, sql: str, parameters) -> io.BytesIO:
    sink = io.BytesIO()

    batch_iterator = cur.execute(sql, parameters=parameters).fetch_record_batch(
        chunk_size=1000
    )
    with pa.ipc.new_file(sink, batch_iterator.schema) as writer:
        for rb in batch_iterator:
            writer.write_batch(rb)
    sink.seek(0)

    return sink


# NOTE: this should be probably called `.arrow`
@app.get("/table/{table_name}.feather")
def table_feather(table_name: str, columns: str = "*", limit: int = 1000000000):
    cur = duckdb_connection(threading.get_ident())

    # TODO: DuckDB / SQLite doesn't allow parameterized table names, how do we escape it properly?
    # on the other hand, we have it as read-only and all data is public...
    q = f"""
    select {columns} from {utils.underscore(table_name)}
    limit ?
    """

    bytes = _read_sql_bytes(cur, q, parameters=[limit])

    return Response(bytes, media_type="application/octet-stream")


@app.post("/sql")
def sql_query(sql: str, type: str = "feather"):
    cur = duckdb_connection(threading.get_ident())

    if type == "feather":
        bytes = _read_sql_bytes(cur, sql, parameters=[])
        return Response(bytes, media_type="application/octet-stream")
    elif type == "csv":
        return cur.execute(sql).fetch_df().to_csv(index=False)


@app.get("/search")
def search(term: str):
    t = time.time()
    cur = duckdb_connection(threading.get_ident())
    log.info("search.connect", t=time.time() - t)

    # sample search
    q = """
    SELECT
        *,
        fts_main_table_meta.match_bm25(path, ?) AS score
    FROM table_meta
    where score is not null
    order by score desc
    limit 10
    """
    t = time.time()
    matches = cur.execute(q, parameters=[term]).fetch_df()
    log.info("search.execute", t=time.time() - t)

    t = time.time()
    # exclude dataset for now
    matches = matches.drop(columns=["dataset"])

    # add table name
    # TODO: return URI to table, can be `/channel/namespace/version/dataset/table`
    matches["table_name"] = matches.path.str.replace("/", "_").str.replace("-", "_")

    out = matches.fillna("").to_dict(orient="records")
    log.info("search.transform", t=time.time() - t)

    return out
