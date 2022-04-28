# TODO: combine with reindex_metadata
from pathlib import Path
import duckdb
from etl.paths import DATA_DIR

import pyarrow.feather as feather

import structlog

from api.reindex_metadata import _load_catalog_frame

DIR_PATH = Path(__file__).parent
DUCKDB_PATH = DIR_PATH / "duck.db"


log = structlog.get_logger()


def main() -> None:

    frame = _load_catalog_frame(
        exclude_datasets=[
            "world_development_indicators_world_bank",
            "food_explorer",
            "reference",
        ]
    )

    con = duckdb.connect(DUCKDB_PATH.as_posix())

    # TODO: could be faster with threading
    for _, t in frame.iterrows():
        # TODO: compare checksum to what we already have in DuckDB and only update if different
        table_name = t.path.replace("/", "_").replace("-", "_")
        table_path = (DATA_DIR / t.path).with_suffix("." + t.format)
        size_mb = table_path.stat().st_size / 1e6
        log.info(
            "loading_table",
            path=table_path,
            size=f"{size_mb:.2f} MB",
        )

        if size_mb >= 10:
            log.info("skipping_large_table", path=table_path)
            continue

        if t.format == "feather":
            con.register("t", feather.read_table(table_path))
            # NOTE: this surprisingly a bit slow (20MB/s), could it be faster?
            #  thought we might not need it if we do it incrementally
            con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM t")
        elif t.format == "csv":
            con.execute(
                f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM '{table_path}'"
            )
        else:
            raise NotImplementedError()


if __name__ == "__main__":
    main()
