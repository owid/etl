from pathlib import Path
import pandas as pd
import duckdb
from owid.catalog import LocalCatalog
from etl.paths import DATA_DIR


import json
from typing import Dict, Any

from owid.catalog.catalogs import CatalogFrame, CatalogSeries
from etl.paths import DATA_DIR

DIR_PATH = Path(__file__).parent


DUCKDB_PATH = DIR_PATH / "duck.db"


def _load_catalog_frame(exclude_datasets=()) -> CatalogFrame:
    frame = LocalCatalog(Path(DATA_DIR)).frame
    frame = frame[frame.namespace != "open_numbers"]

    # only garden
    frame = frame[frame.path.str.startswith("garden")]

    # not this dataset yet
    frame = frame[~frame.dataset.isin(exclude_datasets)]

    return frame


def _table_metadata(table: CatalogSeries) -> Dict[str, Any]:
    # TODO: this should be a method on CatalogSeries
    with open((DATA_DIR / table.path).with_suffix(".meta.json"), "r") as f:
        meta = json.load(f)
    return meta


def main() -> None:
    frame = _load_catalog_frame(
        exclude_datasets=[
            "world_development_indicators_world_bank",
            "food_explorer",
            "reference",
        ]
    )

    metas = []

    for _, t in frame.iterrows():
        # TODO: metadata includes backlink to dataset, that could either help or hurt
        # full-text search
        metas.append(dict(t, **_table_metadata(t)))

    meta_df = pd.DataFrame(metas)

    # load it into duckdb
    con = duckdb.connect(DUCKDB_PATH.as_posix())
    con.register("meta_df", meta_df)
    con.execute("CREATE OR REPLACE TABLE table_meta AS SELECT * FROM meta_df")

    # print(con.execute("PRAGMA table_info('table_meta');").fetch_df())

    # create full text search index
    # NOTE: path is a unique identifier of a table
    # NOTE: we include numbers
    con.execute(
        "PRAGMA create_fts_index('table_meta', 'path', '*', stopwords='english', overwrite=1, ignore='(\\.|[^a-z0-9])+')"
    )

    con.close()


if __name__ == "__main__":
    main()
