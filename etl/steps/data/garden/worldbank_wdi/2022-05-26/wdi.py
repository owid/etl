"""

Harmonize country names:

    $ harmonize data/meadow/worldbank_wdi/{version}/wdi/wdi.feather country etl/steps/data/garden/worldbank_wdi/{version}/wdi.country_mapping.json
"""
import json
from pathlib import Path
from typing import List

import pandas as pd

from owid.catalog import Dataset, Table
from etl.paths import DATA_DIR

COUNTRY_MAPPING_PATH = (Path(__file__).parent / "wdi.country_mapping.json").as_posix()


def run(dest_dir: str) -> None:
    version = Path(__file__).parent.stem
    fname = Path(__file__).stem
    namespace = Path(__file__).parent.parent.stem
    ds_meadow = Dataset((DATA_DIR / f"meadow/{namespace}/{version}/{fname}").as_posix())

    assert (
        len(ds_meadow.table_names) == 1
    ), "Expected meadow dataset to have only one table, but found > 1 table names."
    tb_meadow = ds_meadow[fname]
    df = pd.DataFrame(tb_meadow).reset_index()

    # harmonize entity names
    country_mapping = load_country_mapping()
    excluded_countries = load_excluded_countries()
    df = df.query("country not in @excluded_countries")
    assert df["country"].notnull().all()
    countries = df["country"].apply(lambda x: country_mapping.get(x, None))
    if countries.isnull().any():
        missing_countries = [
            x for x in df["country"].drop_duplicates() if x not in country_mapping
        ]
        raise RuntimeError(
            "The following raw country names have not been harmonized. "
            f"Please: (a) edit {COUNTRY_MAPPING_PATH} to include these country "
            "names; or (b) remove these country names from the raw table."
            f"Raw country names: {missing_countries}"
        )

    df["country"] = countries
    df.set_index(tb_meadow.metadata.primary_key, inplace=True)

    ds_garden = Dataset.create_empty(dest_dir)
    ds_garden.metadata = ds_meadow.metadata

    tb_garden = Table(df)
    tb_garden.metadata = tb_meadow.metadata
    ds_garden.add(tb_garden)

    ds_garden.save()


def load_country_mapping() -> dict:
    with open(COUNTRY_MAPPING_PATH, "r") as f:
        mapping = json.load(f)
    return mapping


def load_excluded_countries() -> List[str]:
    fname = Path(__file__).stem.split(".")[0]
    with open(Path(__file__).parent / f"{fname}.country_exclude.json", "r") as f:
        data = json.load(f)
    return data
