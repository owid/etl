"""Load a meadow dataset and create a garden dataset."""

import json
from pathlib import Path
from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd
from owid.catalog import Dataset, Table
from owid.catalog.utils import underscore
from owid.walden import Catalog
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder
from etl.paths import DATA_DIR
from etl.snapshot import Snapshot

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("un_sdg.start")

    #
    # Load inputs.
    #
    # Load meadow dataset and relevant metadata conversions for units and dimensions
    ds_meadow: Dataset = paths.load_dependency("un_sdg")
    units: Snapshot = paths.load_dependency("un_sdg_unit.csv")
    dimensions: Snapshot = paths.load_dependency("un_sdg_dimension.json")
    # Read table from meadow dataset.
    tb_meadow = ds_meadow["un_sdg"]

    # Create a dataframe with data from the table.
    df = pd.DataFrame(tb_meadow)

    # Create units colums
    df = create_units(df)

    #
    # Process data.
    #
    log.info("un_sdg.harmonize_countries")
    df = geo.harmonize_countries(
        df=df, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )

    # Create a new table with the processed data.
    tb_garden = Table(df, like=tb_meadow)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = Dataset.create_empty(dest_dir, metadata=ds_meadow.metadata)

    # Add table of processed data to the new dataset.
    ds_garden.add(tb_garden)

    # Update dataset and table metadata using the adjacent yaml file.
    ds_garden.update_metadata(paths.metadata_path)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("un_sdg.end")


def create_units(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy(deep=False)
    unit_description = get_attributes_description()
    df["long_unit"] = df["units"].map(unit_description)
    df["short_unit"] = create_short_unit(df["long_unit"])
    return df


def get_attributes_description() -> Any:
    walden_ds = Catalog().find_one(namespace=NAMESPACE, short_name="unit", version=VERSION)
    local_file = walden_ds.ensure_downloaded()
    with open(local_file) as json_file:
        units = json.load(json_file)
    return units


def create_short_unit(long_unit: pd.Series) -> np.ndarray[Any, np.dtype[Any]]:

    conditions = [
        (long_unit.str.contains("PERCENT")) | (long_unit.str.contains("Percentage") | (long_unit.str.contains("%"))),
        (long_unit.str.contains("KG")) | (long_unit.str.contains("Kilograms")),
        (long_unit.str.contains("USD")) | (long_unit.str.contains("usd")),
    ]

    choices = ["%", "kg", "$"]

    short_unit = np.select(conditions, choices, default="")
    return short_unit
