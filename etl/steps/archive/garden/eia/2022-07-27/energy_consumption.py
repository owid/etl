"""Garden step for EIA total energy consumption.

"""

import pandas as pd
from owid import catalog
from owid.catalog.utils import underscore_table
from shared import (
    CURRENT_DIR,
    NAMESPACE,
    OVERLAPPING_DATA_TO_REMOVE_IN_AGGREGATES,
    REGIONS_TO_ADD,
    VERSION,
    add_region_aggregates,
    log,
)

from etl.data_helpers import geo
from etl.paths import DATA_DIR

DATASET_SHORT_NAME = "energy_consumption"
# Path to country mapping file.
COUNTRY_MAPPING_PATH = CURRENT_DIR / "energy_consumption.countries.json"
# Path to metadata file.
METADATA_PATH = CURRENT_DIR / "energy_consumption.meta.yml"

# Conversion factor from terajoules to terawatt-hours.
TJ_TO_TWH = 1 / 3600


def run(dest_dir: str) -> None:
    log.info(f"{DATASET_SHORT_NAME}.start")
    #
    # Load data.
    #
    # Load meadow dataset and get the only table inside (with the same name).
    ds_meadow = catalog.Dataset(DATA_DIR / f"meadow/{NAMESPACE}/{VERSION}/{DATASET_SHORT_NAME}")
    tb_meadow = ds_meadow[DATASET_SHORT_NAME]

    # Convert table into a dataframe.
    df = pd.DataFrame(tb_meadow).reset_index()

    #
    # Process data.
    #
    # Harmonize country names.
    log.info(f"{DATASET_SHORT_NAME}.harmonize_countries")
    df = geo.harmonize_countries(df=df, countries_file=str(COUNTRY_MAPPING_PATH))

    # Convert terajoules to terawatt-hours.
    df["energy_consumption"] = df["values"] * TJ_TO_TWH
    df = df.drop(columns=["values", "members"])

    # Create aggregate regions.
    log.info(f"{DATASET_SHORT_NAME}.add_region_aggregates")
    df = add_region_aggregates(
        data=df,
        regions=list(REGIONS_TO_ADD),
        index_columns=["country", "year"],
        known_overlaps=OVERLAPPING_DATA_TO_REMOVE_IN_AGGREGATES,  # type: ignore
    )

    # Prepare output data.
    df = df.set_index(["country", "year"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new garden dataset (with the same metadata as the meadow version).
    ds_garden = catalog.Dataset.create_empty(dest_dir)
    # ds_garden.metadata = ds_meadow.metadata
    ds_garden.metadata.update_from_yaml(METADATA_PATH)
    ds_garden.save()

    # Create a new table.
    tb_garden = underscore_table(catalog.Table(df))
    tb_garden.metadata = tb_meadow.metadata
    tb_garden.update_metadata_from_yaml(METADATA_PATH, DATASET_SHORT_NAME)
    # Add table to dataset.
    ds_garden.add(tb_garden)

    log.info(f"{DATASET_SHORT_NAME}.end")
