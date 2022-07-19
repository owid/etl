"""Garden step for Shift data on energy production from fossil fuels.

"""

import json
from pathlib import Path
from typing import Dict, List

import pandas as pd
from owid.catalog import Dataset, Table
from owid.catalog.utils import underscore_table
from owid.datautils import geo
from structlog import get_logger

from etl.paths import DATA_DIR

log = get_logger()

NAMESPACE = "shift"
DATASET_SHORT_NAME = "shift_fossil_fuel_production"

VERSION = Path(__file__).parent.name
COUNTRY_MAPPING_PATH = Path(__file__).parent / f"{DATASET_SHORT_NAME}.country_mapping.json"
METADATA_PATH = Path(__file__).parent / f"{DATASET_SHORT_NAME}.meta.yml"


def run(dest_dir: str) -> None:
    log.info(f"{DATASET_SHORT_NAME}.start")
    #
    # Load data.
    #
    # Load meadow dataset and get the only table inside (with the same name).
    ds_meadow = Dataset(DATA_DIR / f"meadow/{NAMESPACE}/{VERSION}/{DATASET_SHORT_NAME}")
    tb_meadow = ds_meadow[DATASET_SHORT_NAME]

    # Convert table into a dataframe.
    df = pd.DataFrame(tb_meadow)

    #
    # Process data.
    #
    # Harmonize country names.
    log.info(f"{DATASET_SHORT_NAME}.harmonize_countries")
    df = geo.harmonize_countries(df=df, countries_file=str(COUNTRY_MAPPING_PATH))

    # TODO: Shift treats Russia and USSR as the same entity. Split them as separate ones.

    #
    # Save outputs.
    #
    # Create a new garden dataset (with the same metadata as the meadow version).
    ds_garden = Dataset.create_empty(dest_dir)
    ds_garden.metadata = ds_meadow.metadata

    # Create a new table.
    tb_garden = underscore_table(Table(df))
    tb_garden.metadata = tb_meadow.metadata

    ds_garden.metadata.update_from_yaml(METADATA_PATH)
    tb_garden.update_metadata_from_yaml(METADATA_PATH, DATASET_SHORT_NAME)
    
    ds_garden.add(tb_garden)
    ds_garden.save()

    log.info(f"{DATASET_SHORT_NAME}.end")
