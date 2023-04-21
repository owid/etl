"""Load a meadow dataset and create a garden dataset."""

import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Unit conversion factor to change from bushels per acre to tonnes per hectare.
# NOTE: This may be the American definition of bushel (which may differ from the British one).
#  Also, check how much the bushel to tonnes conversion changes for different commodities.
BUSHELS_PER_ACRE_TO_TONNES_PER_HECTARE = 0.06725


def run(dest_dir: str) -> None:
    log.info("broadberry_et_al_2015.start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow: Dataset = paths.load_dependency("broadberry_et_al_2015")

    # Read table from meadow dataset.
    tb_meadow = ds_meadow["broadberry_et_al_2015"]

    # Create a dataframe with data from the table.
    df = pd.DataFrame(tb_meadow)

    #
    # Process data.
    #
    # Data is given as decadal averages. Use the average year of each decade (e.g. instead of 1300, use 1305).
    df = df.rename(columns={"decade": "year"})
    df["year"] += 5

    # Add a country column, set an appropriate index and sort conveniently
    df = (
        df.rename(columns={"decade": "year"})
        .assign(**{"country": "United Kingdom"})
        .set_index(["country", "year"], verify_integrity=True)
        .sort_index()
    )

    # Rename columns.
    df = df.rename(columns={column: column + "_yield" for column in df.columns})

    # Ensure all numeric columns are standard floats, and convert units.
    for column in df.columns:
        df[column] = df[column].astype(float) * BUSHELS_PER_ACRE_TO_TONNES_PER_HECTARE

    # Create a new table with the processed data.
    tb_garden = Table(df, short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_garden], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("broadberry_et_al_2015.end")
