"""Load a meadow dataset and create a garden dataset."""

import datetime

import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

VARIABLE_NAMES = {
    "CUUR0000SEEB01": "college_tuition_fees",
    "CUUR0000SAE1": "education",
    "CUUR0000SEEB": "tuition_fees_childcare",
    "CUUR0000SAM": "medical_care",
    "CUUR0000SAH21": "household_energy",
    "CUUR0000SAH": "housing",
    "CUUR0000SAF": "food_beverages",
    "CUUR0000SETG": "public_transport",
    "CUUS0000SS45011": "new_cars",
    "CUUR0000SAA": "clothing",
    "CUUR0000SEEE02": "software",
    "CUUR0000SERE01": "toys",
    "CUUR0000SERA01": "televisions",
}


def run(dest_dir: str) -> None:
    log.info("us_consumer_prices.start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow: Dataset = paths.load_dependency("us_consumer_prices")

    # Read table from meadow dataset.
    tb_meadow = ds_meadow["us_consumer_prices"]

    # Create a dataframe with data from the table.
    df = pd.DataFrame(tb_meadow)

    #
    # Process data.
    #

    # Check that each series_id/year combination doesn't have more than 12 values (1 per month)
    values_per_year = df.groupby(["series_id", "year"], as_index=False).size()
    assert values_per_year["size"].le(12).all()

    # Find the start for each series, and use the latest one as the start year for all series
    latest_start_year = df[["series_id", "year"]].groupby("series_id").min().year.max()
    df = df[df.year >= latest_start_year]

    # Cut the time series at the end of current_year-1
    df = df[df.year < datetime.date.today().year]

    # Calculate the average value per series_id & year
    df = df[["series_id", "year", "value"]].groupby(["series_id", "year"], as_index=False).mean()

    # Translate variables to human-readable names
    df.series_id = df.series_id.replace(VARIABLE_NAMES)

    # Pivot to wide format and add country
    df = df.pivot(columns="series_id", values="value", index="year").assign(country="United States").reset_index()

    # Create a new table with the processed data.
    tb_garden = Table(df, short_name="us_consumer_prices")

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_garden], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("us_consumer_prices.end")
