"""Load a meadow dataset and create a garden dataset."""

import datetime

from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

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
    "CUUR0000SA0": "all_items",
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("us_consumer_prices")

    # Read table from meadow dataset.
    tb = ds_meadow["us_consumer_prices"].reset_index()

    #
    # Process data.
    #

    # Check that each series_id/year combination doesn't have more than 12 values (1 per month)
    values_per_year = tb.groupby(["series_id", "year"], as_index=False).size()
    assert values_per_year["size"].le(12).all()

    # Cut the time series at the end of current_year-1
    tb = tb[tb.year < datetime.date.today().year]

    # Calculate the average value per series_id & year
    tb = tb[["series_id", "year", "value"]].groupby(["series_id", "year"], as_index=False).mean()

    # Translate variables to human-readable names
    tb.series_id = tb.series_id.replace(VARIABLE_NAMES)

    # Pivot to wide format and add country
    tb = tb.pivot(columns="series_id", values="value", index="year").assign(country="United States").reset_index()

    # Create a new table with the processed data.
    tb_garden = Table(tb, short_name="us_consumer_prices").set_index(["country", "year"], verify_integrity=True)

    # Ensure metadata is correctly associated.
    for column in tb_garden.columns:
        tb_garden[column].metadata.origins = tb["college_tuition_fees"].metadata.origins

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb_garden], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
