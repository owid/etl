"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

VARIABLE_NAMES = {
    "CUUR0000SEEB01": "college_tuition_fees",
    "CUUR0000SAE1": "education",
    "CUUR0000SEEB03": "childcare",
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


def run() -> None:
    #
    # Load inputs.
    #
    # Load snapshot.
    snap = paths.load_snapshot("us_consumer_prices.csv")

    # Load data from snapshot.
    tb = snap.read(safe_types=False)

    #
    # Process data.
    #
    # Keep annual data only
    tb = tb[tb["period_name"].isin(["Annual"])]
    tb = tb.drop(columns=["period_name"])

    # Translate variables to human-readable names
    tb.series_id = tb.series_id.replace(VARIABLE_NAMES)

    # Pivot to wide format and add country
    tb = tb.pivot(columns="series_id", values="value", index="year").assign(country="United States").reset_index()
    # Create a new table with the processed data.
    tb_garden = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(tables=[tb_garden], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()
