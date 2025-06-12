"""Load a garden dataset and create a grapher dataset."""

import pandas as pd

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("sst")

    # Read table from garden dataset.
    tb = ds_garden.read("sst")

    # Combine month and year into a single column.
    tb["date"] = pd.to_datetime(tb["year"].astype(str) + "-" + tb["month"].astype(str) + "-01")
    tb["date"] = tb["date"] + pd.offsets.Day(14)

    # Create colour_date column based on decades
    def year_to_decade(year):
        return (year - 1950) // 10 + 1

    tb["colour_date"] = tb["year"].apply(year_to_decade)

    tb["colour_date"] = tb.apply(
        lambda row: row["colour_date"] + 8
        if (row["nino_classification"] == 1 and 1 <= row["colour_date"] <= 9)
        else row["colour_date"],
        axis=1,
    )
    tb["colour_date"] = tb.apply(
        lambda row: 0 if row["nino_classification"] == 0 else row["colour_date"],
        axis=1,
    )
    tb["colour_date"] = tb["colour_date"].copy_metadata(tb["oni_anomaly"])
    # Create date_as_country column (keep uncommented but might use in the future)
    # tb["date_as_country"] = tb["date"].dt.strftime("%B %Y")

    # Drop the original year and month columns
    tb = tb.drop(columns=["year", "month"])

    tb = tb.format(["date", "country"])
    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=ds_garden.metadata)

    # Save changes in the new grapher dataset.
    ds_grapher.save()
