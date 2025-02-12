"""Load a meadow dataset and create a garden dataset."""

import numpy as np
import pandas as pd

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("southern_oscillation_index")

    # Read table from meadow dataset.
    tb = ds_meadow.read("southern_oscillation_index")

    #
    # Process data.
    #
    tb["date"] = tb.apply(lambda row: transform_period_type(row) if row["period_type"] != "annual" else np.nan, axis=1)
    tb["annual"] = tb.apply(lambda row: row["soi"] if row["period_type"] == "annual" else np.nan, axis=1)
    tb["period_type"] = tb.apply(transform_period_type, axis=1)

    tb = tb.drop(columns={"period_type"})
    tb["annual"].metadata.origins = tb["soi"].metadata.origins

    tb = tb.format(["country", "year", "annual", "date"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def transform_period_type(row):
    # Transform period_type values into dates with an offset of 14 days
    if row["period_type"].startswith("month_"):
        month = int(row["period_type"].split("_")[1])
        return pd.to_datetime(f"{row['year']}-{month:02d}") + pd.offsets.Day(14)
    return row["period_type"]
