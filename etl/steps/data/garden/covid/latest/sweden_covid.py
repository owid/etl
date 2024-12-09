"""Load a meadow dataset and create a garden dataset."""

import numpy as np

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("sweden_covid")

    # Read table from meadow dataset.
    tb = ds_meadow["sweden_covid"].reset_index()

    #
    # Process data.
    #
    # Filter columns
    columns_rename = {
        "datum": "date",
        "avlidna_med_underliggande_orsak__antal": "confirmed_deaths",
        "avlidna_med_underliggande_orsak__rullande_medelvarde": "confirmed_deaths_avg",
    }
    tb = tb.loc[:, columns_rename.keys()].rename(columns=columns_rename)

    # Add NaNs
    tb = tb.replace("-", np.nan)

    # Dtypes
    tb = tb.astype(
        {
            "date": "datetime64[ns]",
            "confirmed_deaths": "string",
            "confirmed_deaths_avg": "string",
        }
    )

    # Add country
    tb["country"] = "Sweden"

    # Format
    tb = tb.format(["date", "country"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
