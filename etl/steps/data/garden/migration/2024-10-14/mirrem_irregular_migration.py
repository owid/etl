"""Load a meadow dataset and create a garden dataset."""

import pandas as pd

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("mirrem_irregular_migration")

    # Read table from meadow dataset.
    tb = ds_meadow["mirrem_irregular_migration"].reset_index()

    tb["high_est_<100k"] = tb["highestimate"] == "<100,000"
    tb["low_est_<100k"] = tb["lowestimate"] == "-"

    tb = tb.replace("nan", pd.NA).replace("NaN", pd.NA)

    tb["lowestimate"] = tb["lowestimate"].replace("-", 0).replace("<100,000", 0).astype("Float64")
    tb["highestimate"] = tb["highestimate"].replace("<100,000", 100000).astype("Float64")
    tb["centralestimate"] = tb["centralestimate"].astype("Float64")

    tb["centralestimate"] = tb["centralestimate"].fillna((tb["highestimate"] + tb["lowestimate"]) / 2)

    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
