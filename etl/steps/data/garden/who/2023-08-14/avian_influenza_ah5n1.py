"""Load a meadow dataset and create a garden dataset."""

from typing import cast

import pandas as pd
from owid.catalog import Dataset, Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = cast(Dataset, paths.load_dependency("avian_influenza_ah5n1"))

    # Read table from meadow dataset.
    tb = ds_meadow["avian_influenza_ah5n1"].reset_index()

    #
    # Process data.
    #
    # To date
    tb["date"] = pd.to_datetime(tb["month"]).astype('datetime64[ns]')

    # Drop columns
    tb = tb.drop(columns=["range", "month"])

    # Harmonize country names
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Set index
    tb = tb.set_index(["country", "date"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()
