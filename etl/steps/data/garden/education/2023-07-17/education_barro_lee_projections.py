"""Load a meadow dataset and create a garden dataset."""

from typing import cast

import education_lee_lee
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
    ds_meadow = cast(Dataset, paths.load_dependency("education_barro_lee_projections"))

    # Read table from meadow dataset.
    tb = ds_meadow["education_barro_lee_projections"]
    tb.reset_index(inplace=True)

    #
    # Process data.
    #
    tb: Table = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    tb["age_group"] = tb["age_group"].replace(
        {"15-64": "Youth and Adults (15-64 years)", "15-24": "Youth (15-24 years)", "25-64": "Adults (25-64 years)"}
    )
    df_attainment = education_lee_lee.prepare_attainment_data(tb)
    tb_future = Table(df_attainment, short_name=paths.short_name, underscore=True)
    tb_future.set_index(["country", "year"], inplace=True)

    ds_past = cast(Dataset, paths.load_dependency("education_lee_lee"))
    tb_past = ds_past["education_lee_lee"]

    cols_to_drop = [col for col in tb_past.columns if "enrollment_rates" in col]
    tb_past = tb_past.drop(columns=cols_to_drop)

    stiched = pd.concat([tb_future, tb_past])

    tb_stiched = Table(stiched, short_name=paths.short_name, underscore=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_stiched], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()
