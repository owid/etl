"""Load a meadow dataset and create a garden dataset."""

import os
from typing import cast

import shared
from owid.catalog import Dataset, Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
REGIONS = [
    "North America",
    "South America",
    "Europe",
    "European Union (27)",
    "Africa",
    "Asia",
    "Oceania",
    "Low-income countries",
    "Upper-middle-income countries",
    "Lower-middle-income countries",
    "High-income countries",
]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = cast(Dataset, paths.load_dependency("education"))

    # Load regions dataset.
    ds_regions: Dataset = paths.load_dependency("regions")

    # Load income groups dataset.
    ds_income_groups: Dataset = paths.load_dependency("income_groups")

    # Read table from meadow dataset.
    tb = ds_meadow["education"]
    tb.reset_index(inplace=True)

    #
    # Process data.
    #
    # Combine the directory and filename to get the full file path
    base_directory = os.path.dirname(paths.country_mapping_path)

    tb = geo.harmonize_countries(
        df=tb,
        excluded_countries_file=os.path.join(base_directory, "education.excluded_countries.json"),
        countries_file=os.path.join(base_directory, "education.countries.json"),
    )
    snap: Snapshot = paths.load_dependency("education.zip")
    df_metadata = shared.read_metadata(snap)
    # First, filter the DataFrame to get only the rows with the specified topics
    filtered_df = df_metadata[df_metadata["Topic"] == "Literacy"]

    # Now, you can access the rows in the indicator column
    indicator_values = filtered_df["Indicator Name"].tolist()

    tb = tb[tb["indicator_name"].isin(indicator_values)]

    tb.reset_index(inplace=True)

    # Pivot the dataframe so that each indicator is a separate column
    tb = tb.pivot(index=["country", "year"], columns="indicator_name", values="value")
    tb.reset_index(inplace=True)
    # Add region aggregates.
    tb = shared.add_data_for_regions(tb=tb, regions=REGIONS, ds_regions=ds_regions, ds_income_groups=ds_income_groups)

    # Set an appropriate index and sort conveniently.
    tb = tb.set_index(["country", "year"], verify_integrity=True).sort_index().sort_index(axis=1)

    tb = Table(tb, short_name=paths.short_name, underscore=True)
    tb = shared.add_metadata(tb, df_metadata)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=ds_meadow.metadata)
    # Save changes in the new garden dataset.
    ds_garden.save()
