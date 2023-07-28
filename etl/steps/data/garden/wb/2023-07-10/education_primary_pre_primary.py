"""Load a meadow dataset and create a garden dataset."""

import os
import zipfile
from typing import List, cast

import pandas as pd
from owid.catalog import Dataset, Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot


def read_metadata():
    snap: Snapshot = paths.load_dependency("education.zip")

    # Step 1: Unzip the file
    with zipfile.ZipFile(snap.path, "r") as zip_ref:
        # Replace 'data.csv' with the name of your CSV file in the zip archive
        csv_file_name = "EdStatsSeries.csv"
        destination_directory = os.path.dirname(snap.path)
        zip_ref.extract(csv_file_name, destination_directory)
    df_metadata = pd.read_csv(os.path.join(destination_directory, csv_file_name))
    return df_metadata


def add_metadata(tb, df_metadata):
    df_metadata = df_metadata[["Indicator Name", "Source", "Long definition"]]

    for column in tb.columns:
        title_to_find = tb[column].metadata.title
        tb[column].metadata.description = df_metadata["Long definition"][df_metadata["Indicator Name"] == title_to_find]
        tb[column].metadata.display = {}
        tb[column].metadata.display["numDecimalPlaces"] = 0

    return tb


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


def add_data_for_regions(tb: Table, regions: List[str], ds_regions: Dataset, ds_income_groups: Dataset) -> Table:
    tb_with_regions = tb.copy()

    aggregations = {column: "median" for column in tb_with_regions.columns if column not in ["country", "year"]}

    for region in REGIONS:
        # Find members of current region.
        members = geo.list_members_of_region(
            region=region,
            ds_regions=ds_regions,
            ds_income_groups=ds_income_groups,
        )
        tb_with_regions = geo.add_region_aggregates(
            df=tb_with_regions,
            region=region,
            countries_in_region=members,
            countries_that_must_have_data=[],
            num_allowed_nans_per_year=None,
            frac_allowed_nans_per_year=0.99999,
            aggregations=aggregations,
        )
    tb_with_regions = tb_with_regions.copy_metadata(from_table=tb)

    return tb_with_regions


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
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    df_metadata = read_metadata()
    # First, filter the DataFrame to get only the rows with the specified topics
    filtered_df = df_metadata[df_metadata["Topic"].isin(["Early Childhood Education", "Pre-Primary", "Primary"])]

    # Now, you can access the rows in the indicator column
    indicator_values = filtered_df["Indicator Name"].tolist()

    tb = tb[tb["indicator_name"].isin(indicator_values)]

    tb.reset_index(inplace=True)

    # Pivot the dataframe so that each indicator is a separate column
    tb = tb.pivot(index=["country", "year"], columns="indicator_name", values="value")
    tb.reset_index(inplace=True)
    # Add region aggregates.
    tb = add_data_for_regions(tb=tb, regions=REGIONS, ds_regions=ds_regions, ds_income_groups=ds_income_groups)

    # Set an appropriate index and sort conveniently.
    tb = tb.set_index(["country", "year"], verify_integrity=True).sort_index().sort_index(axis=1)

    tb = Table(tb, short_name=paths.short_name, underscore=True)
    tb = add_metadata(tb, df_metadata)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=ds_meadow.metadata)
    # Save changes in the new garden dataset.
    ds_garden.save()
