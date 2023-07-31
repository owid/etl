"""Load a meadow dataset and create a garden dataset."""

import os
import zipfile
from typing import List

import pandas as pd
from owid.catalog import Dataset, Table

from etl.data_helpers import geo
from etl.snapshot import Snapshot


def read_metadata(snap: Snapshot):
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
        if (
            "%" in title_to_find
            or "Percentage" in title_to_find
            or "percentage" in title_to_find
            or "share of" in title_to_find
        ):
            tb[column].metadata.display["numDecimalPlaces"] = 0
            tb[column].metadata.unit = "%"
            tb[column].metadata.short_unit = "%"
        elif "ratio" in title_to_find:
            tb[column].metadata.display["numDecimalPlaces"] = 1
            tb[column].metadata.unit = "ratio"
            tb[column].metadata.short_unit = " "
        elif "(years)" in title_to_find or "years" in title_to_find:
            tb[column].metadata.display["numDecimalPlaces"] = 1
            tb[column].metadata.unit = "years"
            tb[column].metadata.short_unit = " "
        elif "number of pupils" in title_to_find or "number" in title_to_find:
            tb[column].metadata.display["numDecimalPlaces"] = 0
            tb[column].metadata.unit = "pupils"
            tb[column].metadata.short_unit = " "
        else:
            tb[column].metadata.unit = " "
            tb[column].metadata.short_unit = " "

    return tb


def add_data_for_regions(tb: Table, regions: List[str], ds_regions: Dataset, ds_income_groups: Dataset) -> Table:
    tb_with_regions = tb.copy()

    aggregations = {
        column: "median"
        for column in tb_with_regions.columns
        if column not in ["country", "year"] and "number" not in column
    }

    for region in regions:
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
