"""Load a snapshot and create a meadow dataset."""

from typing import cast

import pandas as pd
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap_years_education = cast(Snapshot, paths.load_dependency("years_of_education.xlsx"))
    snap_years_education_gini = cast(Snapshot, paths.load_dependency("years_of_education_gini.xlsx"))
    snap_years_education_gender = cast(Snapshot, paths.load_dependency("years_of_education_gender.xlsx"))
    snap_numeracy = cast(Snapshot, paths.load_dependency("numeracy.xlsx"))
    snap_numeracy_gender = cast(Snapshot, paths.load_dependency("numeracy_gender.xlsx"))
    #
    # Process data.
    #
    dfs = []

    for snap in [
        snap_years_education,
        snap_years_education_gini,
        snap_years_education_gender,
        snap_numeracy,
        snap_numeracy_gender,
    ]:
        df = pd.read_excel(snap.path)
        # Melting the DataFrame
        year_cols = [str(year) for year in range(1500, 2051)]
        df_melted = pd.melt(
            df,
            id_vars=["country name"],
            value_vars=year_cols,
            var_name="year",
            value_name=snap.metadata.short_name,
        )
        dfs.append(df_melted)

    merged_df = dfs[0]
    # Iterate through the remaining DataFrames and merge them
    for df in dfs[1:]:
        merged_df = pd.merge(merged_df, df, on=["country name", "year"], how="outer")
    merged_df.rename(columns={"country name": "country"}, inplace=True)

    # Create a new table and ensure all columns are snake-case.
    tb = Table(merged_df, short_name=paths.short_name, underscore=True)
    tb = tb.set_index(["country", "year"], verify_integrity=True)
    tb = tb.dropna(how="all")

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=None)

    # Save changes in the new garden dataset.
    ds_meadow.save()
