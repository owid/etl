"""Load a snapshot and create a meadow dataset."""

from functools import reduce

import numpy as np
import owid.catalog.processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    # Load input data
    # Retrieve snapshots for both sexes and split by sex
    snaps_both_sexes = [
        paths.load_snapshot(f"pisa_{files}.xls") for files in ["math_all", "science_all", "reading_all"]
    ]
    snaps_split_by_sex = [
        paths.load_snapshot(f"pisa_{files}.xls")
        for files in [
            "math_boys_girls",
            "science_boys_girls",
            "reading_boys_girls",
        ]
    ]

    # Load and process data for both sexes combined
    tbs_both_sexes = []
    for snap in snaps_both_sexes:
        # Load data from snapshot, starting from row 12
        tb = snap.read_excel(header=11)

        # Fill missing 'Year/Study' values with the previous non-missing value
        tb["Year/Study"] = tb["Year/Study"].fillna(method="ffill")

        # Replace special characters indicating missing data with NaNs
        tb = tb.replace(["—", "†"], np.nan)

        # Rename columns for clarity
        tb = tb.rename(
            columns={"Year/Study": "year", "Jurisdiction": "country", "Average": "average", "Standard Error": "se"}
        )

        # Drop rows with missing 'country', 'average', and 'se' values
        tb = tb.dropna(subset=["country", "average", "se"])

        # Select relevant columns
        tb = tb[["year", "country", "average", "se"]]

        # Rename columns with a prefix indicating the subject
        tb = tb.rename(columns={col: snap.metadata.short_name + "_" + col for col in ["average", "se"]})

        tbs_both_sexes.append(tb)

    # Merge all dataframes for both sexes into one
    tbs_both_sexes = reduce(
        lambda left, right: pr.merge(left, right, on=["country", "year"], how="outer"), tbs_both_sexes
    )

    # Load and process data for boys and girls separately
    tbs_split = []
    for snap in snaps_split_by_sex:
        # Similar processing steps as for both sexes combined
        tb = snap.read_excel(header=11)
        tb["Year/Study"] = tb["Year/Study"].fillna(method="ffill")
        tb = tb.replace(["—", "†"], np.nan)
        tb = tb.rename(
            columns={
                "Year/Study": "year",
                "Jurisdiction": "country",
                "Average": "average_girls",
                "Standard Error": "se_girls",
                "Average.1": "average_boys",
                "Standard Error.1": "se_boys",
            }
        )
        tb = tb.dropna(subset=["country", "average_boys", "average_girls", "se_girls", "se_boys"])
        tb = tb[["year", "country", "average_boys", "average_girls", "se_girls", "se_boys"]]
        tb = tb.rename(
            columns={
                col: snap.metadata.short_name + "_" + col
                for col in ["average_boys", "average_girls", "se_girls", "se_boys"]
            }
        )

        tbs_split.append(tb)

    # Merge all dataframes for boys and girls into one
    tbs_split = reduce(lambda left, right: pr.merge(left, right, on=["country", "year"], how="outer"), tbs_split)

    # Merge both sexes and split by sex dataframes into one
    tb_merged = pr.merge(tbs_split, tbs_both_sexes, on=["year", "country"], how="outer")

    # Convert column names to snake-case, set a multi-index, and sort by index
    tb_merged = tb_merged.underscore().set_index(["year", "country"], verify_integrity=True).sort_index()

    # Save processed data
    # Create a new dataset with the same metadata as the snapshot
    ds_meadow = create_dataset(dest_dir, tables=[tb_merged], check_variables_metadata=True)

    # Save changes in the new dataset
    ds_meadow.save()
