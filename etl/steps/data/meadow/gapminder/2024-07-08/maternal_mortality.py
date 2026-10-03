"""Load a snapshot and create a meadow dataset."""

import pandas as pd

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("maternal_mortality.xlsx")

    # Load data from snapshot.
    tb = snap.read(safe_types=False)

    # drop source & comment columns
    tb = tb.drop(
        columns=[
            "Unnamed: 7",
            "Source MMR, maternal death and Live birth",
            "Source women reproductive age",
            "Comment",
            "Comments",
        ]
    )

    # drop columns without year
    tb = tb.dropna(subset=["year"])

    # remove leading/ trailing whitespaces from country names
    tb["Country"] = tb["Country"].str.strip()

    # replace ../ .../ no data with None
    tb = tb.replace("..", pd.NA)
    tb = tb.replace("...", pd.NA)
    tb = tb.replace("no data", pd.NA)

    # change year to string (to allow for ranges, these will be fixed in garden step), change other columns to numeric
    tb["year"] = tb["year"].astype(str)
    tb["Maternal deaths"] = tb["Maternal deaths"].astype("Float64")
    tb["MMR"] = tb["MMR"].astype("Float64")
    tb["Live Births"] = tb["Live Births"].astype("Float64")
    tb["Maternal deaths"] = tb["Maternal deaths"].astype("Float64")
    tb["MM-rate"] = tb["MM-rate"].astype("Float64")

    #
    # cleaning errors (manually):
    tb = paths.apply_corrections(tb, country_col="Country", year_col="year")

    # remove zeros from data (these are artifacts)
    # check with - print(tb.loc[tb[tb==0].dropna(how="all").index])
    tb = tb.replace(0, pd.NA)

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
