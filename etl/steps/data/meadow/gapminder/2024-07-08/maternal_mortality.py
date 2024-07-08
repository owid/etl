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
    tb = snap.read()

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
    # Australia (first two rows span 110 years, when they should only span 10 as seen in https://docs.google.com/spreadsheets/u/0/d/14ZtQy9kd0pMRKWg_zKsTg3qKHoGtflj-Ekal9gIPZ4A/pub?gid=1#)
    # replace with middle of the decade numbers
    tb.loc[(tb["Country"] == "Australia") & (tb["year"] == "1871-1980"), "year"] = "1875"
    tb.loc[(tb["Country"] == "Australia") & (tb["year"] == "1881-1990"), "year"] = "1885"

    # wrong entries for Finland (duplicate 1772, 1775, 1967 -> should be 1872, 1875, 1957)
    tb.loc[(tb["year"] == "1772") & (tb["Country"] == "Finland") & (tb["Maternal deaths"] == 487), "year"] = "1872"
    tb.loc[(tb["year"] == "1775") & (tb["Country"] == "Finland") & (tb["Maternal deaths"] == 629), "year"] = "1875"
    tb.loc[(tb["year"] == "1967") & (tb["Country"] == "Finland") & (tb["Maternal deaths"] == 77), "year"] = "1957"

    # wrong entry for Sweden (duplicate 1967 -> should be 1957)
    tb.loc[(tb["year"] == "1967") & (tb["Country"] == "Sweden") & (tb["Maternal deaths"] == 39), "year"] = "1957"

    # wrong entry for US (duplicate 1967 -> should be 1957)
    tb.loc[
        (tb["year"] == "1967") & (tb["Country"] == "United States") & (tb["Live Births"] == 4308000), "year"
    ] = "1957"

    # wrong entry for Belgium (duplicate 1973 -> should be 1873)
    tb.loc[(tb["year"] == "1973") & (tb["Country"] == "Belgium") & (tb["Maternal deaths"] == 1283), "year"] = "1873"

    # wrong entry for New Zealand (range 1989-02 -> should be 1898-02, take midpoint 1900)
    tb.loc[(tb["year"] == "1989-02") & (tb["Country"] == "New Zealand"), "year"] = "1900"

    # duplicate entry for New Zealand (1950, drop Loudon data)
    tb = tb.drop(tb[(tb["year"] == "1950") & (tb["Country"] == "New Zealand") & (tb["MMR"] == 90)].index)

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
