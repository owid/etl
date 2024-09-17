"""Load a meadow dataset and create a garden dataset."""

from typing import List

import pandas as pd
from owid.catalog import Table
from owid.catalog.processing import concat

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


# Rename organism
ORGANISM_RENAME = {
    "Human_coronavirus": "Other coronaviruses",
    "Human_coronavirus.2019": "COVID-19",
    "Influenza.A.H3N2": "Influenza A (H3N2)",
    "Influenza.A.H1N1": "Influenza A (H1N1)",
    "Influenza.B": "Influenza B",
    "RSV.A": "RSV A",
    "RSV.B": "RSV B",
    "Adenovirus": "Adenovirus",
    "Enterovirus": "Enterovirus",
    "Human_metapneumovirus": "Metapneumovirus",
    "Human_parainfluenza": "Parainfluenza",
    "Rhinovirus": "Rhinovirus",
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("seattle_pathogens")

    # Read table from meadow dataset.
    tb = ds_meadow["seattle_pathogens"].reset_index()

    #
    # Process data.
    #
    # Check all organisms are present
    assert set(tb["organism"].unique()) == set(ORGANISM_RENAME.keys())
    # Rename organism
    tb["organism"] = tb["organism"].cat.rename_categories(ORGANISM_RENAME)
    # Change date format from week YYYY-Www to YYYY-MM-DD
    tb["date"] = pd.to_datetime(tb["week"].astype("string") + "-1", format="%G-W%V-%w")
    # Keep relevant columns
    tb = tb[["date", "organism", "present", "tested"]]

    # Month
    tb_month = make_monthly_table(tb)

    # Weekly data
    tb = process_table(tb)
    # Monthly data
    tb_month = process_table(tb_month)

    # Format
    tables = [
        tb.format(["country", "date", "organism"]),
        tb_month.format(["country", "date", "organism"], short_name="seattle_pathogens_month"),
    ]

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir,
        tables=tables,
        check_variables_metadata=True,
        default_metadata=ds_meadow.metadata,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def add_pathogen_group(tb: Table, group_name: str, group_pathogens: List[str]):
    tb_group = tb[tb["organism"].isin(group_pathogens)].copy()

    tb_group = tb_group.groupby("date", as_index=False)[["present", "percentage"]].sum()
    tb_group["organism"] = group_name

    tb = concat([tb, tb_group])
    return tb


def make_monthly_table(tb: Table) -> Table:
    tb_month = tb.copy()
    # Extract year and month from date
    tb_month["date"] = tb_month["date"].dt.to_period("M")
    # Group by 'date' and 'organism' and sum the 'present' values
    tb_month = tb_month.groupby(["date", "organism"], as_index=False)[["present", "tested"]].sum()
    # Convert 'date' back to datetime format if needed
    tb_month["date"] = tb_month["date"].dt.to_timestamp(how="end").dt.date

    return tb_month


def process_table(tb: Table) -> Table:
    # Estimate percentage
    assert (tb["tested"] != 0).all(), "Some zeroes in tested column! This can lead to division by zero."
    tb["percentage"] = 100 * tb["present"] / tb["tested"]

    # Add groupings
    groups = {
        "RSV": ["RSV A", "RSV B"],
        "Influenza A": ["Influenza A (H3N2)", "Influenza A (H1N1)"],
        # # "Influenza": ["Influenza A (H3N2)", "Influenza A (H1N1)", "Influenza B"],
    }
    for group_name, group_pathogens in groups.items():
        tb = add_pathogen_group(tb, group_name, group_pathogens)

    # Add entity
    tb["country"] = "Seattle"

    return tb
