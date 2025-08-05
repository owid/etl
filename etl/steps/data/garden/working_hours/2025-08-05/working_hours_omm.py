"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow_pwt = paths.load_dataset("penn_world_table")
    ds_meadow_hm = paths.load_dataset("huberman_minns")

    # Read table from meadow dataset.
    tb_pwt = ds_meadow_pwt.read("penn_world_table")
    tb_hm = ds_meadow_hm.read("huberman_minns")

    #
    # Process data.
    #
    tb = merge_tables(tb_pwt, tb_hm)

    print(tb)

    # Improve table format.
    tb = tb.format(["country", "year"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow_pwt.metadata)

    # Save garden dataset.
    ds_garden.save()


def merge_tables(tb_pwt: Table, tb_hm: Table) -> Table:
    """
    Merge the PWT and Huberman and Minns tables on country and year.
    """
    tb_pwt = tb_pwt.copy()
    tb_hm = tb_hm.copy()

    # Select relevant columns
    tb_pwt = tb_pwt[["country", "year", "avh"]]
    tb_hm = tb_hm[["country", "year", "working_hours_year"]]

    # Rename hours columns before merging
    tb_pwt = tb_pwt.rename(columns={"avh": "working_hours_omm"})
    tb_hm = tb_hm.rename(columns={"working_hours_year": "working_hours_omm"})

    # Select Huberman and Minns data only until 1938
    tb_hm = tb_hm[tb_hm["year"] <= 1938].reset_index(drop=True)

    # Concatenate the two tables
    tb = pr.concat([tb_pwt, tb_hm], ignore_index=True)

    # Make working_hours_omm float
    tb["working_hours_omm"] = tb["working_hours_omm"].astype(float)

    return tb
