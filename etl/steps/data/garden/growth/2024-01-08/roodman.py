"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("roodman")

    # Read table from meadow dataset.
    tb = ds_meadow["roodman"].reset_index()

    #
    # Process data.
    # Restructure table to include country and year columns. Also adjust numbers (population in millions, GDP in billions).
    tb = restructure_table_and_adjust_numbers(tb)

    tb = tb.set_index(["country", "year"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def restructure_table_and_adjust_numbers(tb: Table) -> Table:
    """
    Restructure a table to include country and year columns.
    Adjust numbers (population in millions, GDP in billions).
    """

    # Add a country column to each table.
    tb["country"] = "World"

    # Drop gdp_pc_france from tb
    tb = tb.drop(columns=["gdp_pc_france"])

    # Rename gwp to gdp in tb
    tb = tb.rename(
        columns={
            "gwp": "gdp",
            "gwp_pc": "gdp_pc",
        }
    )

    # Adjust numbers
    tb["population"] *= 1e-6
    tb["gdp"] *= 1e-9

    return tb
