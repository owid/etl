"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define columns and their conversions
# They are all using the same conversion factor, but I am keeping them separate for clarity and future-proofing
UNIT_CONVERSIONS = {"milex": 1e3, "milper": 1e3, "irst": 1e3, "pec": 1e3, "tpop": 1e3, "upop": 1e3}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("national_material_capabilities")
    ds_cow_countries = paths.load_dataset("cow_ssm")

    # Read table from meadow dataset.
    tb = ds_meadow["national_material_capabilities"].reset_index()
    tb_cow_countries = ds_cow_countries["cow_ssm_countries"].reset_index()

    #
    # Process data.
    #
    tb = harmonize_cow_country_codes(tb=tb, tb_cow=tb_cow_countries)
    tb = adjust_units(tb=tb)

    # Add military personnel as a share of the total population
    tb["milper_share"] = tb["milper"] / tb["tpop"] * 100

    # Remove columns that are not needed
    tb = tb.drop(columns=["stateabb", "version"])

    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def harmonize_cow_country_codes(tb: Table, tb_cow: Table) -> Table:
    """
    Get code to country table, by creating an id-country table from the COW countries table, eliminating year.
    """

    # Simplify the tb_cow_countries table to only include id and country columns.
    tb_cow = tb_cow[["id", "country"]].drop_duplicates().set_index("id", verify_integrity=True).reset_index()

    # Merge the two tables to get the id-country table.
    tb = pr.merge(tb, tb_cow, how="left", left_on="ccode", right_on="id")

    # Check for missing country names
    assert tb["country"].notna().all(), f"Missing country names! {list(tb.loc[tb['country'].isna(), 'id'].unique())}"

    # Drop columns
    tb = tb.drop(columns=["ccode", "id"])

    return tb


def adjust_units(tb: Table) -> Table:
    """
    Adjust units for each column of the table.
    """

    for column, conversion in UNIT_CONVERSIONS.items():
        tb[column] = tb[column] * conversion

    return tb
