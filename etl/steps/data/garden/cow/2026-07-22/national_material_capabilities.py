"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define columns and their conversions
# They are all using the same conversion factor, but I am keeping them separate for clarity and future-proofing
UNIT_CONVERSIONS = {"milex": 1e3, "milper": 1e3, "irst": 1e3, "pec": 1e3, "tpop": 1e3, "upop": 1e3}

# Columns expected in the abridged source file (missing values coded as -9, converted to NaN in meadow).
EXPECTED_INPUT_COLUMNS = {
    "stateabb",
    "ccode",
    "year",
    "milex",
    "milper",
    "irst",
    "pec",
    "tpop",
    "upop",
    "cinc",
    "version",
}


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("national_material_capabilities")
    ds_cow_countries = paths.load_dataset("cow_ssm")

    # Read table from meadow dataset.
    tb = ds_meadow.read("national_material_capabilities")
    tb_cow_countries = ds_cow_countries.read("cow_ssm_countries")

    sanity_check_inputs(tb=tb)

    #
    # Process data.
    #
    tb = harmonize_cow_country_codes(tb=tb, tb_cow=tb_cow_countries)
    tb = adjust_units(tb=tb)

    # Add military personnel as a share of the total population
    tb["milper_share"] = tb["milper"] / tb["tpop"] * 100

    # Remove columns that are not needed
    tb = tb.drop(columns=["stateabb", "version"])

    sanity_check_outputs(tb=tb)

    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()


def sanity_check_inputs(tb: Table) -> None:
    """Validate the meadow table before processing."""
    missing = EXPECTED_INPUT_COLUMNS - set(tb.columns)
    assert not missing, f"Meadow table missing expected columns: {sorted(missing)}"
    assert not tb.duplicated(subset=["stateabb", "year"]).any(), "Duplicate (stateabb, year) rows in meadow input."


def sanity_check_outputs(tb: Table) -> None:
    """Validate the assembled garden table before formatting."""
    assert not tb.duplicated(subset=["country", "year"]).any(), "Duplicate (country, year) rows in output."
    all_nan = tb.columns[tb.isna().all()].tolist()
    assert not all_nan, f"Fully-NaN column(s): {all_nan}"
    # Capability components and expenditure are non-negative amounts/counts.
    for col in ["milex", "milper", "irst", "pec", "upop", "milper_share"]:
        s = tb[col].dropna()
        assert (s >= 0).all(), f"Negative value in '{col}' (min={s.min()})."
    # Total population is strictly positive.
    assert (tb["tpop"].dropna() > 0).all(), "Non-positive total population found."
    # CINC is a share of the world total, bounded to [0, 1].
    cinc = tb["cinc"].dropna()
    assert (cinc >= 0).all() and (cinc <= 1).all(), f"CINC outside [0, 1]: min={cinc.min()}, max={cinc.max()}."
    # Military personnel as a share of population is a percentage in [0, 100].
    mps = tb["milper_share"].dropna()
    assert (mps >= 0).all() and (mps <= 100).all(), f"milper_share outside [0, 100]: min={mps.min()}, max={mps.max()}."


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
