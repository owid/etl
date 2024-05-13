"""Load a meadow dataset and create a garden dataset.

LABELS

`exec_reccomp_polity`
    0 "power seized"
    1 "elite selection"
    2 "dual or transitional"
    3 "election"

`exec_recopen_polity`
    0 "power seized"
    1 "hereitary succession"
    2 "dual, chief minister designated"
    3 "dual, chief minister elected"
    4 "open"

`exec_constr_polity`
    1 "unconstrained"
    2 ?
    3 "slight to moderate"
    4 ?
    5 "substantial"
    6 ?
    7 "executive parity or subordination"

`polpart_reg_polity`
    1 "unregulated"
    2 "multiple identities"
    3 "sectarian"
    4 "restricted"
    5 "unrestricted and stable"

`polpart_comp_polity`
    0 "unregulated"
    1 "repressed"
    2 "suppressed"
    3 "factional"
    4 "transitional"
    5 "competitive"
"""

import pandas as pd
from owid.catalog import Table
from shared import add_age_groups, add_count_years_in_regime

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Regime labels
REGIME_LABELS = {
    0: "autocracy",
    1: "anocracy",
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("polity")

    # Read table from meadow dataset.
    tb = ds_meadow["polity"].reset_index()

    #
    # Process data.
    #
    # Column rename
    columns_rename = {
        "polity2": "democracy_polity",
        "xrcomp": "exec_reccomp_polity",
        "xropen": "exec_recopen_polity",
        "xconst": "exec_constr_polity",
        "parreg": "polpart_reg_polity",
        "parcomp": "polpart_comp_polity",
    }
    tb = tb.rename(columns=columns_rename)

    # Assign NaNs to categories -66, -77 and -88
    cols = list(columns_rename.values())
    tb[cols] = tb[cols].replace([-66, -77, -88], pd.NA).astype("Int64")

    # Generate regime variables as per the (conventional) rules here: https://www.systemicpeace.org/polityproject.html
    tb = add_regime_category(tb)

    # Harmonize country names
    tb = harmonize_country_names(tb)

    # Recode
    tb["democracy_recod_polity"] = tb["democracy_polity"] + 10

    # Age and experience of democracy
    tb = add_age_and_experience(tb)

    # Remove columns
    tb = tb.drop(columns=["ccode"])

    # Format table
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


def add_regime_category(tb: Table) -> Table:
    """Estimate indicator for regime type based on `democracy_polity` categories."""
    series = pd.cut(tb["democracy_polity"], [-10.1, -6, 5, 10], labels=[0, 1, 2])
    tb["regime_polity"] = series.copy_metadata(tb["democracy_polity"])
    return tb


def harmonize_country_names(tb: Table) -> Table:
    """Harmonize country names, including former state corrections."""
    ## Fix Pakistan entity (ccode = 769 is actually 'Pakistan (former)')
    tb["country"] = tb["country"].astype("string")
    tb.loc[tb["ccode"] == 769, "country"] = "Pakistan (former)"
    ## Classic harmization
    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
    )

    # Sanity check: Only one ccode for country
    assert tb.groupby("country")["ccode"].nunique().max() == 1, "Multiple ccode for country"

    return tb


def add_age_and_experience(tb: Table) -> Table:
    """Add age and experience related indicators.

    This includes:
        - Number of consecutive years in electoral democracy and polyarchy (age)
        - Number of total years in electoral democracy and polyarchy (experience)
        - Age groups for electoral democracy and polyarchy
    """
    columns = [
        ("regime_polity", "dem_polity", 1),
        # ("regime_lied", "polyarchy_lied", 6),
    ]
    # Add age and experience counts
    tb = add_count_years_in_regime(
        tb=tb,
        columns=columns,  # type: ignore
        na_is_zero=True,
    )

    for col in columns:
        col_age = f"age_{col[1]}"

        # Add age groups
        tb = add_age_groups(tb=tb, column=col_age, column_raw=col[0], category_names=REGIME_LABELS, threshold=col[2])

        # Replace category numbers with labels (age in *)
        mapping = {num: label for num, label in REGIME_LABELS.items() if num <= col[2]}
        mask = (tb[col_age] == 0) | (tb[col_age].isna())
        tb.loc[mask, col_age] = tb.loc[mask, col[0]].replace(mapping)
        tb[col_age] = tb[col_age].astype("string")

    return tb
