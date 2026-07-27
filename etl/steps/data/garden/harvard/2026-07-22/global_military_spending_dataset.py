"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define milex indicator to use
MILEX_INDICATOR = "milex_con_2022_sipri"

# Define burden indicators I need
BURDEN_INDICATORS = ["milexgdp", "milexsurplus1095", "milexsurplus365", "milexsurplus730"]


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("global_military_spending_dataset")
    ds_gleditsch = paths.load_dataset("gleditsch")
    ds_nmc = paths.load_dataset("national_material_capabilities")

    # Read table from meadow dataset.
    tb = ds_meadow.read("global_military_spending_dataset")
    tb_burden = ds_meadow.read("global_military_spending_dataset_burden")

    # Read Gleditsch country codes
    tb_gleditsch = ds_gleditsch.read("gleditsch_countries")

    # Read National Material Capabilities
    tb_nmc = ds_nmc.read("national_material_capabilities")

    sanity_check_inputs(tb=tb, tb_burden=tb_burden)

    #
    # Process data.
    #
    # For tb_burden, select gwno, year, and the columns in BURDEN_INDICATORS
    tb_burden = tb_burden[["gwno", "year"] + BURDEN_INDICATORS]

    # Multiply value by 100 to get percentage
    tb_burden[BURDEN_INDICATORS] = tb_burden[BURDEN_INDICATORS] * 100

    tb = pick_gmsd_estimates(tb)

    # Merge the two tables
    tb = pr.merge(tb, tb_burden, on=["gwno", "year"], how="outer")

    tb = harmonize_country_names(tb=tb, tb_gw=tb_gleditsch)

    tb = calculate_milex_per_capita(tb=tb)

    tb = calculate_milex_per_military_personnel(tb=tb, tb_nmc=tb_nmc)

    sanity_check_outputs(tb=tb)

    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()


def sanity_check_inputs(tb: Table, tb_burden: Table) -> None:
    """Validate the source tables before any processing."""
    # The specific latent-estimate series we select must exist in the source constant-US$ table.
    assert MILEX_INDICATOR in set(tb["indicator"]), (
        f"Expected series '{MILEX_INDICATOR}' missing from source constant-US$ table."
    )
    # Every burden column we consume must be present.
    missing_burden = set(BURDEN_INDICATORS) - set(tb_burden.columns)
    assert not missing_burden, f"Burden columns missing from source: {sorted(missing_burden)}"
    # Burden series are published as fractions in [0, 1] (the source caps military burden at 1.0 = 100%).
    # NOTE: if a future release un-caps this, revisit the [0, 100] output bound in sanity_check_outputs.
    b_min = tb_burden[BURDEN_INDICATORS].min().min()
    b_max = tb_burden[BURDEN_INDICATORS].max().max()
    assert b_min >= 0 and b_max <= 1, f"Burden fractions outside [0, 1]: min={b_min}, max={b_max}"


def sanity_check_outputs(tb: Table) -> None:
    """Validate the assembled garden table before formatting."""
    # Key uniqueness.
    assert not tb.duplicated(subset=["country", "year"]).any(), "Duplicate (country, year) rows in output."
    # No indicator column should be entirely missing.
    all_nan = tb.columns[tb.isna().all()].tolist()
    assert not all_nan, f"Fully-NaN column(s): {all_nan}"
    # Military expenditure levels and per-denominator ratios are strictly positive.
    for col in ["milex_estimate", "milex_estimate_per_capita", "milex_per_military_personnel"]:
        s = tb[col].dropna()
        assert (s > 0).all(), f"Non-positive value in '{col}' (min={s.min()})."
    # Burden shares are percentages in [0, 100] (source caps burden at 100%).
    for col in BURDEN_INDICATORS:
        s = tb[col].dropna()
        assert (s >= 0).all() and (s <= 100).all(), f"'{col}' outside [0, 100]: min={s.min()}, max={s.max()}"


def get_code_to_country(tb_gw):
    """
    Get code to country table.
    From Lucas' code on `population_fariss`
    """
    # Sanity check: no duplicate country codes
    ## We expect only two codes to have multiple country names assigned: 260 and 580.
    x = tb_gw.groupby("id")["country"].nunique()
    codes = set(x[x > 1].index)
    assert codes == {260, 580}, "Unexpected duplicate country codes!"

    # Make country string
    tb_gw["country"] = tb_gw["country"].astype("string")

    # Fix: Although there were different namings in the past for countries with codes 260 and 580, we want these to have the modern name.
    tb_gw["country"] = tb_gw["country"].replace(
        {
            "Madagascar (Malagasy)": "Madagascar",
            "West Germany": "Germany",
        }
    )

    # Simplify table
    tb_gw = tb_gw[["id", "country"]].drop_duplicates().set_index("id", verify_integrity=True).reset_index()

    return tb_gw


def pick_gmsd_estimates(tb: Table) -> Table:
    """
    Pick the mean GMSD estimates in SIPRI units for military spending.
    """

    tb = tb[tb["indicator"] == MILEX_INDICATOR].reset_index(drop=True).copy()

    # Keep only country, year and mean for tb
    tb = tb[["gwno", "year", "mean"]]

    # Multiply by 1e6 to get expenditure in US$
    tb["mean"] = tb["mean"] * 1e6

    # Rename columns
    tb = tb.rename(columns={"mean": "milex_estimate"})

    return tb


def make_burden_table_wide(tb: Table) -> Table:
    """
    Make the military burden table wide.
    """

    tb = tb[["ccode", "year", "indicator", "value"]]

    # Multiply value by 100 to get percentage
    tb.loc[:, "value"] = tb["value"] * 100

    tb = tb.pivot(index=["ccode", "year"], columns="indicator", values="value").reset_index()

    # Rename columns
    tb = tb.rename(columns={"ccode": "gwno"})

    return tb


def harmonize_country_names(tb: Table, tb_gw: Table) -> Table:
    """
    Harmonize country names in the table.
    """

    # Get code to country table
    tb_gw = get_code_to_country(tb_gw)

    # Get country names
    tb = pr.merge(tb, tb_gw, left_on=["gwno"], right_on=["id"], how="left")

    # Check for missing country names
    assert tb["country"].notna().all(), f"Missing country names! {list(tb.loc[tb['country'].isna(), 'gwno'].unique())}"

    # Drop columns
    tb = tb.drop(columns=["gwno", "id"])

    return tb


def calculate_milex_per_capita(tb: Table) -> Table:
    """
    Calculate military spending per capita.
    """

    tb = paths.regions.add_population(tb=tb, warn_on_missing_countries=False)

    # Calculate military spending per capita
    tb["milex_estimate_per_capita"] = tb["milex_estimate"] / tb["population"]

    # Drop population column
    tb = tb.drop(columns=["population"])

    return tb


def calculate_milex_per_military_personnel(tb: Table, tb_nmc: Table) -> Table:
    """
    Calculate military spending per military personnel.
    """

    # Merge tables
    tb = pr.merge(tb, tb_nmc[["country", "year", "milper"]], on=["country", "year"], how="left")

    # Calculate military spending per military personnel
    tb["milex_per_military_personnel"] = tb["milex_estimate"] / tb["milper"]

    # Replace infinite values with NaN
    tb["milex_per_military_personnel"] = tb["milex_per_military_personnel"].replace(
        [float("inf"), float("-inf")], float("nan")
    )

    # Drop milper column
    tb = tb.drop(columns=["milper"])

    return tb
