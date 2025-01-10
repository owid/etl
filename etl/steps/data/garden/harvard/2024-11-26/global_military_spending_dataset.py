"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
from owid.catalog import Dataset, Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

BURDEN_INDICATORS = ["milexgdp", "milexsurplus1095", "milexsurplus365", "milexsurplus730"]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("global_military_spending_dataset")
    ds_population = paths.load_dataset("population")
    ds_gleditsch = paths.load_dataset("gleditsch")
    ds_nmc = paths.load_dataset("national_material_capabilities")

    # Read table from meadow dataset.
    tb = ds_meadow["global_military_spending_dataset"].reset_index()
    tb_burden = ds_meadow["global_military_spending_dataset_burden"].reset_index()

    # Read Gleditsch country codes
    tb_gleditsch = ds_gleditsch["gleditsch_countries"].reset_index()

    # Read National Material Capabilities
    tb_nmc = ds_nmc["national_material_capabilities"].reset_index()

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

    tb = calculate_milex_per_capita(tb=tb, ds_population=ds_population)

    tb = calculate_milex_per_military_personnel(tb=tb, tb_nmc=tb_nmc)

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

    tb = tb[tb["indicator"] == "milex_con_sipri"].reset_index(drop=True).copy()

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


def calculate_milex_per_capita(tb: Table, ds_population: Dataset) -> Table:
    """
    Calculate military spending per capita.
    """

    tb = geo.add_population_to_table(tb=tb, ds_population=ds_population, warn_on_missing_countries=False)

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
