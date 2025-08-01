"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table
from owid.catalog import processing as pr
from shared import add_variable_description_from_producer

from etl.data_helpers.geo import add_regions_to_table, harmonize_countries
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

REGIONS_TO_ADD = [
    "North America",
    "South America",
    "Europe",
    "Africa",
    "Asia",
    "Oceania",
    "Low-income countries",
    "Upper-middle-income countries",
    "Lower-middle-income countries",
    "High-income countries",
    "World",
]


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("budget")
    ds_regions = paths.load_dataset("regions")
    # Load income groups dataset.
    ds_income_groups = paths.load_dataset("income_groups")
    snap = paths.load_snapshot("data_dictionary.csv")
    # Read table from meadow dataset.
    tb = ds_meadow["budget"].reset_index()
    dd = snap.read(safe_types=False)
    # Process data.
    #
    tb = add_variable_description_from_producer(tb, dd)
    tb = harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    tb = calculate_budget_gap(tb)
    tb = add_regional_aggregates(tb, ds_regions, ds_income_groups)
    tb = tb.set_index(["country", "year"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()


def calculate_budget_gap(tb: Table) -> Table:
    """
    Calculating the budget gap for each country-year.

    We substract the total expected funding received from all sources (cf_tot_sources) from the total budget required (budget_tot).
    """

    tb["budget_gap"] = tb["budget_tot"].astype("Int64") - tb["cf_tot_sources"].astype("Int64")

    return tb


def add_regional_aggregates(tb: Table, ds_regions: Table, ds_income_groups: Table) -> Table:
    """
    Add regional sum aggregates for columns that aren't rates.
    """
    cols_to_drop = ["budget_cpp_dstb", "budget_cpp_mdr", "budget_cpp_xdr", "budget_cpp_tpt"]

    tb_no_agg = tb[["country", "year"] + cols_to_drop]
    tb_agg = tb.drop(columns=cols_to_drop)

    tb_agg = add_regions_to_table(
        tb_agg, ds_regions, ds_income_groups, REGIONS_TO_ADD, min_num_values_per_year=1, frac_allowed_nans_per_year=0.3
    )

    tb = pr.merge(tb_agg, tb_no_agg, on=["country", "year"], how="left")

    return tb
