"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

REGIONS = geo.REGIONS


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_igme = paths.load_dataset("igme")
    ds_gapminder_v11 = paths.load_dataset("under_five_mortality", version="2023-09-21")
    ds_gapminder_v7 = paths.load_dataset("under_five_mortality", version="2023-09-18")

    # Read table from meadow dataset and filter out the main indicator, under five mortality, central estimate, both sexes, all wealth quintiles.
    tb_igme = ds_igme.read("igme", reset_metadata="keep_origins")
    tb_igme = tb_igme[
        (tb_igme["indicator"] == "Child mortality rate")
        & (tb_igme["sex"] == "Total")
        & (tb_igme["wealth_quintile"] == "Total")
    ]
    assert len(tb_igme) > 0, "Check you are using the right terms to filter the data"
    tb_igme = tb_igme.rename(columns={"observation_value": "child_mortality_rate"}, errors="raise").drop(
        columns=["sex", "wealth_quintile", "indicator", "unit_of_measure"]
    )
    # Select out columns of interest.
    tb_igme["source"] = "UN IGME"

    # Load full Gapminder data v11, v11 includes projections, so we need to remove years beyond the last year of IGME data

    max_year = tb_igme["year"].max()
    tb_gap_full = ds_gapminder_v11["under_five_mortality"].reset_index()
    tb_gap_full = tb_gap_full[tb_gap_full["year"] <= max_year].reset_index(drop=True)
    tb_gap_full = tb_gap_full.rename(columns={"child_mortality": "child_mortality_rate"}, errors="raise")
    tb_gap_full["source"] = "Gapminder"
    tb_gap_full["child_mortality_rate"] = tb_gap_full["child_mortality_rate"].div(10)

    # Load Gapminder data v7 - has the source of the data (unlike v11)
    # We've removed some years from the v7 data, for years where the source was 'Guesstimate' or 'Model based on Life Expectancy'
    tb_gap_sel = ds_gapminder_v7["under_five_mortality_selected"].reset_index()
    tb_gap_sel["source"] = "Gapminder"
    tb_gap_sel = tb_gap_sel.rename(columns={"under_five_mortality": "child_mortality_rate"}, errors="raise")
    tb_gap_sel["child_mortality_rate"] = tb_gap_sel["child_mortality_rate"].div(10)
    # Remove the early years for Austria - there is a signicant jump in the data in 1830 which suggests an incongruency in method or data availability
    tb_gap_sel = remove_early_years_austria(tb_gap_sel)

    # Combine IGME and Gapminder data with two versions

    tb_combined_full = combine_datasets(tb_igme, tb_gap_full, "long_run_child_mortality")
    tb_combined_sel = combine_datasets(tb_igme, tb_gap_sel, "long_run_child_mortality_selected")

    # Calculate and estimate globally the number of children surviving their first five years
    tb_surviving = calculate_share_surviving_first_five_years(tb_combined_full)
    # Combine with full Gapminder dataset
    tb_combined_full = pr.merge(tb_combined_full, tb_surviving, on=["country", "year"], how="left")
    tb_combined_full = tb_combined_full.rename(
        columns={"child_mortality_rate": "child_mortality_rate_full"}, errors="raise"
    )

    # Save outputs.
    tb_combined_full = tb_combined_full.drop(columns=["source"]).format(["country", "year"])
    tb_combined_sel = tb_combined_sel.format(["country", "year"])
    tb_combined_sel["source"].metadata.origins = tb_combined_sel["child_mortality_rate"].metadata.origins

    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(tables=[tb_combined_full, tb_combined_sel], check_variables_metadata=True)
    # Save changes in the new garden dataset.
    ds_garden.save()


def combine_datasets(tb_igme: Table, tb_gap: Table, table_name: str) -> Table:
    """
    Combine IGME and Gapminder data.
    """
    tb_combined = pr.concat([tb_igme, tb_gap]).sort_values(["country", "year", "source"])
    tb_combined.metadata.short_name = table_name
    tb_combined = remove_duplicates(tb_combined, preferred_source="UN IGME")

    return tb_combined


def remove_early_years_austria(tb: Table) -> Table:
    """
    Remove years prior to 1830 for Austria - there is a signicant jump in the data in 1830 which suggests an incongruency in method or data availability
    """
    # Remove years prior to 1830 for Austria
    msk = (tb["country"] == "Austria") & (tb["year"] < 1830)
    tb = tb[~msk]

    return tb


def remove_duplicates(tb: Table, preferred_source: str) -> Table:
    """
    Removing rows where there are overlapping years with a preference for IGME data.

    """
    assert tb["source"].str.contains(preferred_source).any()

    duplicate_rows = tb.duplicated(subset=["country", "year"], keep=False)

    tb_no_duplicates = tb[~duplicate_rows]

    tb_duplicates = tb[duplicate_rows]

    tb_duplicates_removed = tb_duplicates[tb_duplicates["source"] == preferred_source]

    tb = pr.concat([tb_no_duplicates, tb_duplicates_removed])

    assert len(tb[tb.duplicated(subset=["country", "year"], keep=False)]) == 0

    return tb


def calculate_share_surviving_first_five_years(tb_combined: Table) -> Table:
    """
    Calculate and estimate globally the number of children surviving their first five years.
    """
    # Drop out years prior to 1800 and regions that aren't countries

    tb_world = tb_combined[(tb_combined["country"] == "World")].drop(columns=["source"])

    # Add global labels and calculate the share of children surviving/dying in their first five years

    tb_world["share_dying_first_five_years"] = tb_world["child_mortality_rate"]
    tb_world["share_surviving_first_five_years"] = 100 - (tb_world["child_mortality_rate"])

    tb_world = tb_world.drop(columns=["child_mortality_rate"])

    return tb_world
