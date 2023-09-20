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
    ds_igme = paths.load_dataset("igme")
    ds_gapminder = paths.load_dataset("under_five_mortality")
    # Read table from meadow dataset.
    tb_igme = ds_igme["igme"].reset_index()

    # Select out columns of interest.
    columns = {
        "country": "country",
        "year": "year",
        "observation_value_deaths_per_1_000_live_births_under_five_mortality_rate_both_sexes_all_wealth_quintiles": "under_five_mortality",
    }
    tb_igme = tb_igme[list(columns)].rename(columns=columns, errors="raise")

    tb_igme["source"] = "igme"
    # Load full Gapminder data
    tb_gap = ds_gapminder["under_five_mortality"].reset_index()
    tb_gap["source"] = "gapminder"
    #
    tb_gap_sel = ds_gapminder["under_five_mortality_selected"].reset_index()
    tb_gap_sel["source"] = "gapminder"

    # Combine IGME and Gapminder data
    tb_combined = combine_datasets(tb_igme, tb_gap, "long_run_child_mortality")
    tb_combined_sel = combine_datasets(tb_igme, tb_gap_sel, "long_run_child_mortality_selected")

    tb_surviving = calculate_share_surviving_first_five_years(tb_combined)
    #
    # Save outputs.
    tb_combined = tb_combined.drop(columns=["source"]).set_index(["country", "year"], verify_integrity=True)
    tb_combined_sel = tb_combined_sel.drop(columns=["source"]).set_index(["country", "year"], verify_integrity=True)
    tb_surviving = tb_surviving.set_index(["country", "year"], verify_integrity=True)

    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb_combined, tb_combined_sel, tb_surviving], check_variables_metadata=True
    )
    # Save changes in the new garden dataset.
    ds_garden.save()


def combine_datasets(tb_igme: Table, tb_gap: Table, table_name: str) -> Table:
    """
    Combine IGME and Gapminder data.
    """
    tb_combined = pr.concat([tb_igme, tb_gap]).sort_values(["country", "year", "source"])
    tb_combined.metadata.short_name = table_name
    tb_combined = remove_duplicates(tb_combined, preferred_source="igme")

    return tb_combined


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
    # Load population and countries-regions data
    population = geo._load_population()
    countries_regions = geo._load_countries_regions()
    countries = countries_regions[countries_regions["region_type"] == "country"]["name"].tolist()

    # Calculate the population weights of each country: world_pop_share/100
    population["population_weight"] = population["world_pop_share"] / 100

    # Drop out years prior to 1800 and regions that aren't countries
    tb_combined = tb_combined[(tb_combined["year"] >= 1800) & (tb_combined["country"].isin(countries))]

    # Combine child mortality and population tables
    tb_pop = pr.merge(tb_combined, population, on=["country", "year"], how="left")
    # Calculate the weighted under five mortality rate
    tb_pop["weighted_u5mr"] = tb_pop["under_five_mortality"] * tb_pop["population_weight"]
    tb_global = tb_pop.groupby(["year"]).agg({"weighted_u5mr": lambda x: x.sum(skipna=True)}).reset_index()
    # Add metadata
    tb_global.metadata.short_name = "share_surviving_first_five_years"

    # Add global labels and calculate the share of children surviving/dying in their first five years
    tb_global["country"] = "World"
    tb_global["share_dying_first_five_years"] = tb_global["weighted_u5mr"] / 10
    tb_global["share_surviving_first_five_years"] = 100 - (tb_global["weighted_u5mr"] / 10)

    return tb_global
