"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Dataset, Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Column names to select and how to rename them.
COLUMNS = {
    "country": "country",
    "year": "year",
    "estimated_numbers__millions__lower": "n_farmed_fish_low",
    "estimated_numbers__millions__upper": "n_farmed_fish_high",
}

# Regions to create aggregates for.
REGIONS_TO_ADD = [
    "North America",
    "South America",
    "Europe",
    "European Union (27)",
    "Africa",
    "Asia",
    "Oceania",
    "Low-income countries",
    "Upper-middle-income countries",
    "Lower-middle-income countries",
    "High-income countries",
    "World",
]


def run_sanity_checks_on_inputs(tb: Table) -> None:
    for year in [2015, 2016, 2017]:
        # Calculate the lower and upper bounds of the number of fish with or without an EMW.
        lower = tb[(tb["country"] != "Totals") & (tb["year"] == year)]["estimated_numbers__millions__lower"].sum()
        upper = tb[(tb["country"] != "Totals") & (tb["year"] == year)]["estimated_numbers__millions__upper"].sum()

        # Calculate the lower and upper bounds of the number of fish for which there is an EMW.
        lower_emw = tb[(tb["country"] != "Totals") & (tb["year"] == year) & (tb["estimated_mean_weight__lower"] > 0)][
            "estimated_numbers__millions__lower"
        ].sum()
        upper_emw = tb[(tb["country"] != "Totals") & (tb["year"] == year) & (tb["estimated_mean_weight__upper"] > 0)][
            "estimated_numbers__millions__upper"
        ].sum()

        # Check that the lower and upper bounds of "Totals" is equal to the sum of rows with or without EMW.
        assert round(lower, -1) == round(
            tb[(tb["country"] == "Totals") & (tb["year"] == year)]["estimated_numbers__millions__lower"].item(), -1
        )
        assert round(upper, -1) == round(
            tb[(tb["country"] == "Totals") & (tb["year"] == year)]["estimated_numbers__millions__upper"].item(), -1
        )

        if year == 2015:
            # The estimated number of farmed fish for 2015 includes species with and without an EMW.
            # Check that the number of fish with EMW is smaller by around 20%.
            assert lower_emw * 1.2 < lower
            assert upper_emw * 1.2 < upper
        elif year in [2016, 2017]:
            # The estimated number of farmed fish for 2016 and 2017 includes only species with an EMW.
            assert round(lower_emw, -1) == round(lower, -1)
            assert round(upper_emw, -1) == round(upper, -1)


def add_region_aggregates(tb: Table, ds_regions: Dataset, ds_income_groups: Dataset) -> Table:
    tb = tb.copy()
    for region in REGIONS_TO_ADD:
        # List of countries in region.
        countries_in_region = geo.list_members_of_region(
            region=region,
            ds_regions=ds_regions,
            ds_income_groups=ds_income_groups,
        )

        # Add region aggregates.
        tb = geo.add_region_aggregates(
            df=tb,
            region=region,
            countries_in_region=countries_in_region,
            countries_that_must_have_data=[],
            # Here we allow aggregating even when there are few countries informed.
            # However, if absolutely all countries have nan, we want the aggregate to be nan, not zero.
            frac_allowed_nans_per_year=0.999,
            num_allowed_nans_per_year=None,
        )
    return tb


def add_per_capita_variables(tb: Table, ds_population: Dataset) -> Table:
    tb = geo.add_population_to_table(tb, ds_population=ds_population, warn_on_missing_countries=False)
    tb["n_farmed_fish_low_per_capita"] = tb["n_farmed_fish_low"] / tb["population"]
    tb["n_farmed_fish_per_capita"] = tb["n_farmed_fish"] / tb["population"]
    tb["n_farmed_fish_high_per_capita"] = tb["n_farmed_fish_high"] / tb["population"]
    # Drop population column.
    tb = tb.drop(columns=["population"])
    return tb


def run_sanity_checks_on_outputs(tb: Table) -> None:
    # Check that the total agrees with the sum of aggregates from each continent, for non per capita columns.
    tb = tb[[column for column in tb.columns if "per_capita" not in column]].copy()
    world = tb[tb["country"] == "World"].reset_index(drop=True).drop(columns=["country"])
    test = (
        tb[tb["country"].isin(["Africa", "North America", "South America", "Asia", "Europe", "Oceania"])]
        .groupby("year", as_index=False)
        .sum(numeric_only=True)
    )
    assert (abs(world - test) / world < 1e-5).all().all()


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its main table.
    ds_meadow: Dataset = paths.load_dependency("number_of_farmed_fish")
    tb = ds_meadow["number_of_farmed_fish"].reset_index()

    # Load regions dataset.
    ds_regions = paths.load_dependency("regions")

    # Load income groups dataset.
    ds_income_groups = paths.load_dependency("income_groups")

    # Load population dataset.
    ds_population = paths.load_dependency("population")

    #
    # Process data.
    #
    # Run sanity checks on inputs.
    run_sanity_checks_on_inputs(tb=tb)

    # Harmonize country names.
    tb: Table = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # The number of fish for 2015 includes species with and without an EMW, however 2016 and 2017 includes only fish
    # with an EMW. This means that 2015 includes a 17% of additional production. For consistency, include in 2015 only
    # species with an EMW.
    tb = tb[tb["estimated_mean_weight__lower"] > 0].reset_index(drop=True)

    # Select and rename columns.
    tb = tb[list(COLUMNS)].rename(columns=COLUMNS, errors="raise")

    # Add number of fish for each country and year.
    tb = tb.groupby(["country", "year"], as_index=False, observed=True).sum(min_count=1)

    # Add region aggregates.
    tb = add_region_aggregates(tb, ds_regions=ds_regions, ds_income_groups=ds_income_groups).copy_metadata(tb)

    # Adapt units.
    tb["n_farmed_fish_low"] *= 1e6
    tb["n_farmed_fish_high"] *= 1e6

    # Add midpoint number of farmed fish.
    tb["n_farmed_fish"] = (tb["n_farmed_fish_low"] + tb["n_farmed_fish_high"]) / 2

    # Add per capita number of farmed fish.
    tb = add_per_capita_variables(tb=tb, ds_population=ds_population)

    # Run sanity checks on outputs.
    run_sanity_checks_on_outputs(tb=tb)

    # Set an appropriate index and sort conveniently.
    tb = tb.set_index(["country", "year"], verify_integrity=True).sort_index().sort_index(axis=1)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_garden.save()
