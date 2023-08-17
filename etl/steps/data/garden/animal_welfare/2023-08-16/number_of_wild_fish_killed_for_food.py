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
    "estimated_numbers__lower__millions": "n_wild_fish_low",
    "estimated_numbers__upper__millions": "n_wild_fish_high",
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
]


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


def run_sanity_checks_on_outputs(tb: Table) -> None:
    # Check that the total agrees with the sum of aggregates from each continent.
    world = tb[tb["country"] == "World"].reset_index(drop=True).drop(columns=["country"])
    test = (
        tb[tb["country"].isin(["Africa", "North America", "South America", "Asia", "Europe", "Oceania"])]
        .groupby("year", as_index=False)
        .sum(numeric_only=True)
    )
    assert (abs(world - test) / world < 1e-2).all().all()


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its main table.
    ds_meadow: Dataset = paths.load_dependency("number_of_wild_fish_killed_for_food")
    tb = ds_meadow["number_of_wild_fish_killed_for_food"].reset_index()

    # Load regions dataset.
    ds_regions = paths.load_dependency("regions")

    # Load income groups dataset.
    ds_income_groups = paths.load_dependency("income_groups")

    #
    # Process data.
    #
    # Select and rename columns.
    tb = tb[list(COLUMNS)].rename(columns=COLUMNS, errors="raise")

    # Harmonize country names.
    tb: Table = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Add number of fish for each country and year.
    tb = tb.groupby(["country", "year"], as_index=False).sum()

    # Add region aggregates.
    tb = add_region_aggregates(tb, ds_regions=ds_regions, ds_income_groups=ds_income_groups).copy_metadata(tb)

    # Adapt units.
    tb["n_wild_fish_low"] *= 1e6
    tb["n_wild_fish_high"] *= 1e6

    # Add midpoint number of wild fish.
    tb["n_wild_fish"] = (tb["n_wild_fish_low"] + tb["n_wild_fish_high"]) / 2

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
