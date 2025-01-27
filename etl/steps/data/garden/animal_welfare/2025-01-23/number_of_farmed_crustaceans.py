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
    "numbers__lower__in_millions": "n_farmed_crustaceans_low",
    "numbers__midpoint__in_millions": "n_farmed_crustaceans",
    "numbers__upper__in_millions": "n_farmed_crustaceans_high",
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


def add_per_capita_variables(tb: Table, ds_population: Dataset) -> Table:
    tb = geo.add_population_to_table(tb, ds_population=ds_population, warn_on_missing_countries=False)
    tb["n_farmed_crustaceans_low_per_capita"] = tb["n_farmed_crustaceans_low"] / tb["population"]
    tb["n_farmed_crustaceans_per_capita"] = tb["n_farmed_crustaceans"] / tb["population"]
    tb["n_farmed_crustaceans_high_per_capita"] = tb["n_farmed_crustaceans_high"] / tb["population"]
    # Drop population column.
    tb = tb.drop(columns=["population"], errors="raise")

    return tb


def sanity_check_outputs(tb: Table) -> None:
    # Check that the total agrees (within a few percent) with the sum of aggregates from each continent, for non per capita columns.
    tb = tb[[column for column in tb.columns if "per_capita" not in column]].copy()
    world = tb[tb["country"] == "World"].reset_index(drop=True).drop(columns=["country"])
    test = (
        tb[tb["country"].isin(["Africa", "North America", "South America", "Asia", "Europe", "Oceania"])]
        .groupby("year", as_index=False)
        .sum(numeric_only=True)
    )
    assert (100 * abs(world - test) / world < 2).all().all()


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its main table.
    ds_meadow = paths.load_dataset("number_of_farmed_crustaceans")
    tb = ds_meadow.read("number_of_farmed_crustaceans")

    # Load regions dataset.
    ds_regions = paths.load_dataset("regions")

    # Load income groups dataset.
    ds_income_groups = paths.load_dataset("income_groups")

    # Load population dataset.
    ds_population = paths.load_dataset("population")

    #
    # Process data.
    #
    # Select and rename columns.
    tb = tb[list(COLUMNS)].rename(columns=COLUMNS, errors="raise")

    # Harmonize country names.
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # There are cases of "<1" in the data. Replace those with the average value in that range, namely 0.5.
    # Convert to float and change from million to units.
    for column in ["n_farmed_crustaceans_low", "n_farmed_crustaceans", "n_farmed_crustaceans_high"]:
        tb[column] = tb[column].replace("< 1", "0.5").astype("Float64") * 1e6

    # Add region aggregates.
    tb = geo.add_regions_to_table(
        tb=tb,
        ds_regions=ds_regions,
        ds_income_groups=ds_income_groups,
        regions=REGIONS_TO_ADD,
        min_num_values_per_year=1,
    )

    # Add per capita number of farmed decapod crustaceans.
    tb = add_per_capita_variables(tb=tb, ds_population=ds_population)

    # Run sanity checks on outputs.
    sanity_check_outputs(tb=tb)

    # Set an appropriate index and sort conveniently.
    tb = tb.format(sort_columns=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_garden.save()
