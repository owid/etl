"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table

from etl.helpers import PathFinder

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


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its main table.
    ds_meadow = paths.load_dataset("number_of_farmed_crustaceans")
    tb = ds_meadow.read("number_of_farmed_crustaceans")

    #
    # Process data.
    #
    # Select and rename columns.
    tb = tb[list(COLUMNS)].rename(columns=COLUMNS, errors="raise")

    # Harmonize country names.
    tb = paths.regions.harmonize_names(tb=tb)

    # There are cases of "<1" in the data. Replace those with the average value in that range, namely 0.5.
    # Convert to float and change from million to units.
    for column in ["n_farmed_crustaceans_low", "n_farmed_crustaceans", "n_farmed_crustaceans_high"]:
        tb[column] = tb[column].replace("< 1", "0.5").astype("Float64") * 1e6

    # Add region aggregates.
    tb = paths.regions.add_aggregates(tb=tb, regions=REGIONS_TO_ADD, min_num_values_per_year=1)

    # Add per capita number of farmed decapod crustaceans.
    tb = paths.regions.add_per_capita(tb=tb, warn_on_missing_countries=False)

    # Run sanity checks on outputs.
    sanity_check_outputs(tb=tb)

    # Set an appropriate index and sort conveniently.
    tb = tb.format(sort_columns=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], check_variables_metadata=True)
    ds_garden.save()
