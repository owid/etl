"""Load a meadow dataset and create a garden dataset."""

from typing import cast

from owid.catalog import Dataset

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = cast(Dataset, paths.load_dependency("population"))

    # Read table from meadow dataset.
    tb = ds_meadow["population"]
    tb.metadata.short_name = "population_doubling_times"

    #
    # Process data.
    #
    # Keep only World data, reset index
    tb = tb.reset_index()
    tb = tb[tb["country"] == "World"][["year", "population"]]

    # Estimate number of years passed since population was half
    year_with_half_population = tb["population"].apply(lambda x: tb.loc[tb.population <= x / 2, "year"].max())
    year_with_half_population.metadata.unit = "years"
    tb["year"].metadata.unit = "years"
    tb["years_since_half_population"] = tb["year"] - year_with_half_population

    # Population transitions of interest
    # If '0.5' appears, it means we are interested in the number of years it took to go from 0.25 -> 0.5
    population_of_interest = [0.5, 1, 2, 3, 4, 5, 8, 10, 10.4]
    population_of_interest = [x * 1e9 for x in population_of_interest]

    # Keep rows with more than lowest "population of interest"
    tb = tb[tb["population"] >= population_of_interest[0]]
    # Round population values to significant (resolution of 1e8), e.g. 521,324,321 -> 5e8
    tb["population_rounded"] = tb["population"].round(-8)
    # Keep only one row for each population rounded
    # There are multiple rows mapped to the same "population_rounded", but we are only interested in the one that has the lowest original value
    tb = tb.drop_duplicates(subset="population_rounded", keep="first")

    # Keep only population of interest values
    tb = tb[tb["population_rounded"].isin(population_of_interest)]

    # Add country
    tb["country"] = "World"

    # Set index
    tb = tb.set_index(["country", "year"]).sort_index()

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()
