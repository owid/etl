"""Load a meadow dataset and create a garden dataset.

This dataset captures the number of years it takes for a region to double its population size.

To do so, we use our OMM population indicator. We linearly interpolate its values, so that we have a complete time-series of population.

NOTE 1: For now, this step and its Grapher counterpart are only capturing this data for the World and ignore the remaining regions.
NOTE 2: In the future, we might want to have other countries and regions in this dataset. In that scenario, please review all the code below and Grapher's.

"""

from typing import cast

import numpy as np
import pandas as pd
from owid.catalog import Table, Variable

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Population values for which we want to track doubling times
## If '0.5' appears, it means we are interested in the number of years it took to go from 0.25 -> 0.5
POPULATION_TARGETS = [0.25, 0.5, 1, 1.5, 2, 2.5, 3, 4, 5, 8, 10]
POPULATION_TARGETS = [x * 1e9 for x in POPULATION_TARGETS]


def run(dest_dir: str) -> None:
    paths.log.info("start")
    #
    # Load inputs.
    #
    # Load meadow dataset.
    paths.log.info("load data")
    ds_meadow = paths.load_dataset("population")

    # Read table from meadow dataset.
    tb = ds_meadow["population_original"].reset_index()

    #
    # Process data.
    #
    # Keep only World data, reset index
    tb = tb.loc[tb["country"] == "World", ["year", "population"]]

    # Linearly interpolate population
    tb = interpolate_population(tb)

    # Estimate number of years passed since population was half
    # tb["years_since_half_population"] = get_years_since_half_population(tb)

    # Round population values to significant (resolution of 1e8), e.g. 521,324,321 -> 5e8
    tb = round_population_values(tb)

    # Keep only population of interest values
    tb = tb.loc[tb["population_target"].isin(POPULATION_TARGETS)]

    # Keep one row for each population rounded. That's when the 'target' is reached.
    tb = get_target_years(tb)

    # Estimate doubling time (in years)
    tb["previous_population_target"] = tb["population_target"] // 2
    tb = tb.merge(tb, left_on="previous_population_target", right_on="population_target", suffixes=("", "_previous"))
    tb["num_years_to_double"] = tb["year"] - tb["year_previous"]
    tb = tb.loc[:, ["year", "population_target", "num_years_to_double"]]

    # Add country
    paths.log.info("Add entity 'World'")
    tb["country"] = "World"

    # Set index
    tb = tb.format(["country", "year"], short_name="population_doubling_times")

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()


def interpolate_population(tb: Table) -> Table:
    """Interpolate population values for missing years.

    tb should be of Nx2 dimensions, 2 being the number of culumns: year, population.
    """
    paths.log.info("interpolate values (missing years)")
    # Create new index ([start_year, end_year])
    idx = pd.RangeIndex(tb.year.min(), tb.year.max() + 1, name="year")
    tb = tb.set_index(["year"]).reindex(idx)
    # Interpolate population values
    tb["population"] = tb["population"].interpolate(method="index")
    tb = tb.reset_index()
    return tb


def get_years_since_half_population(tb: Table) -> Variable:
    """Get the number of years since population was half."""

    def _get_year_half_population(population: int, tb: Table):
        idx = np.argmin(np.abs(tb.population.values - population / 2))
        return tb.year.values[idx]

    paths.log.info("estimate years since half population")
    year_with_half_population = tb["population"].apply(lambda x: _get_year_half_population(x, tb))
    year_with_half_population = cast(Variable, year_with_half_population)
    return tb.loc[:, "year"] - year_with_half_population


def round_population_values(tb: Table) -> Table:
    """Round population values to significant figures."""
    msk = tb["population"] <= 1e9
    tb.loc[msk, "population_target"] = tb.loc[msk, "population"].round(-7)
    tb.loc[-msk, "population_target"] = tb.loc[-msk, "population"].round(-8)
    return tb


def get_target_years(tb: Table) -> Table:
    """Get years of interest.

    Multiple population values are rounded to the same value. We want to keep only the row for the year when the population target was reached.
    How? We obtain rows where target population was reached (e.g target-crossing)
    """
    paths.log.info("Get target years")

    ## 1. Calculate the sign of the population error
    tb["population_error"] = tb["population"] - tb["population_target"]
    ## 2. Check if the sign of the population error changes (from negative to positive)
    ## Keep start and end year of target-crossing
    ## Tag target-crossing with a number (so that we know that the start- and end-years belong to the same target-crossing)
    tb["target_crossing"] = np.sign(tb["population_error"]).diff().fillna(0) > 0
    tb["target_crossing"] = np.where(tb["target_crossing"], tb["target_crossing"].cumsum(), 0)
    tb["target_crossing"] = tb["target_crossing"] + tb["target_crossing"].shift(-1).fillna(0)

    ## 2b. Sometimes there is no 'crossing' due to resolution
    x = tb["population_target"].value_counts()
    target_no_crossing = x[x == 1].index
    assert len(target_no_crossing) == 1, f"Population targets with no crossing: {target_no_crossing}"
    mask = tb["population_target"].isin(target_no_crossing)
    tb.loc[mask, "target_crossing"] = tb.loc[mask].index

    tb = tb[tb["target_crossing"] > 0]

    ## 3. Keep start OR end year of target-crossing, based on which is closed to target
    tb["population_error_abs"] = tb["population_error"].abs()
    tb = tb.sort_values("population_error_abs").drop_duplicates(subset=["target_crossing"])

    ## 4. Keep relevant columns
    tb = tb.loc[:, ["year", "population", "population_target"]]

    return tb
