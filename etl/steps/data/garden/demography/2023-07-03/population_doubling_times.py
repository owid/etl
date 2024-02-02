"""Load a meadow dataset and create a garden dataset.

This dataset captures the number of years it takes for a region to double its population size.

To do so, we use our OMM population indicator. We linearly interpolate its values, so that we have a complete time-series of population.

NOTE 1: For now, this step and its Grapher counterpart are only capturing this data for the World and ignore the remaining regions.
NOTE 2: In the future, we might want to have other countries and regions in this dataset. In that scenario, please review all the code below and Grapher's.

"""

import numpy as np
import pandas as pd
from owid.catalog import Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
log = get_logger()


def run(dest_dir: str) -> None:
    log.info("population_doubling_times: start")
    #
    # Load inputs.
    #
    # Load meadow dataset.
    log.info("population_doubling_times: load data")
    ds_meadow = paths.load_dataset("population")

    # Read table from meadow dataset.
    tb = ds_meadow["population_original"]
    tb.metadata.short_name = "population_doubling_times"

    #
    # Process data.
    #
    # Keep only World data, reset index
    tb = tb.reset_index()
    tb = tb[tb["country"] == "World"][["year", "population"]]

    # Linearly interpolate population
    log.info("population_doubling_times: interpolate values (missing years)")
    # Create new index ([start_year, end_year])
    idx = pd.RangeIndex(tb.year.min(), tb.year.max() + 1, name="year")
    tb = tb.set_index(["year"]).reindex(idx)
    # Interpolate population values
    tb["population"] = tb["population"].interpolate(method="index")
    tb = tb.reset_index()

    # Estimate number of years passed since population was half
    def _get_year_half_population(population: int, tb: Table):
        idx = np.argmin(np.abs(tb.population.values - population / 2))
        return tb.year.values[idx]

    log.info("population_doubling_times: estimate years since half population")
    year_with_half_population = tb["population"].apply(lambda x: _get_year_half_population(x, tb))
    year_with_half_population.metadata.unit = "years"
    tb["year"].metadata.unit = "years"
    tb["years_since_half_population"] = tb["year"] - year_with_half_population

    # Population transitions of interest
    # If '0.5' appears, it means we are interested in the number of years it took to go from 0.25 -> 0.5
    population_of_interest = [0.5, 1, 2, 3, 4, 5, 8, 10, 10.4]
    population_of_interest = [x * 1e9 for x in population_of_interest]

    # Keep rows with more than lowest "population of interest"
    log.info("population_doubling_times: keep relevant population size transitions")
    tb = tb[tb["population"] >= population_of_interest[0]]
    # Round population values to significant (resolution of 1e8), e.g. 521,324,321 -> 5e8
    msk = tb["population"] <= 1e9
    tb.loc[msk, "population_rounded"] = tb.loc[msk, "population"].round(-7)
    tb.loc[-msk, "population_rounded"] = tb.loc[-msk, "population"].round(-8)
    # Keep only one row for each population rounded
    # There are multiple rows mapped to the same "population_rounded", but we are only interested in the one that is closest to the rounded value (lowest error)
    tb["population_error"] = (tb["population"] - tb["population_rounded"]).abs()
    tb = tb[tb["population_error"] == tb.groupby("population_rounded")["population_error"].transform("min")]
    tb = tb.drop(columns=["population_error"])

    # Keep only population of interest values
    tb = tb[tb["population_rounded"].isin(population_of_interest)]

    # Add country
    log.info("population_doubling_times: final touches")
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
    log.info("population_doubling_times: end")
