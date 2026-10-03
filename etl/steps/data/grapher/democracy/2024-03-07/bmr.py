"""Load a garden dataset and create a grapher dataset."""

import numpy as np

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("bmr")

    # Read table from garden dataset.
    tb = ds_garden["bmr"]
    tb_num_countries = ds_garden["num_countries_regime"]
    tb_num_countries_years = ds_garden["num_countries_regime_years"]
    tb_population = ds_garden["population_regime"]
    tb_population_years = ds_garden["population_regime_years"]

    #
    # Process data.
    #

    # Main table
    ## Special indicator values renamings
    cols = [
        # "num_years_in_democracy",
        # "num_years_in_democracy_ws",
        "num_years_in_democracy_consecutive",
        "num_years_in_democracy_ws_consecutive",
    ]
    for col in cols:
        tb[col] = tb[col].astype("string").replace({"0": "non-democracy"})

    ## Drop indicators (only useful in Garden)
    columns_drop = [
        "regime_imputed_country",
        "regime_imputed",
        "num_years_in_democracy_consecutive_group",
        "num_years_in_democracy_ws_consecutive_group",
    ]
    tb = tb.drop(columns=columns_drop)

    # Region table
    ## Set to NaN so that indicator is not imported to Grapher
    ## WHY? These indicators count the number of countries (and people) living in countries for which we do not have regime (with WS) data (i.e. can't tell if these countries are democracies with women's suffrage or not). These indicators are equivalent to the counts of countries (and people) living in countries with no regime data (i.e. without necessarily having WS). Hence, this is to avoid having duplicate indicators.
    mask = (slice(None), slice(None), -1)
    tb_num_countries.loc[mask, "num_countries_regime_ws"] = np.nan
    tb_population.loc[mask, "population_regime_ws"] = np.nan

    #
    # Save outputs.
    #
    tables = [
        tb,
        tb_num_countries,
        tb_num_countries_years,
        tb_population,
        tb_population_years,
    ]
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir, tables=tables, check_variables_metadata=True, default_metadata=ds_garden.metadata
    )

    # Save changes in the new grapher dataset.
    ds_grapher.save()
