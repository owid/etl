"""Load a meadow dataset and create a garden dataset."""

import numpy as np

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("census_us_idb")

    # Read table from meadow dataset.
    tb = ds_meadow.read("census_us_idb")

    #
    # Process data.
    #

    # Harmonize country names.
    tb = paths.regions.harmonize_names(tb=tb)

    # Keep relevant columns
    # https://api.census.gov/data/timeseries/idb/5year/variables.html
    cols_index = ["country", "year"]
    cols_indicators = [
        "pop",
        "tfr",
        "e0",
        "nim",
    ]
    # Age-specific columns needed for computing regional TFR from scratch
    AGE_GROUPS = [f"{i}_{i+4}" for i in range(15, 50, 5)]
    cols_births = [f"births{ag}" for ag in AGE_GROUPS]
    cols_fpop = [f"fpop{ag}" for ag in AGE_GROUPS]
    cols_asfr = cols_births + cols_fpop

    cols = cols_indicators + cols_asfr + cols_index
    tb = tb[cols].dropna(subset=cols_indicators, how="all")

    # Add region aggregates for World and continents
    REGIONS = [
        "World",
        "Africa",
        "North America",
        "South America",
        "Asia",
        "Europe",
        "Oceania",
    ]
    aggregations = {
        "pop": "sum",
        "nim": "sum",
        **{col: "sum" for col in cols_asfr},
    }
    tb = paths.regions.add_aggregates(
        tb=tb,
        aggregations=aggregations,
        regions=REGIONS,
        min_frac_countries_informed=0.7,
        countries_that_must_have_data={
            "World": ["China", "India", "Indonesia", "United States"],
            "Asia": ["China", "India", "Indonesia"],
            "North America": ["United States", "Mexico", "Canada"],
        },
    )
    tb.loc[tb["country"] == "World", "nim"] = np.nan

    # Compute TFR for regions from age-specific births and female population
    is_region = tb["country"].isin(REGIONS)
    asfr_sum = sum(tb[f"births{ag}"] / tb[f"fpop{ag}"] for ag in AGE_GROUPS)
    tb.loc[is_region, "tfr"] = (5 * asfr_sum).loc[is_region]

    # Drop intermediate age-specific columns
    tb = tb.drop(columns=cols_asfr)

    #
    # Save outputs.
    #
    cols_final = cols_indicators + cols_index
    # Format table
    tb = tb[cols_final].format(["country", "year"])
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()
