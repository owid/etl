"""Load a meadow dataset and create a garden dataset."""

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

    # Keep relevant countries
    # https://api.census.gov/data/timeseries/idb/5year/variables.html
    cols_index = ["country", "year"]
    cols_indicators = [
        "pop",
        "tfr",
        "e0",
        "nim",
    ]
    cols = cols_indicators + cols_index
    tb = tb[cols].dropna(subset=cols_indicators, how="all")

    # Add region aggregates for World and continents
    aggregations = {
        "pop": "sum",
        "nim": "sum",
    }
    tb = paths.regions.add_aggregates(
        tb=tb,
        aggregations=aggregations,
        min_frac_countries_informed=0.7,
        countries_that_must_have_data={"World": ["China", "India", "Indonesia", "United States"]},
    )

    #
    # Save outputs.
    #
    # Format table
    tb = tb[cols].format(["country", "year"])
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()
