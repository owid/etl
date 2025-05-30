"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("famines_by_regime_gdp_population")

    # Read table from garden dataset.
    tb = ds_garden.read("famines", reset_index=True)

    #
    # Process data.
    #

    # Drop rows where the Chinese famine is broken down by year (only China 1958-1962 should exist)
    famine_names_to_drop = ["China 1958", "China 1959", "China 1960", "China 1961", "China 1962"]
    tb = tb[~tb["famine_name"].isin(famine_names_to_drop)]

    tb = tb.rename({"famine_name": "country", "country": "country_name"}, axis=1)

    # Keep the first year for each famine (country)
    tb = tb.sort_values(by="year").drop_duplicates(subset="country", keep="first")
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=ds_garden.metadata)

    # Save changes in the new grapher dataset.
    ds_grapher.save()
