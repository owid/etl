"""Load a garden dataset and create a grapher dataset."""

from owid.catalog import processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("migration_stock_flows")
    ds_regions = paths.load_dataset("regions")

    # Read table from garden dataset.
    tb = ds_garden.read("migrant_stock_dest_origin")
    regions = ds_regions.read("regions")

    # Copy table
    tb_switched = tb.copy(deep=True)

    # rename columns, so that "country select" is country to be selected in mdim
    # and "country" is the country of origin, which will be shown on the map
    tb = tb.rename(columns={"country_destination": "country_select", "country_origin": "country"})
    tb["metric"] = "immigrants"

    # the other way around for the second table
    tb_switched = tb_switched.rename(columns={"country_destination": "country", "country_origin": "country_select"})
    tb_switched["metric"] = "emigrants"

    # combine tables
    tb = pr.concat([tb, tb_switched])

    # remove regions as "country_select" dimension
    countries = regions[regions["region_type"] == "country"]["name"].unique()
    tb = tb[tb["country_select"].isin(countries)]

    # set migrants_all_sexes to 0 if country and country_select are the same
    tb.loc[tb["country"] == tb["country_select"], "migrants_all_sexes"] = 0

    # drop female and male migrants as not to clutter the grapher
    tb = tb.drop(columns=["migrants_female", "migrants_male"])

    tb = tb.format(["country", "country_select", "metric", "year"])

    #
    # Save outputs.
    #
    # Initialize a new grapher dataset.
    ds_grapher = paths.create_dataset(tables=[tb], default_metadata=ds_garden.metadata)

    # Save grapher dataset.
    ds_grapher.save()
