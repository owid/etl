"""Load a garden dataset and create a grapher dataset."""

from owid.catalog import processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# country_select is the country to be selected in the multidim
# country is the country/ region which will be shown on the map


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
    countries = regions[regions["region_type"] == "country"]["name"].unique()
    #
    # Create table where indicator is split into immigrants and emigrants instead of having country_origin and country_destination
    #
    # Copying the table to include both directions of flows between countries
    tb_switched = tb.copy(deep=True)

    # Rename columns, so that "country select" is country to be selected in mdim
    # and "country" is the country of origin, which will be shown on the map
    tb = tb.rename(columns={"country_destination": "country_select", "country_origin": "country"})
    tb["metric"] = "immigrants"

    # the other way around (country is now country of destination) for the second table
    tb_switched = tb_switched.rename(columns={"country_destination": "country", "country_origin": "country_select"})
    tb_switched["metric"] = "emigrants"

    # combine tables
    tb = pr.concat([tb, tb_switched])

    # remove regions as from mdim dropdown and sort values
    tb = tb[tb["country_select"].isin(countries)]
    tb = tb.sort_values(by=["country_select"], ascending=True)

    # No row is added for country == country_select: that flow is inapplicable by construction
    # (e.g. "Afghans who emigrated to Afghanistan"), and the chart/explorer steps mark the
    # country_select entity as inapplicable on the map rather than faking a data point for it.

    tb = tb.format(["country", "country_select", "metric", "gender", "year"])

    # Initialize a new grapher dataset.
    ds_grapher = paths.create_dataset(tables=[tb], default_metadata=ds_garden.metadata)

    # Save grapher dataset.
    ds_grapher.save()
