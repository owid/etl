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

    # remove regions as "country_select" dimension and sort countries
    tb = tb[tb["country_select"].isin(countries)]
    tb = tb.sort_values(by=["country_select"], ascending=True)

    # Add rows for country and country select being equal
    tb = add_same_country_rows(tb)

    # convert column to string and set unit
    tb["migrants"] = tb["migrants"].astype(str).copy_metadata(tb["migrants"])

    tb = tb.format(["country", "country_select", "metric", "gender", "year"])

    #
    # Save outputs.
    #
    # Initialize a new grapher dataset.
    ds_grapher = paths.create_dataset(tables=[tb], default_metadata=ds_garden.metadata)

    # Save grapher dataset.
    ds_grapher.save()


def add_same_country_rows(tb):
    """Create rows for countries where country and country select are the same and set migrants to 0."""
    # Get dataframe with same-country rows
    tb_same = tb[["country_select", "year", "gender", "metric"]].drop_duplicates()
    tb_same.loc[:, ["migrants"]] = "Selected country"
    tb_same["country"] = tb_same["country_select"]

    # Add to main table
    tb = pr.concat([tb, tb_same])

    # Ensure typing
    tb["migrants"] = tb["migrants"].astype("string")
    return tb
