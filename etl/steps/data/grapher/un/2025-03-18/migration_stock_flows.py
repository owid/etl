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
    tb = ds_garden.read("migration_stock_flows")
    regions = ds_regions.read("regions")

    #
    # Processing
    #
    # Keep only relevant countries
    countries = regions[regions["region_type"] == "country"]["name"].unique()
    tb = tb.loc[tb["country_origin_or_dest"].isin(countries)]

    # Add rows for country and country select being equal
    tb = add_same_country_rows(tb)

    # Format
    tb = tb.format(["country", "country_origin_or_dest", "year", "gender"])

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
    tb_same = tb[["country_origin_or_dest", "year", "gender"]].drop_duplicates()
    tb_same.loc[:, ["emigrants", "immigrants"]] = "Selected country"
    tb_same["country"] = tb_same["country_origin_or_dest"]

    # Add to main table
    tb = pr.concat([tb, tb_same])

    # Ensure typing
    cols = ["emigrants", "immigrants"]
    tb[cols] = tb[cols].astype("string")
    return tb
