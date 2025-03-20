"""Load a garden dataset and create a grapher dataset."""

import pandas as pd
from owid.catalog import Table
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

    # remove regions as "country_select" dimension
    tb = tb[tb["country_select"].isin(countries)]

    # Add rows for country and country select being equal
    tb_same_country = create_same_country_rows(tb["country_select"].unique())

    # include rows for countries where country and country select are the same
    tb = pr.concat([tb, tb_same_country])  # type: ignore

    # convert all columns to string
    for col in tb.columns:
        if col not in ["year"]:
            tb[col] = tb[col].astype(str)

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


def create_same_country_rows(countries):
    """Create rows for countries where country and country select are the same and set migrants to 0."""
    rows = []
    for country in countries:
        for year in [1990, 1995, 2000, 2005, 2010, 2015, 2020, 2024]:
            for metric in ["immigrants", "emigrants"]:
                row = {
                    "country_select": country,
                    "country": country,
                    "year": year,
                    "migrants_all_sexes": "Selected country",
                    "migrants_female": "Selected country",
                    "migrants_male": "Selected country",
                    "metric": metric,
                }
                rows.append(row)
    return Table(pd.DataFrame(rows))
