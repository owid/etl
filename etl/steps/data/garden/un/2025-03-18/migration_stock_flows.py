"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import processing as pr

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

REGIONS = list(geo.REGIONS.keys()) + ["World"]


def add_others_to_world(tb, country_col):
    """Add "Others" region to "World" totals, as it isn't included automatically when summing over countries."""
    # add "World" as a region by summing over all countries
    tb_w_o = tb[tb[country_col].isin(["World", "Others"])].copy()

    if country_col == "country_destination":
        tb_world = tb_w_o.groupby(["year", "country_origin"]).sum().reset_index()
    if country_col == "country_origin":
        tb_world = tb_w_o.groupby(["year", "country_destination"]).sum().reset_index()

    tb_world[country_col] = "World"

    # remove "World" from original table and add the new "World" totals
    tb = tb[~tb[country_col].isin(["World"])]
    tb = pr.concat([tb, tb_world], ignore_index=True)

    return tb


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_migrant_stock = paths.load_dataset("migrant_stock")
    # Load regions dataset
    ds_regions = paths.load_dataset("regions")
    # Load income groups dataset
    ds_income_groups = paths.load_dataset("income_groups")

    # Read table from meadow dataset.
    tb = ds_migrant_stock.read("migrant_stock_dest_origin")

    agg = {
        "migrants_all_sexes": "sum",
        "migrants_female": "sum",
        "migrants_male": "sum",
    }

    # add regions to data

    # sum over country destination
    tb = geo.add_regions_to_table(
        tb,
        regions=REGIONS,
        ds_regions=ds_regions,
        ds_income_groups=ds_income_groups,
        aggregations=agg,
        country_col="country_destination",
        index_columns=["country_destination", "country_origin", "year"],
    )

    tb = add_others_to_world(tb, country_col="country_destination")

    # sum over country origin
    tb = geo.add_regions_to_table(
        tb,
        regions=REGIONS,
        ds_regions=ds_regions,
        ds_income_groups=ds_income_groups,
        aggregations=agg,
        country_col="country_origin",
        index_columns=["country_destination", "country_origin", "year"],
    )

    tb = add_others_to_world(tb, country_col="country_origin")

    # make male and female migrants dimensions
    tb = tb.melt(
        id_vars=["country_origin", "country_destination", "year"],
        value_vars=["migrants_all_sexes", "migrants_female", "migrants_male"],
        var_name="gender",
        value_name="migrants",
    )

    tb["gender"] = tb["gender"].map({"migrants_all_sexes": "all", "migrants_female": "female", "migrants_male": "male"})

    # Improve table format.
    tb = tb.format(["country_destination", "country_origin", "year", "gender"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_migrant_stock.metadata)

    # Save garden dataset.
    ds_garden.save()
