"""Load a meadow dataset and create a garden dataset."""

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

REGIONS = list(geo.REGIONS.keys()) + ["World"]


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("migrant_stock")
    # Load regions dataset
    ds_regions = paths.load_dataset("regions")
    # Load income groups dataset
    ds_income_groups = paths.load_dataset("income_groups")

    # Read table from meadow dataset.
    tb = ds_meadow.read("migrant_stock_dest_origin")

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

    # Improve table format.
    tb = tb.format(["country_destination", "country_origin", "year"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()
