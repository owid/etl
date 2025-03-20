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

    # make male and female migrants dimensions
    tb = tb.melt(
        id_vars=["country_origin", "country_destination", "year"],
        value_vars=["migrants_all_sexes", "migrants_female", "migrants_male"],
        var_name="gender",
        value_name="migrants",
    )
    tb["gender"] = tb["gender"].map(
        {
            "migrants_all_sexes": "all",
            "migrants_female": "female",
            "migrants_male": "male",
        }
    )

    # Copy table
    tb_dest_origin = tb.copy()

    # Adapt table
    tb = get_table_column_pet_indicator(tb)

    # Format table
    tables = [
        tb_dest_origin.format(["country_destination", "country_origin", "year", "gender"]),
        tb.format(["country", "year", "gender", "country_origin_or_dest"], short_name="migration_stock_flows"),
    ]

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=tables, default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()


def get_table_column_pet_indicator(tb):
    """Have a column for 'migrants' and another for 'emigrants'."""
    tb_emigrants = tb.rename(
        columns={
            "country_origin": "country",
            "country_destination": "country_origin_or_dest",
            "migrants": "emigrants",
        }
    )
    tb_immigrants = tb.rename(
        columns={
            "country_origin": "country_origin_or_dest",
            "country_destination": "country",
            "migrants": "immigrants",
        }
    )
    tb = tb_emigrants.merge(tb_immigrants, on=["country", "country_origin_or_dest", "year", "gender"], how="outer")

    return tb
