"""Load a meadow dataset and create a garden dataset."""

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

REGIONS = [reg for reg in geo.REGIONS.keys() if reg != "European Union (27)"]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_migration = paths.load_dataset("migrant_stock")
    ds_regions = paths.load_dataset("regions")
    ds_income_groups = paths.load_dataset("income_groups")

    # Read table from meadow dataset.
    tb = ds_migration["migrant_stock_dest_origin"].reset_index()

    # Aggregate regions (twice, once for destination and once for origin).
    tb_reg = geo.add_regions_to_table(
        tb,
        ds_regions=ds_regions,
        ds_income_groups=ds_income_groups,
        index_columns=["country_destination", "country_origin", "year"],
        country_col="country_destination",
        frac_allowed_nans_per_year=0.1,
    )

    tb_reg = geo.add_regions_to_table(
        tb_reg,
        ds_regions=ds_regions,
        ds_income_groups=ds_income_groups,
        index_columns=["country_destination", "country_origin", "year"],
        country_col="country_origin",
        frac_allowed_nans_per_year=0.1,
    )

    # Filter only on regions
    tb_reg = tb_reg[tb_reg["country_destination"].isin(REGIONS) & tb_reg["country_origin"].isin(REGIONS)]

    tb_reg = tb_reg.format(["country_destination", "country_origin", "year"], short_name="migration_between_regions")

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb_reg], check_variables_metadata=True, default_metadata=ds_migration.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
