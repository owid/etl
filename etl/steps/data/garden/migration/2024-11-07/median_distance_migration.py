"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import processing as pr

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


# Missing countries from distance file:
# Liechtenstein, Montenegro, Serbia, Bonaire Sint Eustatius and Saba, Curacao, Sint Maarten (Dutch part), United States Virgin Islands, Isle of Man, South Sudan, American Samoa, Guam, Mayotte, Channel Islands, Monaco, Vatican


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_flows = paths.load_dataset("migrant_stock")
    ds_distance = paths.load_dataset("geodist")

    # Read table from meadow dataset.
    tb = ds_flows["migrant_stock_dest_origin"].reset_index()
    tb_dist = ds_distance["geodist"].reset_index()

    tb_dist = tb_dist[["country_origin", "country_dest", "dist_capital_city"]]
    tb_dist = tb_dist.rename(columns={"country_dest": "country_destination"})

    tb = pr.merge(tb, tb_dist, on=["country_origin", "country_destination"], how="left")

    #
    # Process data.
    #
    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_flows.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()
