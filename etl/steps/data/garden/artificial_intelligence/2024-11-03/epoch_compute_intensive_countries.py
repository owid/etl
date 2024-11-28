"""Generate aggregated table for total yearly and cumulative number of compute intensive AI systems in each country."""

import datetime as dt

import shared as sh

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    paths.log.info("epoch_compute_intensive_countries.start")

    #
    # Load inputs.
    #
    # Load the ds_meadow dataset.
    ds_meadow = paths.load_dataset("epoch_compute_intensive")

    # Read table from meadow dataset.
    tb = ds_meadow["epoch_compute_intensive"]
    tb = tb.reset_index()

    #
    # Process data.
    #
    # Define the columns that are not needed
    unused_columns = [
        "domain",
        "authors",
        "organization",
        "parameters",
        "training_compute__flop",
        "training_dataset_size__datapoints",
    ]

    # Aggregate the data by country
    tb_agg = sh.calculate_aggregates(tb, "country__from_organization", paths.short_name, unused_columns)

    # Rename the 'country__from_organization' column to 'country'
    tb_agg = tb_agg.rename(columns={"country__from_organization": "country"})

    # Harmonize the country names
    tb_agg = geo.harmonize_countries(df=tb_agg, countries_file=paths.country_mapping_path)

    # Set the index to year and country
    tb_agg = tb_agg.format(["year", "country"])

    date_acessed = tb_agg.yearly_count.m.origins[0].date_accessed

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir,
        tables=[tb_agg],
        yaml_params={"date_accessed": dt.datetime.strptime(date_acessed, "%Y-%m-%d").strftime("%d %B %Y")},
    )

    # Save changes in the new garden dataset.
    ds_garden.save()

    paths.log.info("epoch_compute_intensive_countries.end")
