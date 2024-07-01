"""Load a meadow dataset and create a garden dataset."""

from shared import add_share_population

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
REGIONS = ["North America", "South America", "Europe", "Africa", "Asia", "Oceania"]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("gbd_mental_health_burden")
    # Read table from meadow dataset.
    tb = ds_meadow["gbd_mental_health_burden"].reset_index()
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Add a share of the population column
    tb = add_share_population(tb)
    #

    tb_prev = tb.query("measure == 'Prevalence'").drop(columns="measure")
    tb_daly = tb.query("measure == 'DALYs'").drop(columns="measure")
    # Format the tables
    tb_prev = tb_prev.format(
        ["country", "year", "cause", "metric", "age"], short_name="gbd_mental_health_burden_prevalence"
    )
    tb_daly = tb_daly.format(["country", "year", "cause", "metric", "age"], short_name="gbd_mental_health_burden_dalys")

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir,
        tables=[tb_prev, tb_daly],
        check_variables_metadata=True,
        default_metadata=ds_meadow.metadata,
        # Table has optimal types already and repacking can be time consuming.
        repack=False,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
