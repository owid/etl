"""Load a meadow dataset and create a garden dataset."""

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("gbd_mental_health")

    # Read table from meadow dataset.
    tb = ds_meadow["gbd_mental_health"].reset_index()

    #
    # Process data.
    #
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    # Split into two tables: one for deaths, one for DALYs
    tb_deaths = tb[tb["measure"] == "Deaths"].copy()
    tb_dalys = tb[tb["measure"] == "DALYs (Disability-Adjusted Life Years)"].copy()
    # Shorten the metric name for DALYs
    tb_dalys["measure"] = "DALYs"

    # Drop the measure column
    tb_deaths = tb_deaths.drop(columns="measure")
    tb_dalys = tb_dalys.drop(columns="measure")

    # Format the tables
    tb_deaths = tb_deaths.format(["country", "year", "metric", "age", "cause"], short_name="gbd_cause_deaths")
    tb_dalys = tb_dalys.format(["country", "year", "metric", "age", "cause"], short_name="gbd_cause_dalys")

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb_deaths, tb_dalys], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
