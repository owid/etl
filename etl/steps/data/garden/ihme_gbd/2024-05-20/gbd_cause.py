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
    ds_meadow = paths.load_dataset("gbd_cause")

    # Read table from meadow dataset.
    tb = ds_meadow["gbd_cause"].reset_index()

    #
    # Process data.
    #
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    # Split into two tables: one for deaths, one for DALYs
    tb_deaths = tb[tb["metric"] == "Deaths"].copy()
    tb_dalys = tb[tb["metric"] == "DALYs (Disability-Adjusted Life Years)"].copy()
    # Shorten the metric name for DALYs
    tb_dalys["metric"] = "DALYs"

    # Format the tables
    tb_deaths = tb_deaths.format(["country", "year", "measure", "age", "cause"], short_name="gbd_cause_deaths")
    tb_dalys = tb_dalys.format(["country", "year", "measure", "age", "cause"], short_name="gbd_cause_dalys")

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb_deaths, tb_dalys], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
