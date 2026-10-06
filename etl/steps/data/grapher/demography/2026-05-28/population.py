"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


# Each variable has data from 10,000 BCE until 2100. We create two new versions for each variables:
# - Historical: data from 10,000 BCE until YEAR_THRESHOLD - 1.
# - Projection: data from YEAR_THRESHOLD until 2100.
YEAR_THRESHOLD = 2024


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("population")

    # Read table from garden dataset.
    tb_original = ds_garden["population_original"].update_metadata(short_name="population")

    # Set origins on `source`
    tb_original.source.m.origins = tb_original.population.m.origins

    #
    # Save outputs.
    #
    tables = [
        tb_original,
        ds_garden["population_density"],
        ds_garden["population_growth_rate"],
        ds_garden["historical"],
        ds_garden["projections"],
    ]
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(dest_dir, tables=tables, default_metadata=ds_garden.metadata)

    # Save changes in the new grapher dataset.
    ds_grapher.save()
