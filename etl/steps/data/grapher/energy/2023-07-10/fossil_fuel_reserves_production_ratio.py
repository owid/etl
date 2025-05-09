"""Grapher step for the fossil fuel reserves-to-production ratio dataset."""

from owid.catalog import Dataset

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load garden dataset and read its main table.
    ds_garden: Dataset = paths.load_dependency("fossil_fuel_reserves_production_ratio")
    tb_garden = ds_garden["fossil_fuel_reserves_production_ratio"]

    # Create new grapher dataset.
    dataset = create_dataset(
        dest_dir=dest_dir, tables=[tb_garden], default_metadata=ds_garden.metadata, check_variables_metadata=True
    )
    dataset.save()
