"""Grapher step for the primary energy consumption dataset.
"""
from owid.catalog import Dataset

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load garden dataset and read its main table.
    ds_garden: Dataset = paths.load_dependency("primary_energy_consumption")
    tb_garden = ds_garden["primary_energy_consumption"].reset_index()

    #
    # Process data.
    #
    # Remove unnecessary columns.
    tb = tb_garden.drop(columns=["gdp", "population", "source"])

    # Set an appropriate index and sort conveniently.
    tb = tb.set_index(["country", "year"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    ds_grapher = create_dataset(
        dest_dir=dest_dir, tables=[tb], default_metadata=ds_garden.metadata, check_variables_metadata=True
    )
    ds_grapher.save()
