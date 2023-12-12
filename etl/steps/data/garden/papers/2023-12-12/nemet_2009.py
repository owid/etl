"""Harmonize data from Nemet (2009) paper on cost and capacity of photovoltaic energy.

"""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current data step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load dataset from meadow and read its main table.
    ds_meadow = paths.load_dataset("nemet_2009")
    tb_meadow = ds_meadow["nemet_2009"]

    #
    # Save outputs.
    #
    # Create a new dataset with the same metadata as meadow
    ds_garden = create_dataset(dest_dir=dest_dir, tables=[tb_meadow], check_variables_metadata=True)
    ds_garden.save()
