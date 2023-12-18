"""Create a garden dataset on renewable energy investments based on UNEP data.

"""
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load dataset from meadow and read its main table.
    ds_meadow = paths.load_dataset("renewable_energy_investments")
    tb_meadow = ds_meadow["renewable_energy_investments"]

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_meadow], check_variables_metadata=True)
    ds_garden.save()
