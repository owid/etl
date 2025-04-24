"""Common grapher step for all FAOSTAT domains."""

from pathlib import Path

from etl.helpers import PathFinder

# Define path to current folder, namespace and version of all datasets in this folder.
CURRENT_DIR = Path(__file__).parent
VERSION = CURRENT_DIR.name


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Fetch the dataset short name from dest_dir.
    dataset_short_name = Path(dest_dir).name

    # Define path to current step file.
    current_step_file = (CURRENT_DIR / dataset_short_name).with_suffix(".py")

    # Get paths and naming conventions for current data step.
    paths = PathFinder(current_step_file.as_posix())

    # Load latest garden dataset.
    ds_garden = paths.load_dataset(dataset_short_name)

    # Load wide  table from dataset.
    tb_garden = ds_garden[f"{dataset_short_name}_flat"]

    #
    # Save outputs.
    #
    # Create a new grapher dataset.
    ds_grapher = paths.create_dataset(
        tables=[tb_garden], default_metadata=ds_garden.metadata, check_variables_metadata=True
    )
    ds_grapher.save()
