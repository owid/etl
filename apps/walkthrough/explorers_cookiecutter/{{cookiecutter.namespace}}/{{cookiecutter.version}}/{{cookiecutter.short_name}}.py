"""Load a garden dataset and create an explorers dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("{{cookiecutter.short_name}}")

    # Read table from garden dataset.
    tb_garden = ds_garden["{{cookiecutter.short_name}}"]

    # Create explorer dataset, with garden table and metadata in csv format
    ds_explorer = create_dataset(dest_dir, tables=[tb_garden], formats=["csv"], default_metadata=ds_garden.metadata)
    ds_explorer.save()
