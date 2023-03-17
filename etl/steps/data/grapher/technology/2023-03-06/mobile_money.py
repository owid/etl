"""Load a garden dataset and create a grapher dataset."""

from owid.catalog import Dataset

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden: Dataset = paths.load_dependency("mobile_money")

    # Read table from garden dataset.
    tb_garden = ds_garden["mobile_money"].rename(columns={"region": "country"})

    #
    # Process data.
    #

    #
    # Checks.
    #
    assert {"year", "country"} <= set(tb_garden.reset_index().columns), "Table must have columns country and year."

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(dest_dir, tables=[tb_garden], default_metadata=ds_garden.metadata)

    # Save changes in the new grapher dataset.
    ds_grapher.save()
