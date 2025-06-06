"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("fertility_rate")

    # Read table from garden dataset.
    tb = ds_garden.read("fertility_rate", reset_index=False)
    tb_by_age = ds_garden.read("fertility_rate_by_age", reset_index=False)
    tb_by_age = tb_by_age.rename_index_names({"age": "year"})

    #
    # Save outputs.
    #
    tables = [
        tb,
        tb_by_age,
    ]
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir, tables=tables, check_variables_metadata=True, default_metadata=ds_garden.metadata
    )

    # Save changes in the new grapher dataset.
    ds_grapher.save()
