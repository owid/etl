"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("hfd")

    # Read table from garden dataset.
    tables = [
        ds_garden.read("period", reset_index=False),
        ds_garden.read("cohort", reset_index=False).rename_index_names(
            {
                "cohort": "year",
            }
        ),
        ds_garden.read("period_ages", reset_index=False),
        ds_garden.read("cohort_ages", reset_index=False).rename_index_names(
            {
                "cohort": "year",
            }
        ),
    ]
    #
    # Process data.
    #

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir, tables=tables, check_variables_metadata=True, default_metadata=ds_garden.metadata, long_to_wide=False
    )

    # Save changes in the new grapher dataset.
    ds_grapher.save()
