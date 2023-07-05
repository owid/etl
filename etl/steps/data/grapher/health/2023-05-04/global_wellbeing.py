"""Load a garden dataset and create a grapher dataset."""

from owid.catalog import Dataset

from etl.helpers import PathFinder, create_dataset, grapher_checks

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden: Dataset = paths.load_dependency("global_wellbeing")

    # Read table from garden dataset.
    tb_questions = ds_garden["global_wellbeing"]
    tb_index = ds_garden["global_wellbeing_index"]
    #
    # Process data.
    #

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    tables = [
        tb_questions,
        tb_index,  # I think this leads to an error in Grapher, due to duplicate entries for the same country and year.
    ]
    ds_grapher = create_dataset(dest_dir, tables=tables, default_metadata=ds_garden.metadata)

    #
    # Checks.
    #
    grapher_checks(ds_grapher)

    # Save changes in the new grapher dataset.
    ds_grapher.save()
