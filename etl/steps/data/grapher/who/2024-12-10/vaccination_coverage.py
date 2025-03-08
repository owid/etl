"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("vaccination_coverage")

    # Read table from garden dataset.
    tb = ds_garden.read("vaccination_coverage", reset_index=False)
    tb_infants = ds_garden.read("number_of_one_year_olds", reset_index=False)
    tb_newborns = ds_garden.read("number_of_newborns", reset_index=False)

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir,
        tables=[tb, tb_infants, tb_newborns],
        check_variables_metadata=True,
        default_metadata=ds_garden.metadata,
    )

    # Save changes in the new grapher dataset.
    ds_grapher.save()
