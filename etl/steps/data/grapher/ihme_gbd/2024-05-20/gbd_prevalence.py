"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("gbd_prevalence")
    ds_garden.metadata.title = "GBD Prevalence"
    # Read table from garden dataset.
    tb_prevalence = ds_garden["gbd_prevalence"]
    # tb_incidence = ds_garden["gbd_incidence"]

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir,
        tables=[tb_prevalence],
        check_variables_metadata=True,
        default_metadata=ds_garden.metadata,
    )

    # Save changes in the new grapher dataset.
    ds_grapher.save()
