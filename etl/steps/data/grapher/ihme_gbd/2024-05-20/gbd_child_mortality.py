"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("gbd_child_mortality")
    # Read table from garden dataset.
    tb_deaths = ds_garden["gbd_child_mortality_deaths"]
    tb_dalys = ds_garden["gbd_child_mortality_dalys"]
    tb_deaths_slope = ds_garden["gbd_child_mortality_slope"]
    tb_india = ds_garden["gbd_child_mortality_india"]
    tb_infant = ds_garden["gbd_child_mortality_infant"]
    # Save outputs .
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir,
        tables=[tb_deaths, tb_dalys, tb_deaths_slope, tb_india, tb_infant],
        check_variables_metadata=True,
        default_metadata=ds_garden.metadata,
    )

    # Save changes in the new grapher dataset.
    ds_grapher.save()
