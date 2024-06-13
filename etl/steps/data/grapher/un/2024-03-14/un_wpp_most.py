"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("un_wpp_most")

    # Read five-year age-group table from garden dataset.
    tb_five = ds_garden["population_5_year_age_groups"].reset_index()
    tb_five = tb_five.rename(columns={"location": "country"})
    tb_five = tb_five.set_index(["country", "year"], verify_integrity=True)

    # Read ten-year age-group table from garden dataset.
    tb_ten = ds_garden["population_10_year_age_groups"].reset_index()
    tb_ten = tb_ten.rename(columns={"location": "country"})
    tb_ten = tb_ten.set_index(["country", "year"], verify_integrity=True)
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir, tables=[tb_five, tb_ten], check_variables_metadata=True, default_metadata=ds_garden.metadata
    )

    # Save changes in the new grapher dataset.
    ds_grapher.save()
