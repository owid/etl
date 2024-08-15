"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("atkinson_2008_canada")

    # Read table from meadow dataset.
    tb_oecd_lms = ds_meadow["oecd_lms"].reset_index()
    tb_census = ds_meadow["census"].reset_index()
    tb_manufacturing = ds_meadow["manufacturing"].reset_index()

    #
    # Process data.
    #
    tb_oecd_lms = tb_oecd_lms.format(["country", "year"])
    tb_census = tb_census.format(["country", "year"])
    tb_manufacturing = tb_manufacturing.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir,
        tables=[tb_oecd_lms, tb_census, tb_manufacturing],
        check_variables_metadata=True,
        default_metadata=ds_meadow.metadata,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
