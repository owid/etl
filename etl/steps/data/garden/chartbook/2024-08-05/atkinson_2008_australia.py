"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("atkinson_2008_australia")

    # Read table from meadow dataset.
    tb_oecd_lms = ds_meadow["oecd_lms"].reset_index()
    tb_eeh = ds_meadow["eeh"].reset_index()

    #
    # Process data.
    #
    tb_oecd_lms = tb_oecd_lms.format(["country", "year"], short_name="oecd_lms")
    tb_eeh = tb_eeh.format(["country", "year"], short_name="eeh")

    # Only keep p90
    tb_oecd_lms = tb_oecd_lms[["p90"]]
    tb_eeh = tb_eeh[["p90"]]

    # Remove missing values
    tb_oecd_lms = tb_oecd_lms.dropna()
    tb_eeh = tb_eeh.dropna()

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb_oecd_lms, tb_eeh], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
