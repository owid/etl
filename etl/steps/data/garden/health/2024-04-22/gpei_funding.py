"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("gpei_funding")

    # Read table from meadow dataset.
    tb = ds_meadow["gpei_funding"].reset_index()

    #
    # Process data.
    tb["total"] = tb[
        [
            "domestic_resources",
            "g7_countries__and__european_commission",
            "multilateral_sector",
            "non_g7_oecd_countries",
            "other_donor_countries",
            "private_sector__non_governmental_donors",
        ]
    ].sum(axis=1)
    tb["total"] = tb["total"].copy_metadata(tb["domestic_resources"])
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
