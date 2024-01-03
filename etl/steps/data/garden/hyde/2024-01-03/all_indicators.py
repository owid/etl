"""Load a meadow dataset and create a garden dataset."""

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("all_indicators")
    # Read table from meadow dataset.
    tb = ds_meadow["all_indicators"].reset_index()

    # Load meadow dataset.
    ds_codes = paths.load_dataset("general_files")
    tb_codes = ds_codes["general_files"].reset_index()

    #
    # Process data.
    #
    # Add country name
    tb = tb.rename(columns={"country": "iso_code"}).astype({"iso_code": "str"})
    tb_codes = tb_codes.astype("str")
    tb = tb.merge(tb_codes, on="iso_code", how="left")
    tb.loc[tb["iso_code"] == "Total", "country"] = "World"
    # Drop columns
    tb = tb.drop(columns=["iso_code"])

    # Set index
    tb = tb.set_index(["country", "year"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
