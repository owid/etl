"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("un_sdg")

    # Read table from meadow dataset.
    tb = ds_meadow["un_sdg"].reset_index()

    urbanization_tb = tb[(tb["indicator"] == "11.7.1") | (tb["indicator"] == "11.2.1")]
    cols = ["seriesdescription", "country", "year", "value", "cities"]
    urbanization_tb = urbanization_tb[cols]
    # Convert the seriesdescription to string to avoid using categorical data
    urbanization_tb["seriesdescription"] = urbanization_tb["seriesdescription"].astype(str)

    urbanization_tb = urbanization_tb.set_index(
        ["country", "seriesdescription", "year", "cities"], verify_integrity=True
    )

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(
        dest_dir, tables=[urbanization_tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new meadow dataset.
    ds_meadow.save()
