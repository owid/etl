"""Load a meadow dataset and create a garden dataset."""

from typing import cast

from owid.catalog import Dataset

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = cast(Dataset, paths.load_dependency("icd_codes"))

    # Read table from meadow dataset.
    tb = ds_meadow["icd_codes"]
    tb = tb.reset_index()

    tb = tb.groupby(["year", "icd"]).count().reset_index()
    tb = tb.rename(columns={"icd": "country", "country": "countries_using_icd_code"})
    tb = tb.set_index(["year", "country"], verify_integrity=True)
    tb.metadata.short_name = "icd_codes"
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()
