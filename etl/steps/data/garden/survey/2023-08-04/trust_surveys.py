"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load Latinobarómetro table
    ds_meadow_latino = paths.load_dataset("latinobarometro_trust")
    tb_latino = ds_meadow_latino["latinobarometro_trust"].reset_index()

    # Load Afrobarometer table
    ds_meadow_afro = paths.load_dataset("afrobarometer_trust")
    tb_afro = ds_meadow_afro["afrobarometer_trust"].reset_index()

    # Load ESS table
    ds_meadow_ess = paths.load_dataset("ess_trust")
    tb_ess = ds_meadow_ess["ess_trust"].reset_index()

    #
    # Process data.

    # Concatenate the tables
    tb = pr.concat(
        [tb_latino, tb_afro, tb_ess[["country", "year", "trust"]]], ignore_index=True, short_name="trust_surveys"
    )

    # Create index, verify columns, and sort.
    tb = tb.set_index(["country", "year"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb])

    # Save changes in the new garden dataset.
    ds_garden.save()
