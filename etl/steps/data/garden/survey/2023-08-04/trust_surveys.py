"""Load a meadow dataset and create a garden dataset."""

from typing import cast

from owid.catalog import Dataset, Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow_latino = cast(Dataset, paths.load_dependency("latinobarometro_trust"))

    # Read table from meadow dataset.
    tb_latino = ds_meadow_latino["latinobarometro_trust"].reset_index()

    # Load meadow dataset.
    ds_meadow_ess = cast(Dataset, paths.load_dependency("ess_trust"))

    # Read table from meadow dataset.
    tb_ess = ds_meadow_ess["ess_trust"].reset_index()

    #
    # Process data.

    # Concatenate the two tables.
    tb = tb_latino.append(tb_ess[["country", "year", "trust"]], ignore_index=True)

    # Create index, verify columns, and sort.
    tb = tb.set_index(["country", "year"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=ds_meadow_latino.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()
