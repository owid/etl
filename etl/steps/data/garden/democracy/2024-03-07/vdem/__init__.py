"""Load a meadow dataset and create a garden dataset."""

from typing import cast

import vdem_clean as clean  # VDEM's cleaning library
import vdem_impute as impute  # VDEM's imputing library
import vdem_refine as refine  # VDEM's imputing library
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    # %% Load data
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("vdem")

    # Read table from meadow dataset.
    tb = ds_meadow["vdem"].reset_index()
    tb = cast(Table, tb.astype({"v2exnamhos": str}))

    #
    # Process data.
    #

    # %% PART 1: CLEAN
    # The following lines (until "PART 2") are the cleaning steps.
    # This is a transcription from Bastian's work: https://github.com/owid/notebooks/blob/main/BastianHerre/democracy/scripts/vdem_row_clean.do

    tb = clean.run(tb)

    # %% PART 2: IMPUTE
    # The following lines concern imputing steps.
    # Equivalent to: https://github.com/owid/notebooks/blob/main/BastianHerre/democracy/scripts/vdem_row_impute.do

    tb = impute.run(tb)

    # %% PART 3: REFINE
    tb = refine.run(tb)

    # %% Set index
    tb = tb.format()

    # %% Save
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


# %%
