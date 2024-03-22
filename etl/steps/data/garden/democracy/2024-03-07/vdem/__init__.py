"""Load a meadow dataset and create a garden dataset."""

from typing import cast

import vdem_clean as clean  # VDEM's cleaning library
import vdem_impute as impute  # VDEM's imputing library
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

    # %% Impute values from adjacent years
    # Conditions for Australia and the year 1900
    condition_australia_1900 = (tb["country"] == "Australia") & (tb["year"] == 1900)
    # Perform replacements (is this just based on 1899?)
    tb.loc[condition_australia_1900, "regime_row_owid"] = 3
    tb.loc[condition_australia_1900, "regime_redux_row_owid"] = 2
    tb.loc[condition_australia_1900, "regime_amb_row_owid"] = 8

    # The following are other candidates, but we discarded them because there are too many years missing.
    # - Honduras 1922-1933 (12 years missing)
    #   I favor no imputation because of 12 years of missing data, and the country may have met the criteria for democracy.
    # - Peru 1886-1891 (6 years missing)
    #   I favor no imputation because of six years of missing data, and even though one criterion for electoral autocracy is not met, the country may have met the criteria for democracy (if unlikely), thereby overriding the former.

    # %%
    # 0. checks

    # For each country:
    # 2.

    # %% Set index
    tb = tb.set_index(["country", "year"], verify_integrity=True)

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
