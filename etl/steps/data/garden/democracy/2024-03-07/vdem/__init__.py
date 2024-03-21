"""Load a meadow dataset and create a garden dataset."""

from copy import deepcopy
from typing import cast

import vdem_clean as clean  # VDEM's cleaning library

# import vdem_impute as impute  # VDEM's imputing library
from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    # %% Intro 1
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("vdem")

    # Read table from meadow dataset.
    tb = ds_meadow["vdem"].reset_index()

    tb = tb.astype(
        {
            "v2exnamhos": str,
        }
    )

    # %% Intro 2
    #
    # Process data.
    #
    tb = cast(Table, tb)
    tb = clean.initial_cleaning(tb)

    # While the head-of-government indicators generally should refer to the one in office on December 31, v2exfemhog seems to (occasionally?) refer to other points during the year. For most purposes, it makes sense to consistently refer to December 31, so I am recoding here.
    tb = clean.clean_female_flag(tb)

    # Harmonize country names
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Sort
    tb = tb.sort_values(["country", "year"])

    # %% PART 1: CLEAN
    # The following lines (until "PART 2") are the cleaning steps.
    # This is a transcription from Bastian's work: https://github.com/owid/notebooks/blob/main/BastianHerre/democracy/scripts/vdem_row_clean.do

    # Copy origins (some indicators will loose their 'origins' in metadata)
    origins = deepcopy(tb["country"].metadata.origins)

    # %% Create expanded and refined Regimes of the World indicator
    # (L76-L94) Create indicators for multi-party elections, and free and fair elections
    tb = clean.estimate_mulpar_indicators(tb)
    # (L96-L109) Create indicators for multi-party executive elections, and multi-party executive elections with imputed values between election-years:
    tb = clean.estimate_ex_indicators(tb)
    # (L109-L122) Create indicators for multi-party legislative elections, and multi-party legislative elections with imputed values between election-years
    tb = clean.estimate_leg_indicators(tb)
    # (L122-L141) Create indicators for multi-party head of state elections with imputed values between election-years
    tb = clean.estimate_hos_indicators(tb)
    # (L141-L167) Create indicators for multi-party head of government elections with imputed values between election-years
    tb = clean.estimate_hog_indicators(tb)
    # (L167-L175) Create indicators for multi-party executive and legislative elections with imputed values between election-years
    tb = clean.estimate_exleg_indicators(tb)
    # (L177-L201) Create indicator for multi-party head of executive elections with imputed values between election-years
    tb = clean.estimate_hoe_indicators(tb)
    # (L202-L300) Create dichotomous indicators for classification criteria
    tb = clean.estimate_dichotomous_indicators(tb)
    # (L302-L314) Create indicators for Regimes of the World with expanded coverage and minor changes to coding
    tb = clean.estimate_row_indicators(tb)

    # %% (L322-L389) Compare our and standard RoW coding
    tb = clean.compare_with_row_coding(tb)

    # %% (L389-L401) Finalize expanded and refined Regimes of the World indicator with ambiguous categories
    tb = clean.add_regime_amb_row(tb)
    # (L419-L424) Create reduced version of political regimes, only distinguishing between closed autocracies, electoral autocracies, and electoral democracies (including liberal democracies)
    tb["regime_redux_row"] = tb["regime_row_owid"].replace({3: 2})

    # %% Drop and rename columns
    # (L416) Drop irrelevant columns now
    tb = clean.drop_columns(tb)
    # (L427-L669) Rename columns of interest
    tb = clean.rename_columns(tb)

    # %% Ratio as share (share of adult citizens with vote right)
    tb["suffr_vdem"] = tb["suffr_vdem"] * 100

    # %% Gender indicators
    # Create variable identifying gender of chief executive
    tb = clean.estimate_gender_hoe_indicator(tb)
    # Estimate gender of HOG
    tb.loc[(tb["wom_hos_vdem"].notna()) & (tb["v2exhoshog"] == 1), "wom_hog_vdem"] = tb["wom_hos_vdem"]
    tb = tb.drop(columns=["v2exhoshog"])

    # %% Bring origins back
    columns = [col for col in tb.columns if col not in ["country", "year"]]
    for col in columns:
        tb[col].metadata.origins = origins

    # %% PART 2: IMPUTE
    # The following lines concern imputing steps.
    # Equivalent to: https://github.com/owid/notebooks/blob/main/BastianHerre/democracy/scripts/vdem_row_impute.do

    # %% Proceed
    # Expand to have observations for all years and countries
    # Indicator `vdem_obs` is created to flag the original observations (though they can easily be identified by the non-missing values in the other columns)
    # tb = impute.expand_observations(tb)

    # %% Impute values from adjacent years
    # Conditions for Australia and the year 1900
    condition_australia_1900 = (tb["country"] == "Australia") & (tb["year"] == 1900)
    # Perform replacements (is this just based on 1899?)
    tb.loc[condition_australia_1900, "regime_row_owid"] = 3
    tb.loc[condition_australia_1900, "regime_redux_row"] = 2
    tb.loc[condition_australia_1900, "regime_amb_row_owid"] = 8

    # The following are other candidates, but we discarded them because there are too many years missing.
    # - Honduras 1922-1933 (12 years missing)
    #   I favor no imputation because of 12 years of missing data, and the country may have met the criteria for democracy.
    # - Peru 1886-1891 (6 years missing)
    #   I favor no imputation because of six years of missing data, and even though one criterion for electoral autocracy is not met, the country may have met the criteria for democracy (if unlikely), thereby overriding the former.

    # %%

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
