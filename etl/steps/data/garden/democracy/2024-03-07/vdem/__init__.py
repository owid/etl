"""Load a meadow dataset and create a garden dataset."""

from typing import cast

import vdem_clean as clean  # VDEM's cleaning library
from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    # %% Intro
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

    #
    # Process data.
    #
    tb = cast(Table, tb)
    tb = clean.initial_cleaning(tb)

    # While the head-of-government indicators generally should refer to the one in office on December 31, v2exfemhog seems to (occasionally?) refer to other points during the year. For most purposes, it makes sense to consistently refer to December 31, so I am recoding here.
    tb = clean.clean_female_flag(tb)

    # Sort
    tb = tb.sort_values(["country", "year"])

    # (L76-L94) Create expanded and refined Regimes of the World indicator
    # %% Create indicators for multi-party elections, and free and fair elections
    tb = clean.estimate_mulpar_indicators(tb)

    # %% (L96-L109) Create indicators for multi-party executive elections, and multi-party executive elections with imputed values between election-years:
    tb = clean.estimate_ex_indicators(tb)

    # %% (L109-L122) Create indicators for multi-party legislative elections, and multi-party legislative elections with imputed values between election-years
    tb = clean.estimate_leg_indicators(tb)

    # %% (L122-L141) Create indicators for multi-party head of state elections with imputed values between election-years
    tb = clean.estimate_hos_indicators(tb)

    # %% (L141-L167) Create indicators for multi-party head of government elections with imputed values between election-years
    tb = clean.estimate_hog_indicators(tb)

    # %% (L167-L175) Create indicators for multi-party executive and legislative elections with imputed values between election-years
    tb = clean.estimate_exleg_indicators(tb)

    # %% (L177-L201) Create indicator for multi-party head of executive elections with imputed values between election-years
    tb = clean.estimate_hoe_indicators(tb)

    # %% (L202-L300) Create dichotomous indicators for classification criteria
    tb = clean.estimate_dichotomous_indicators(tb)

    # %% (L302-L314) Create indicators for Regimes of the World with expanded coverage and minor changes to coding
    tb = clean.estimate_row_indicators(tb)

    # %% (L322-L389) Compare our and standard RoW coding
    tb = clean.compare_with_row_coding(tb)

    # %% (L389-L401) Finalize expanded and refined Regimes of the World indicator with ambiguous categories
    tb = clean.add_regime_amb_row(tb)

    # %% (L416) Drop irrelevant columns now
    tb = clean.drop_columns(tb)

    # %% (L419-L424) Create reduced version of political regimes, only distinguishing between closed autocracies, electoral autocracies, and electoral democracies (including liberal democracies)
    tb["regime_redux_row"] = tb["regime_row_owid"].replace({3: 2})

    # %% (L427-L669) Rename columns of interest
    tb = clean.rename_columns(tb)

    # %% Ratio as share (share of adult citizens with vote right)
    tb["suffr_vdem"] = tb["suffr_vdem"] * 100

    # %% Create variable identifying gender of chief executive
    tb = clean.estimate_gender_hoe_indicator(tb)

    # %% Estimate gender of HOG
    tb.loc[(tb["wom_hos_vdem"].notna()) & (tb["v2exhoshog"] == 1), "wom_hog_vdem"] = tb["wom_hos_vdem"]
    tb = tb.drop(columns=["v2exhoshog"])

    # %% Proceed
    # Harmonize country names
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Set index
    tb = tb.set_index(["country", "year"], verify_integrity=True)

    # Dtypes
    tb = tb.astype(dtype={"v2exnamhos": "string"})

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
