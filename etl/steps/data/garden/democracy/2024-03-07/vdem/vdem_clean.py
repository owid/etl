"""Code equivalent to clean step (STATA).

ref: https://github.com/owid/notebooks/blob/main/BastianHerre/democracy/scripts/vdem_row_do
"""
from copy import deepcopy
from typing import Union, cast

import numpy as np
from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(tb: Table) -> Table:
    tb = cast(Table, tb)
    tb = initial_cleaning(tb)

    # While the head-of-government indicators generally should refer to the one in office on December 31, v2exfemhog seems to (occasionally?) refer to other points during the year. For most purposes, it makes sense to consistently refer to December 31, so I am recoding here.
    tb = clean_female_flag(tb)

    # Harmonize country names
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Sort
    tb = tb.sort_values(["country", "year"])

    # %% Create expanded and refined Regimes of the World indicator
    # (L76-L94) Create indicators for multi-party elections, and free and fair elections
    tb = estimate_mulpar_indicators(tb)
    # (L96-L109) Create indicators for multi-party executive elections, and multi-party executive elections with imputed values between election-years:
    tb = estimate_ex_indicators(tb)
    # (L109-L122) Create indicators for multi-party legislative elections, and multi-party legislative elections with imputed values between election-years
    tb = estimate_leg_indicators(tb)
    # (L122-L141) Create indicators for multi-party head of state elections with imputed values between election-years
    tb = estimate_hos_indicators(tb)
    # (L141-L167) Create indicators for multi-party head of government elections with imputed values between election-years
    tb = estimate_hog_indicators(tb)
    # (L167-L175) Create indicators for multi-party executive and legislative elections with imputed values between election-years
    tb = estimate_exleg_indicators(tb)
    # (L177-L201) Create indicator for multi-party head of executive elections with imputed values between election-years
    tb = estimate_hoe_indicators(tb)
    # (L202-L300) Create dichotomous indicators for classification criteria
    tb = estimate_dichotomous_indicators(tb)
    # (L302-L314) Create indicators for Regimes of the World with expanded coverage and minor changes to coding
    tb = estimate_row_indicators(tb)

    # %% (L322-L389) Compare our and standard RoW coding
    tb = compare_with_row_coding(tb)

    # %% (L389-L401) Finalize expanded and refined Regimes of the World indicator with ambiguous categories
    tb = add_regime_amb_row(tb)
    # (L419-L424) Create reduced version of political regimes, only distinguishing between closed autocracies, electoral autocracies, and electoral democracies (including liberal democracies)
    tb["regime_redux_row_owid"] = tb["regime_row_owid"].replace({3: 2})

    # %% Drop and rename columns
    # (L416) Drop irrelevant columns now
    tb = drop_columns(tb)
    # (L427-L669) Rename columns of interest
    tb = rename_columns(tb)

    # %% Ratio as share (share of adult citizens with vote right)
    tb["suffr_vdem"] = tb["suffr_vdem"] * 100

    # %% Gender indicators
    # Create variable identifying gender of chief executive
    tb = estimate_gender_hoe_indicator(tb)
    # Estimate gender of HOG
    tb.loc[(tb["wom_hos_vdem"].notna()) & (tb["v2exhoshog"] == 1), "wom_hog_vdem"] = tb["wom_hos_vdem"]
    tb = tb.drop(columns=["v2exhoshog"])

    return tb


def initial_cleaning(tb: Table) -> Table:
    """Initial data cleaning."""
    # Drop superfluous observations
    tb = tb.loc[~((tb["country"] == "Italy") & (tb["year"] == 1861))]
    tb.loc[(tb["country"] == "Piedmont-Sardinia") & (tb["year"] == 1861), "country"] = "Italy"

    # Goemans et al.'s (2009) Archigos dataset, rulers.org, and worldstatesmen.org identify non-elected General Raoul Cédras as the de-facto leader of Haiti from 1991 until 1994.
    tb.loc[(tb["country"] == "Haiti") & (tb["year"] >= 1991) & (tb["year"] <= 1993), "v2exnamhos"] = "Raoul Cédras"
    tb.loc[(tb["country"] == "Haiti") & (tb["year"] >= 1991) & (tb["year"] <= 1993), "v2ex_hosw"] = 1
    tb.loc[(tb["country"] == "Haiti") & (tb["year"] >= 1991) & (tb["year"] <= 1993), "v2ex_hogw"] = 0
    tb.loc[(tb["country"] == "Haiti") & (tb["year"] >= 1991) & (tb["year"] <= 1993), "v2exaphogp"] = 0
    return tb


def clean_female_flag(tb: Table) -> Table:
    """While the head-of-government indicators generally should refer to the one in office on December 31, v2exfemhog seems to (occasionally?) refer to other points during the year. For most purposes, it makes sense to consistently refer to December 31, so I am recoding here."""
    ## HOG
    tb.loc[tb["v2exnamhog"] == "Diango Cissoko", "v2exfemhog"] = 0
    tb.loc[tb["v2exnamhog"] == "Ion Chicu", "v2exfemhog"] = 0
    tb.loc[tb["v2exnamhog"] == "Joseph Jacques Jean Chrétien", "v2exfemhog"] = 0
    tb.loc[tb["v2exnamhog"] == "KåreIsaachsen Willoch", "v2exfemhog"] = 0
    tb.loc[tb["v2exnamhog"] == "YuriiIvanovych Yekhanurov", "v2exfemhog"] = 0
    ## HOS
    tb.loc[tb["v2exnamhos"] == "Ali Ben Bongo Ondimba", "v2exfemhos"] = 0
    tb.loc[tb["v2exnamhos"] == "Chulalongkorn (Rama V)", "v2exfemhos"] = 0
    tb.loc[tb["v2exnamhos"] == "Dieudonné François Joseph Marie Reste", "v2exfemhos"] = 0
    tb.loc[tb["v2exnamhos"] == "Letsie III", "v2exfemhos"] = 0
    tb.loc[tb["v2exnamhos"] == 'Miguel I "o Rei Absoluto"', "v2exfemhos"] = 0
    tb.loc[tb["v2exnamhos"] == "Moshoeshoe II", "v2exfemhos"] = 0
    tb.loc[tb["v2exnamhos"] == "Prince Zaifeng", "v2exfemhos"] = 0
    tb.loc[tb["v2exnamhos"] == "Rajkeswur Purryag", "v2exfemhos"] = 0
    tb.loc[tb["v2exnamhos"] == "Shimon Peres", "v2exfemhos"] = 0

    return tb


def estimate_mulpar_indicators(tb: Table) -> Table:
    """Create indicators for multi-party elections, and free and fair elections."""
    columns = {
        # Create indicators for multi-party elections with imputed values between election-years
        "v2elmulpar_osp": "v2elmulpar_osp",
        "v2elmulpar_osp_codehigh": "v2elmulpar_osp_high",
        "v2elmulpar_osp_codelow": "v2elmulpar_osp_low",
        # Create indicators for free and fair elections with imputed values between election-years
        "v2elfrfair_osp": "v2elfrfair_osp",
        "v2elfrfair_osp_codehigh": "v2elfrfair_osp_high",
        "v2elfrfair_osp_codelow": "v2elfrfair_osp_low",
    }
    columns_old = list(columns.keys())
    columns_new = list(columns.values())

    ## Forward fill indicators when there are regularly scheduled national elections on course, as stipulated by election law or well-established precedent
    mask = tb["v2x_elecreg"] == 1
    tb[columns_new] = tb[columns_old].copy()
    tb.loc[mask, columns_new] = tb.groupby(["country"])[columns_new].ffill().loc[mask]
    # condition = (tb['v2x_elecreg'] == 1) & (tb['v2elmulpar_osp_imp'].isna())
    # tb.loc[:, columns_new] = tb.groupby(["country"])[columns_old].transform(lambda x: x.ffill().where(x.bfill().eq(1)))
    return tb


def estimate_ex_indicators(tb: Table) -> Table:
    """Create indicators for multi-party executive elections, and multi-party executive elections with imputed values between election-years."""
    MASK_PRESIDENTIAL = (tb["v2eltype_6"] == 1) | (tb["v2eltype_7"] == 1)
    columns = {
        "v2elmulpar_osp": "v2elmulpar_osp_ex",
        "v2elmulpar_osp_codehigh": "v2elmulpar_osp_ex_high",
        "v2elmulpar_osp_codelow": "v2elmulpar_osp_ex_low",
    }
    columns_old = list(columns.keys())
    columns_new = list(columns.values())
    tb[columns_new] = tb[columns_old].copy()
    tb.loc[~MASK_PRESIDENTIAL, columns_new] = np.nan

    ## Forward fill indicators when there are regularly scheduled national elections on the executive on course, as stipulated by election law or well-established precedent
    mask = tb["v2xex_elecreg"] == 1
    tb.loc[mask, columns_new] = tb.groupby(["country"])[columns_new].ffill().loc[mask]
    return tb


def estimate_leg_indicators(tb: Table) -> Table:
    """Create indicators for multi-party legislative elections, and multi-party legislative elections with imputed values between election-years."""
    ## v2eltype_4 and v2eltype_5 excluded in Marcus Tannenberg and Anna Lührmann's Stata code; included here to align coding with code in V-Dem's data pipeline.
    MASK_LEG = (tb["v2eltype_0"] == 1) | (tb["v2eltype_1"] == 1) | (tb["v2eltype_4"] == 1) | (tb["v2eltype_5"] == 1)
    columns = {
        "v2elmulpar_osp": "v2elmulpar_osp_leg",
        "v2elmulpar_osp_codehigh": "v2elmulpar_osp_leg_high",
        "v2elmulpar_osp_codelow": "v2elmulpar_osp_leg_low",
    }
    columns_old = list(columns.keys())
    columns_new = list(columns.values())
    tb[columns_new] = tb[columns_old].copy()
    tb.loc[~MASK_LEG, columns_new] = np.nan

    ## Forward fill indicators when there are regularly scheduled national elections on the legislature on course, as stipulated by election law or well-established precedent
    mask = tb["v2xlg_elecreg"] == 1
    tb.loc[mask, columns_new] = tb.groupby(["country"])[columns_new].ffill().loc[mask]
    return tb


def estimate_hos_indicators(tb: Table) -> Table:
    """Create indicators for multi-party head of state elections with imputed values between election-years."""

    def _set_mulpar_hos(tb: Table, column_new: str, column_ex: str, column_leg: str) -> Table:
        # Initialize new column
        tb[column_new] = 0
        tb.loc[tb["v2x_elecreg"].isna(), column_new] = np.nan
        # Define mask
        mask = (
            # Marcus Tannenberg does not know why electoral regime used as filter instead of relative power of heads of state and government as filter, as above; Anna Lührmann wrote this code. Using electoral regime as filter for this and all following variables yields identical coding.
            (
                # If head of state is directly elected, elections for executive must be multi-party.
                (tb["v2expathhs"] == 7) & (tb["v2xex_elecreg"] == 1) & (tb[column_ex] > 1) & (tb[column_ex].notna())
            )
            | (
                # If head of state is appointed by legislature, elections for legislature must be multi-party.
                (tb["v2expathhs"] == 6) & (tb["v2xlg_elecreg"] == 1) & (tb[column_leg] > 1) & (tb[column_leg].notna())
            )
            |
            # It is unclear why v2elmulpar_osp_ex and not v2elmulpar_osp_leg is used, if this is about legislative elections; this seems to be an error, which is why I use the following code instead
            (
                # If head of state is appointed otherwise, but approval by the legislature is necessary, elections for legislature must be multi-party.
                (tb["v2ex_legconhos"] == 1)
                & (tb["v2xlg_elecreg"] == 1)
                & (tb[column_leg] > 1)
                & (tb[column_leg].notna())
            )
        )
        # Set 1 when mask is True
        tb.loc[mask, column_new] = 1

        return tb

    tb = _set_mulpar_hos(tb, "v2elmulpar_osp_hos", "v2elmulpar_osp_ex", "v2elmulpar_osp_leg")
    tb = _set_mulpar_hos(tb, "v2elmulpar_osp_hos_high", "v2elmulpar_osp_ex_high", "v2elmulpar_osp_leg_high")
    tb = _set_mulpar_hos(tb, "v2elmulpar_osp_hos_low", "v2elmulpar_osp_ex_low", "v2elmulpar_osp_leg_low")

    return tb


def estimate_hog_indicators(tb: Table) -> Table:
    """Create indicators for multi-party head of government elections with imputed values between election-years."""
    tb["v2elmulpar_osp_hog"] = 0
    tb.loc[tb["v2x_elecreg"].isna(), "v2elmulpar_osp_hog"] = np.nan

    # Define mask
    mask = (
        (
            # If head of government is directly elected, elections for executive must be multi-party
            (tb["v2expathhs"] == 8)
            & (tb["v2xex_elecreg"] == 1)
            & (tb["v2elmulpar_osp_ex"] > 1)
            & (tb["v2elmulpar_osp_ex"].notna())
        )
        | (
            # If head of government is appointed by legislature, elections for legislature must be multi-party.
            (tb["v2expathhs"] == 7)
            & (tb["v2xex_elecreg"] == 1)
            & (tb["v2elmulpar_osp_leg"] > 1)
            & (tb["v2elmulpar_osp_leg"].notna())
        )
        | (
            # If head of government is appointed by the head of state, elections for the head of state must be multi-party
            (tb["v2expathhs"] == 6) & (tb["v2elmulpar_osp_hos"] == 1)
        )
    )
    tb.loc[mask, "v2elmulpar_osp_hog"] = 1

    ## If head of government is appointed otherwise, but approval by the legislature is necessary, elections for legislature must be multi-party
    tb.loc[
        (tb["v2ex_legconhog"] == 1)
        & (tb["v2xlg_elecreg"] == 1)
        & (tb["v2elmulpar_osp_leg"] > 1)
        & tb["v2elmulpar_osp_leg"].notna(),
        "v2elmulpar_osp_hog",
    ] = 1

    def _set_mulpar_hog(tb: Table, column_new: str, column_ex: str, column_leg: str, column_hos: str) -> Table:
        # Iniitalize new column
        tb[column_new] = 0
        tb.loc[tb["v2x_elecreg"].isna(), column_new] = np.nan
        # Define mask
        mask = (
            (
                # If head of government is directly elected, elections for executive must be multi-party.
                (tb["v2expathhg"] == 8) & (tb["v2xex_elecreg"] == 1) & (tb[column_ex] > 1) & (tb[column_ex].notna())
            )
            | (
                # If head of government is appointed by legislature, elections for legislature must be multi-party.
                (tb["v2expathhs"] == 7) & (tb["v2xlg_elecreg"] == 1) & (tb[column_leg] > 1) & (tb[column_leg].notna())
            )
            | (
                # If head of government is appointed by the head of state, elections for the head of state must be multi-party.
                (tb["v2expathhs"] == 6) & (tb[column_hos] == 1)
            )
            |
            # If head of government is appointed otherwise, but approval by the legislature is necessary, elections for legislature must be multi-party.
            ((tb["v2ex_legconhog"] == 1) & (tb["v2xlg_elecreg"] == 1) & (tb[column_leg] > 1) & (tb[column_leg].notna()))
        )
        # Set 1 when mask is True
        tb.loc[mask, column_new] = 1

        return tb

    tb = _set_mulpar_hog(
        tb, "v2elmulpar_osp_hog_high", "v2elmulpar_osp_ex_high", "v2elmulpar_osp_leg_high", "v2elmulpar_osp_hos_high"
    )
    tb = _set_mulpar_hog(
        tb, "v2elmulpar_osp_hog_low", "v2elmulpar_osp_ex_low", "v2elmulpar_osp_leg_low", "v2elmulpar_osp_hos_low"
    )
    return tb


def estimate_exleg_indicators(tb: Table) -> Table:
    """Create indicators for multi-party executive and legislative elections with imputed values between election-years."""

    def _set_mulpar_exleg(tb: Table, column_new: str, column_ex: str, column_leg: str) -> Table:
        # Iniitalize new column
        tb[column_new] = 0
        tb.loc[tb["v2ex_hosw"].isna(), column_new] = np.nan

        tb.loc[
            (tb["v2xlg_elecreg"] == 1)
            & (tb["v2xex_elecreg"] == 1)
            & (tb[column_ex] > 1)
            & (tb[column_ex].notna())
            & (tb[column_leg] > 1)
            & (tb[column_leg].notna()),
            column_new,
        ] = 1
        return tb

    tb = _set_mulpar_exleg(tb, "v2elmulpar_osp_exleg", "v2elmulpar_osp_ex", "v2elmulpar_osp_leg")
    tb = _set_mulpar_exleg(tb, "v2elmulpar_osp_exleg_high", "v2elmulpar_osp_ex_high", "v2elmulpar_osp_leg_high")
    tb = _set_mulpar_exleg(tb, "v2elmulpar_osp_exleg_low", "v2elmulpar_osp_ex_low", "v2elmulpar_osp_leg_low")

    return tb


def estimate_hoe_indicators(tb: Table) -> Table:
    """Create indicator for multi-party head of executive elections with imputed values between election-years."""

    def _set_mulpar_hoe(tb: Table, column_new: str, column_hos: str, column_hog: str) -> Table:
        # Iniitalize new column
        tb[column_new] = 0
        tb.loc[tb["v2ex_hosw"].isna(), column_new] = np.nan

        # If head of state is more powerful than head of government, head of state is the head of the executive
        tb.loc[(tb["v2ex_hosw"] <= 1) & (tb["v2ex_hosw"] > 0.5), column_new] = tb[column_hos]
        # If head of state is as or less powerful than head of government, head of government is the head of the executive
        tb.loc[tb["v2ex_hosw"] <= 0.5, column_new] = tb[column_hog]

        # Some values of v2ex_hosw are missing, and using v2exhoshog and v2ex_hogw as well improves coverage; Marcus Lührmann agrees with the addition; I therefore add the next two lines
        ## 1st condition: If head of state is also head of government, they are the head of the executive.
        ## 2nd condition: If head of government is less powerful than head of state, head of state must be more powerful than head of government.
        mask = (tb["v2exhoshog"] == 1) | (tb["v2ex_hogw"] == 0)
        tb.loc[mask, column_new] = tb.loc[mask, column_hos]

        return tb

    tb = _set_mulpar_hoe(tb, "v2elmulpar_osp_hoe", "v2elmulpar_osp_hos", "v2elmulpar_osp_hog")
    tb = _set_mulpar_hoe(tb, "v2elmulpar_osp_hoe_high", "v2elmulpar_osp_hos_high", "v2elmulpar_osp_hog_high")
    tb = _set_mulpar_hoe(tb, "v2elmulpar_osp_hoe_low", "v2elmulpar_osp_hos_low", "v2elmulpar_osp_hog_low")

    return tb


def estimate_dichotomous_indicators(tb: Table) -> Table:
    """Create dichotomous indicators for classification criteria."""

    def _set_dich(
        tb: Table,
        column_new: str,
        column_condition: str,
        low: Union[float, None] = None,
        mid: Union[float, None] = None,
        up: Union[float, None] = None,
    ) -> Table:
        tb[column_new] = np.nan

        if up is not None:
            tb.loc[(tb[column_condition] >= low) & (tb[column_condition] <= mid), column_new] = 0
        else:
            tb.loc[tb[column_condition] <= mid, column_new] = 0

        if up is not None:
            tb.loc[(tb[column_condition] > mid) & (tb[column_condition] <= up), column_new] = 1
        else:
            tb.loc[tb[column_condition] > mid, column_new] = 1

        return tb

    # relative to V-Dem/RoW, = added to v2elmulpar_osp_leg < 1, even if v2elmulpar_osp_leg != 1 for all observations, for possible future iterations.
    tb = _set_dich(tb, "v2x_polyarchy_dich", "v2x_polyarchy", 0, 0.5, 1)
    tb = _set_dich(tb, "v2x_polyarchy_high_dich", "v2x_polyarchy_codehigh", 0, 0.5, 1)
    tb = _set_dich(tb, "v2x_polyarchy_low_dich", "v2x_polyarchy_codelow", 0, 0.5, 1)
    # L215
    tb = _set_dich(tb, "v2elfrfair_osp_dich", "v2elfrfair_osp", 0, 2)
    tb = _set_dich(tb, "v2elfrfair_osp_high_dich", "v2elfrfair_osp_high", 0, 2)
    tb = _set_dich(tb, "v2elfrfair_osp_low_dich", "v2elfrfair_osp_low", 0, 2)
    # L227
    tb = _set_dich(tb, "v2elmulpar_osp_dich", "v2elmulpar_osp", 0, 2)
    tb = _set_dich(tb, "v2elmulpar_osp_high_dich", "v2elmulpar_osp_high", 0, 2)
    tb = _set_dich(tb, "v2elmulpar_osp_low_dich", "v2elmulpar_osp_low", 0, 2)
    # L239
    tb = _set_dich(tb, "v2x_liberal_dich", "v2x_liberal", 0, 0.8, 1)
    tb = _set_dich(tb, "v2x_liberal_high_dich", "v2x_liberal_codehigh", 0, 0.8, 1)
    tb = _set_dich(tb, "v2x_liberal_low_dich", "v2x_liberal_codelow", 0, 0.8, 1)
    # L251
    tb = _set_dich(tb, "v2clacjstm_osp_dich", "v2clacjstm_osp", mid=3)
    tb = _set_dich(tb, "v2clacjstm_osp_high_dich", "v2clacjstm_osp_codehigh", mid=3)
    tb = _set_dich(tb, "v2clacjstm_osp_low_dich", "v2clacjstm_osp_codelow", mid=3)
    # L263
    tb = _set_dich(tb, "v2clacjstw_osp_dich", "v2clacjstw_osp", mid=3)
    tb = _set_dich(tb, "v2clacjstw_osp_high_dich", "v2clacjstw_osp_codehigh", mid=3)
    tb = _set_dich(tb, "v2clacjstw_osp_low_dich", "v2clacjstw_osp_codelow", mid=3)
    # L275
    tb = _set_dich(tb, "v2cltrnslw_osp_dich", "v2cltrnslw_osp", mid=3)
    tb = _set_dich(tb, "v2cltrnslw_osp_high_dich", "v2cltrnslw_osp_codehigh", mid=3)
    tb = _set_dich(tb, "v2cltrnslw_osp_low_dich", "v2cltrnslw_osp_codelow", mid=3)
    # L287: relative to V-Dem/RoW, I added v2elmulpar_osp_leg < ., as otherwise v2elmulpar_osp_leg_dich = 1 if v2elmulpar_osp_leg > 1.
    tb = _set_dich(tb, "v2elmulpar_osp_leg_dich", "v2elmulpar_osp_leg", mid=1)
    tb = _set_dich(tb, "v2elmulpar_osp_leg_high_dich", "v2elmulpar_osp_leg_high", mid=1)
    tb = _set_dich(tb, "v2elmulpar_osp_leg_low_dich", "v2elmulpar_osp_leg_low", mid=1)
    return tb


def estimate_row_indicators(tb: Table) -> Table:
    """Create indicators for Regimes of the World with expanded coverage and minor changes to coding."""
    column_new = "regime_row_owid"
    tb[column_new] = np.nan

    # Replace regime_row_owid based on conditions
    tb.loc[
        (tb["v2elfrfair_osp_dich"] == 1)
        & (tb["v2elmulpar_osp_dich"] == 1)
        & (tb["v2x_polyarchy_dich"] == 1)
        & (tb["v2x_liberal_dich"] == 1)
        & (tb["v2clacjstm_osp_dich"] == 1)
        & (tb["v2clacjstw_osp_dich"] == 1)
        & (tb["v2cltrnslw_osp_dich"] == 1),
        column_new,
    ] = 3

    tb.loc[
        (tb["v2elfrfair_osp_dich"] == 1)
        & (tb["v2elmulpar_osp_dich"] == 1)
        & (tb["v2x_polyarchy_dich"] == 1)
        & (
            (tb["v2x_liberal_dich"] == 0)
            | (tb["v2clacjstm_osp_dich"] == 0)
            | (tb["v2clacjstw_osp_dich"] == 0)
            | (tb["v2cltrnslw_osp_dich"] == 0)
        ),
        column_new,
    ] = 2

    tb.loc[
        ((tb["v2elfrfair_osp_dich"] == 0) | (tb["v2elmulpar_osp_dich"] == 0) | (tb["v2x_polyarchy_dich"] == 0))
        & (tb["v2elmulpar_osp_hoe"] == 1)
        & (tb["v2elmulpar_osp_leg_dich"] == 1),
        column_new,
    ] = 1

    tb.loc[
        ((tb["v2elfrfair_osp_dich"] == 0) | (tb["v2elmulpar_osp_dich"] == 0) | (tb["v2x_polyarchy_dich"] == 0))
        & ((tb["v2elmulpar_osp_hoe"] == 0) | (tb["v2elmulpar_osp_leg_dich"] == 0)),
        column_new,
    ] = 0

    # Previous coding rules allow for some observations being coded as electoral democracies even though they have a chief executive who neither meets the criteria for direct or indirect election, nor for being dependent on the legislature.
    # I do not change the coding for these observations because I presume that the criteria for electoral democracy overrule the criteria for distinguishing between electoral and closed autocracies. This also means that I cannot use these criteria alone to code some observations for which only v2x_polyarchy_dich is missing.

    # But: if one criteria for electoral democracy is not met, and one criteria for electoral autocracy is not met, this must mean that the country is a closed autocracy:
    tb.loc[
        ((tb["v2elfrfair_osp_dich"] == 0) | (tb["v2elmulpar_osp_dich"] == 0) | (tb["v2x_polyarchy_dich"] == 0))
        & ((tb["v2elmulpar_osp_hoe"] == 0) | (tb["v2elmulpar_osp_leg_dich"] == 0))
        & (tb[column_new].isna()),
        column_new,
    ] = 0
    # This also means that if one criteria for electoral democracy is not met, yet both criteria for an electoral autocracy is met, it must be an electoral autocracy
    tb.loc[
        ((tb["v2elfrfair_osp_dich"] == 0) | (tb["v2elmulpar_osp_dich"] == 0) | (tb["v2x_polyarchy_dich"] == 0))
        & (tb["v2elmulpar_osp_hoe"] == 1)
        & (tb["v2elmulpar_osp_leg_dich"] == 1)
        & (tb[column_new].isna()),
        column_new,
    ] = 1

    return tb


def compare_with_row_coding(tb: Table) -> Table:
    """Compare our and standard RoW coding."""
    # import pandas as pd
    # filtered_tb = tb[tb['year'] >= 1900]
    # frequency_table = pd.crosstab(index=filtered_tb['regime_row_owid'], columns=filtered_tb['v2x_regime'], dropna=False)

    # 4 Observations are coded differently because v2x_polyarchy in V-Dem's input dataset is barely above 0.5, whereas in the released dataset it is rounded to 0.5 and therefore is not above the coding threshold (conversation with Marcus Tannenberg and Johannes von Römer).
    assert (
        tb.loc[(tb["regime_row_owid"] == 1) & (tb["v2x_regime"] == 2), ["country", "year", "v2x_polyarchy"]].shape[0]
        == 4
    )
    tb.loc[(tb["regime_row_owid"] == 1) & (tb["v2x_regime"] == 2), "regime_row_owid"] = 2

    # No observations own classification identifies as electoral democracies, whereas RoW identifies them as liberal democracies
    assert (
        tb.loc[(tb["regime_row_owid"] == 2) & (tb["v2x_regime"] == 3), ["country", "year", "v2x_polyarchy"]].shape[0]
        == 0
    )

    # 18 observations own classification identifies as closed autocracies, whereas RoW does not provide data
    assert (
        tb.loc[
            (tb["regime_row_owid"] == 0) & (tb["v2x_regime"].isna()) & (tb["year"] > 1900),
            ["country", "year", "v2x_polyarchy"],
        ].shape[0]
        == 18
    )
    # Libya in 1911, 1914, and 1922-1933 can be coded because I use information from v2exhoshog in addition to information from v2ex_hosw to identify head of the executive
    # Honduras in 1934 and 1935, Kazakhstan in 1990, and Turkmenistan in 1990 can be coded because I use information from the other criteria for democracies and autocracies in the absence of information from v2x_polyarchy

    # 13 observations own classification identifies as electoral autocracies, whereas RoW does not provide data
    assert (
        tb.loc[
            (tb["regime_row_owid"] == 1) & (tb["v2x_regime"].isna() & (tb["year"] > 1900)),
            ["country", "year", "v2x_polyarchy"],
        ].shape[0]
        == 13
    )
    # Observations can be coded because I use information from the other criteria for democracies and autocracies in the absence of information from v2x_polyarchy

    # 141 bservations own classification identifies as closed autocracies, whereas RoW identifies them as electoral autocracies
    assert tb.loc[(tb["regime_row_owid"] == 0) & (tb["v2x_regime"] == 1), ["country", "year"]].shape[0] == 141

    # Belgium in 1919 is hard-recoded in RoW code, though Marcus Tannenberg does not know why that happens even if the errors in a previous version of the V-Dem dataset should by now be remedied; it only continues to make a difference for Belgium in 1919; I keep the recode.
    # replace regime_row_owid = 1 if country_name == "Belgium" & year == 1919
    tb.loc[(tb["country"] == "Belgium") & (tb["year"] == 1919), "regime_row_owid"] = 1

    # 111 observations with multi-party elections for legislature and executive (hence the RoW coding); but which had chief executive which were heads of state that were neither directly or indirectly chosen through multiparty elections, nor were they accountable to a legislature chosen through multi-party elections; I therefore do not recode them.
    assert (
        tb.loc[
            (tb["regime_row_owid"] < tb["v2x_regime"])
            & (~tb["v2x_regime"].isna())
            & (tb["v2ex_hosw"] <= 1)
            & (tb["v2ex_hosw"] > 0.5),
            ["country", "year", "v2exnamhos"],
        ].shape[0]
        == 111
    )
    # Examples include many prominent heads of state which came to office in coup d'etats or rebellions, such as Boumedienne (Algeria 1965), Anez (Bolivia 2019), Buyoya (Burundi 1987), Batista (Cuba 1952), Ankrah (Ghana 1966), Khomeini (Iran 1980), Buhari (Nigeria 1983), Jammeh (The Gambia 1994), and Eyadema (1967 Togo)

    # 8 observations which had multi-party elections for legislature and executive (hence the RoW coding); but which had chief executives which were heads of government that were neither directly or indirectly chosen through multiparty elections, nor were they accountable to a legislature chosen through multi-party elections:
    assert (
        tb.loc[
            (tb["regime_row_owid"] == 0)
            & (tb["v2x_regime"] == 1)
            & (tb["v2elmulpar_osp_exleg"] == 1)
            & (tb["v2ex_hosw"] <= 0.5),
            ["country", "year", "v2elmulpar_osp_exleg", "v2expathhg", "v2ex_legconhog", "v2expathhs", "v2ex_legconhos"],
        ].shape[0]
        == 8
    )
    # Examples include prominent heads of government which came to office in a rebellion or were appointed by a foreign power, such as Castro (Cuba 1959)

    # NOTE: 3 -> 21 observations coded differently because I use v2ex_legconhog above for consistency, while RoW uses v2exaphogp instead. I defer to RoW coding in these cases. It may be that their data pipeline uses date-specific data which are superior to the year-end data used here.
    assert (
        tb.loc[
            (tb["regime_row_owid"] == 0)
            & (tb["v2x_regime"] == 1)
            & (tb["v2elmulpar_osp_exleg"] == 0)
            & (tb["v2ex_hosw"] <= 0.5),
            ["country", "year", "v2expathhg", "v2ex_legconhog", "v2exaphogp"],
        ].shape[0]
        == 21
    )
    tb.loc[
        (tb["regime_row_owid"] == 0)
        & (tb["v2x_regime"] == 1)
        & (tb["v2elmulpar_osp_exleg"] == 0)
        & (tb["v2ex_hosw"] <= 0.5),
        "regime_row_owid",
    ] = 1

    # NOTE: 136 -> 270 bservations own classification identifies as electoral autocracies, whereas RoW identifies them as closed autocracies:
    # tb.loc[(tb["regime_row_owid"] == 1) & (tb["v2x_regime"] == 0), ["country", "year"]]

    # NOTE: 130 -> 180 observations with chief executives that were heads of state directly or indirectly elected chief executive and at least moderately multi-party elections for legislative, but which are affected by RoW's different standard filter (2elmulpar_osp_ex instead of v2elmulpar_osp_leg) above:
    # tb.loc[
    #     (tb["regime_row_owid"] == 1) & (tb["v2x_regime"] == 0) & (tb["v2ex_hosw"] <= 1) & (tb["v2ex_hosw"] > 0.5),
    #     ["country", "year", "v2elmulpar_osp_leg_dich", "v2elmulpar_osp_hoe", "v2elmulpar_osp_ex", "v2elmulpar_osp_leg"],
    # ]

    # NOTE: 6 -> 90 observations with chief executives that were heads of government directly or indirectly elected chief executive and at least moderately multi-party elections for legislative, but which are affected by RoW's different standard filter (v2elmulpar_osp instead of v2xlg_elecreg) above:
    # list v2elmulpar_osp_leg v2elmulpar_osp_hoe v2elmulpar_osp v2xlg_elecreg if regime_row_owid == 1 & v2x_regime == 0 & v2ex_hosw <= 0.5
    assert (
        tb.loc[
            (tb["regime_row_owid"] == 1) & (tb["v2x_regime"] == 0) & (tb["v2ex_hosw"] <= 0.5),
            ["v2elmulpar_osp_leg", "v2elmulpar_osp_hoe", "v2elmulpar_osp", "v2xlg_elecreg"],
        ].shape[0]
        == 90
    )

    # 34 observations which RoW identifies as electoral autocracies, but which own classification identifies as missing:
    # All observations have missing values for multi-party legislative elections, sometimes also for free and fair as well as multi-party elections in general. One could say that if v2x_elecreg == 0 — or v2eltype_0/1/4/5 are all zero — this means that were no (multi-party legislative) elections - but this would make these regimes closed autocracies, not electoral autociraces. So this better stay as is.
    assert (
        tb.loc[
            (tb["regime_row_owid"].isna()) & (tb["v2x_regime"] == 1),
            [
                "country",
                "year",
                "v2x_elecreg",
                "v2elfrfair_osp_dich",
                "v2elmulpar_osp_dich",
                "v2x_polyarchy_dich",
                "v2elmulpar_osp_hoe",
                "v2elmulpar_osp_leg_dich",
                "v2eltype_0",
                "v2eltype_1",
                "v2eltype_4",
                "v2eltype_5",
            ],
        ].shape[0]
        == 34
    )

    # 5 observation which RoW identifies as closed autocracy, but which own classification identifies as missing:
    # Slovakia in 1993 had not held legislative elections yet. This can stay as is.
    assert (
        tb.loc[
            (tb["regime_row_owid"].isna()) & (tb["v2x_regime"] == 0),
            [
                "country",
                "year",
                "v2elfrfair_osp_dich",
                "v2elmulpar_osp_dich",
                "v2x_polyarchy_dich",
                "v2elmulpar_osp_hoe",
                "v2elmulpar_osp_leg_dich",
            ],
        ].shape[0]
        == 5
    )

    # 1 observation which RoW identifies as electoral democracy, but which own classification identifies as missing:
    # We impute Australia in 1900 in later script.
    assert (
        tb.loc[
            (tb["regime_row_owid"].isna()) & (tb["v2x_regime"] == 2),
            [
                "country",
                "year",
                "v2elfrfair_osp_dich",
                "v2elmulpar_osp_dich",
                "v2x_polyarchy_dich",
                "v2x_liberal_dich",
                "v2clacjstm_osp_dich",
                "v2clacjstw_osp_dich",
                "v2cltrnslw_osp_dich",
            ],
        ].shape[0]
        == 1
    )

    return tb


def add_regime_amb_row(tb: Table) -> Table:
    """Finalize expanded and refined Regimes of the World indicator with ambiguous categories."""
    tb["regime_amb_row_owid"] = np.nan

    tb.loc[
        (tb["regime_row_owid"] == 3),
        "regime_amb_row_owid",
    ] = 9
    tb.loc[
        (tb["regime_row_owid"] == 3)
        & (
            (tb["v2elfrfair_osp_low_dich"] == 0)
            | (tb["v2elmulpar_osp_low_dich"] == 0)
            | (tb["v2x_polyarchy_low_dich"] == 0)
            | (tb["v2x_liberal_low_dich"] == 0)
            | (tb["v2clacjstm_osp_low_dich"] == 0)
            | (tb["v2clacjstw_osp_low_dich"] == 0)
            | (tb["v2cltrnslw_osp_low_dich"] == 0)
        ),
        "regime_amb_row_owid",
    ] = 8
    tb.loc[
        (tb["regime_row_owid"] == 2),
        "regime_amb_row_owid",
    ] = 6
    tb.loc[
        (tb["regime_row_owid"] == 2)
        & (
            (tb["v2elfrfair_osp_high_dich"] == 1)
            & (tb["v2elmulpar_osp_high_dich"] == 1)
            & (tb["v2x_polyarchy_high_dich"] == 1)
            & (tb["v2x_liberal_high_dich"] == 1)
            & (tb["v2clacjstm_osp_high_dich"] == 1)
            & (tb["v2clacjstw_osp_high_dich"] == 1)
            & (tb["v2cltrnslw_osp_high_dich"] == 1)
        ),
        "regime_amb_row_owid",
    ] = 7
    # replace regime_amb_row_owid = 5 if regime_row_owid == 2 & (v2elfrfair_osp_low_dich == 0 | v2elmulpar_osp_low_dich == 0 | v2x_polyarchy_low_dich == 0)
    tb.loc[
        (tb["regime_row_owid"] == 2)
        & (
            (tb["v2elfrfair_osp_low_dich"] == 0)
            | (tb["v2elmulpar_osp_low_dich"] == 0)
            | (tb["v2x_polyarchy_low_dich"] == 0)
        ),
        "regime_amb_row_owid",
    ] = 5
    tb.loc[
        (tb["regime_row_owid"] == 1),
        "regime_amb_row_owid",
    ] = 3
    tb.loc[
        (tb["regime_row_owid"] == 1)
        & (
            (tb["v2elfrfair_osp_high_dich"] == 1)
            & (tb["v2elmulpar_osp_high_dich"] == 1)
            & (tb["v2x_polyarchy_high_dich"] == 1)
        ),
        "regime_amb_row_owid",
    ] = 4
    tb.loc[
        (tb["regime_row_owid"] == 1) & ((tb["v2elmulpar_osp_hoe_low"] == 0) | (tb["v2elmulpar_osp_leg_low_dich"] == 0)),
        "regime_amb_row_owid",
    ] = 2
    tb.loc[
        (tb["regime_row_owid"] == 0),
        "regime_amb_row_owid",
    ] = 0
    tb.loc[
        (tb["regime_row_owid"] == 0) & (tb["v2elmulpar_osp_hoe_high"] == 1) & (tb["v2elmulpar_osp_leg_high_dich"] == 1),
        "regime_amb_row_owid",
    ] = 1

    # import pandas as pd
    # filtered_tb = tb[tb['year'] >= 1900]
    # filtered_tb = tb[(tb['year'] >= 1900) * (tb["regime_row_owid"] == tb["v2x_regime"])]
    # frequency_table = pd.crosstab(index=filtered_tb['regime_amb_row_owid'], columns=filtered_tb['v2x_regime_amb'], dropna=False)
    return tb


def drop_columns(tb: Table) -> Table:
    """Drop columns that are not of interest."""
    tb = tb.drop(
        columns=[
            "v2x_regime",
            "v2x_regime_amb",
            "v2x_elecreg",
            "v2xex_elecreg",
            "v2xlg_elecreg",
            "v2eltype_0",
            "v2eltype_1",
            "v2eltype_2",
            "v2eltype_3",
            "v2eltype_4",
            "v2eltype_5",
            "v2eltype_6",
            "v2eltype_7",
            "v2eltype_8",
            "v2eltype_9",
            "v2elmulpar_osp",
            "v2elfrfair_osp",
            "v2elmulpar_osp",
            "v2elmulpar_osp_leg",
            "v2elfrfair_osp",
            "v2exnamhos",
            "v2expathhs",
            "v2exnamhog",
            "v2exaphogp",
            "v2expathhg",
            "v2ex_legconhog",
            "v2ex_legconhos",
            "v2elmulpar_osp_ex",
            "v2elmulpar_osp_ex",
            "v2elmulpar_osp_leg",
            "v2elmulpar_osp_hos",
            "v2elmulpar_osp_hog",
            "v2elmulpar_osp_exleg",
            "v2cltrnslw_osp",
            "v2clacjstm_osp",
            "v2clacjstw_osp",
            "v2elmulpar_osp_codehigh",
            "v2elmulpar_osp_codelow",
            "v2elfrfair_osp_codehigh",
            "v2elfrfair_osp_codelow",
            "v2elmulpar_osp_high",
            "v2elmulpar_osp_low",
            "v2elfrfair_osp_high",
            "v2elfrfair_osp_low",
            "v2elmulpar_osp_ex_high",
            "v2elmulpar_osp_ex_high",
            "v2elmulpar_osp_ex_low",
            "v2elmulpar_osp_ex_low",
            "v2elmulpar_osp_leg_high",
            "v2elmulpar_osp_leg_high",
            "v2elmulpar_osp_leg_low",
            "v2elmulpar_osp_leg_low",
            "v2elmulpar_osp_hos_high",
            "v2elmulpar_osp_hos_low",
            "v2elmulpar_osp_hog_high",
            "v2elmulpar_osp_hog_low",
            "v2elmulpar_osp_exleg_high",
            "v2elmulpar_osp_exleg_low",
            "v2cltrnslw_osp_codehigh",
            "v2cltrnslw_osp_codelow",
            "v2clacjstm_osp_codehigh",
            "v2clacjstm_osp_codelow",
            "v2clacjstw_osp_codehigh",
            "v2clacjstw_osp_codelow",
        ]
    )
    return tb


def rename_columns(tb: Table) -> Table:
    """Rename variables of interest."""
    tb = tb.rename(
        columns={
            "v2x_polyarchy_dich": "electdem_dich_row_owid",  # _owid suffix to reflect that I coded the variable slightly differently than Lührmann et al.
            "v2x_polyarchy_high_dich": "electdem_dich_high_row_owid",
            "v2x_polyarchy_low_dich": "electdem_dich_low_row_owid",
            "v2elfrfair_osp_dich": "electfreefair_row",
            "v2elfrfair_osp_high_dich": "electfreefair_high_row",
            "v2elfrfair_osp_low_dich": "electfreefair_low_row",
            "v2elmulpar_osp_dich": "electmulpar_row",
            "v2elmulpar_osp_high_dich": "electmulpar_high_row",
            "v2elmulpar_osp_low_dich": "electmulpar_low_row",
            "v2x_liberal_dich": "lib_dich_row",
            "v2x_liberal_high_dich": "lib_dich_high_row",
            "v2x_liberal_low_dich": "lib_dich_low_row",
            "v2clacjstm_osp_dich": "accessjust_m_row",
            "v2clacjstm_osp_high_dich": "accessjust_m_high_row",
            "v2clacjstm_osp_low_dich": "accessjust_m_low_row",
            "v2clacjstw_osp_dich": "accessjust_w_row",
            "v2clacjstw_osp_high_dich": "accessjust_w_high_row",
            "v2clacjstw_osp_low_dich": "accessjust_w_low_row",
            "v2cltrnslw_osp_dich": "transplaws_row",
            "v2cltrnslw_osp_high_dich": "transplaws_high_row",
            "v2cltrnslw_osp_low_dich": "transplaws_low_row",
            "v2elmulpar_osp_hoe": "electmulpar_hoe_row_owid",  # _owid suffix to reflect that I coded the variable slightly differently than Lührmann et al.
            "v2elmulpar_osp_hoe_high": "electmulpar_hoe_high_row_owid",
            "v2elmulpar_osp_hoe_low": "electmulpar_hoe_low_row_owid",
            "v2elmulpar_osp_leg_dich": "electmulpar_leg_row",
            "v2elmulpar_osp_leg_high_dich": "electmulpar_leg_high_row",
            "v2elmulpar_osp_leg_low_dich": "electmulpar_leg_low_row",
            "v2x_polyarchy": "electdem_vdem",
            "v2x_polyarchy_codelow": "electdem_vdem_low",
            "v2x_polyarchy_codehigh": "electdem_vdem_high",
            "v2x_elecoff": "electoff_vdem",
            "v2xel_frefair": "electfreefair_vdem",
            "v2xel_frefair_codelow": "electfreefair_vdem_low",
            "v2xel_frefair_codehigh": "electfreefair_vdem_high",
            "v2x_frassoc_thick": "freeassoc_vdem",
            "v2x_frassoc_thick_codelow": "freeassoc_vdem_low",
            "v2x_frassoc_thick_codehigh": "freeassoc_vdem_high",
            "v2x_suffr": "suffr_vdem",
            "v2x_freexp_altinf": "freeexpr_vdem",
            "v2x_freexp_altinf_codelow": "freeexpr_vdem_low",
            "v2x_freexp_altinf_codehigh": "freeexpr_vdem_high",
            "v2x_libdem": "libdem_vdem",
            "v2x_libdem_codelow": "libdem_vdem_low",
            "v2x_libdem_codehigh": "libdem_vdem_high",
            "v2x_liberal": "lib_vdem",
            "v2x_liberal_codelow": "lib_vdem_low",
            "v2x_liberal_codehigh": "lib_vdem_high",
            "v2xcl_rol": "indiv_libs_vdem",
            "v2xcl_rol_codelow": "indiv_libs_vdem_low",
            "v2xcl_rol_codehigh": "indiv_libs_vdem_high",
            "v2x_jucon": "judicial_constr_vdem",
            "v2x_jucon_codelow": "judicial_constr_vdem_low",
            "v2x_jucon_codehigh": "judicial_constr_vdem_high",
            "v2xlg_legcon": "legis_constr_vdem",
            "v2xlg_legcon_codelow": "legis_constr_vdem_low",
            "v2xlg_legcon_codehigh": "legis_constr_vdem_high",
            "v2x_partipdem": "participdem_vdem",
            "v2x_partipdem_codelow": "participdem_vdem_low",
            "v2x_partipdem_codehigh": "participdem_vdem_high",
            "v2x_partip": "particip_vdem",
            "v2x_partip_codelow": "particip_vdem_low",
            "v2x_partip_codehigh": "particip_vdem_high",
            "v2x_cspart": "civsoc_particip_vdem",
            "v2x_cspart_codelow": "civsoc_particip_vdem_low",
            "v2x_cspart_codehigh": "civsoc_particip_vdem_high",
            "v2xdd_dd": "dirpop_vote_vdem",
            "v2xel_locelec": "locelect_vdem",
            "v2xel_locelec_codelow": "locelect_vdem_low",
            "v2xel_locelec_codehigh": "locelect_vdem_high",
            "v2xel_regelec": "regelect_vdem",
            "v2xel_regelec_codelow": "regelect_vdem_low",
            "v2xel_regelec_codehigh": "regelect_vdem_high",
            "v2x_delibdem": "delibdem_vdem",
            "v2x_delibdem_codelow": "delibdem_vdem_low",
            "v2x_delibdem_codehigh": "delibdem_vdem_high",
            "v2xdl_delib": "delib_vdem",
            "v2xdl_delib_codelow": "delib_vdem_low",
            "v2xdl_delib_codehigh": "delib_vdem_high",
            "v2dlreason": "justified_polch_vdem",
            "v2dlreason_codelow": "justified_polch_vdem_low",
            "v2dlreason_codehigh": "justified_polch_vdem_high",
            "v2dlcommon": "justcomgd_polch_vdem",
            "v2dlcommon_codelow": "justcomgd_polch_vdem_low",
            "v2dlcommon_codehigh": "justcomgd_polch_vdem_high",
            "v2dlcountr": "counterarg_polch_vdem",
            "v2dlcountr_codelow": "counterarg_polch_vdem_low",
            "v2dlcountr_codehigh": "counterarg_polch_vdem_high",
            "v2dlconslt": "elitecons_polch_vdem",
            "v2dlconslt_codelow": "elitecons_polch_vdem_low",
            "v2dlconslt_codehigh": "elitecons_polch_vdem_high",
            "v2dlengage": "soccons_polch_vdem",
            "v2dlengage_codelow": "soccons_polch_vdem_low",
            "v2dlengage_codehigh": "soccons_polch_vdem_high",
            "v2x_egaldem": "egaldem_vdem",
            "v2x_egaldem_codelow": "egaldem_vdem_low",
            "v2x_egaldem_codehigh": "egaldem_vdem_high",
            "v2x_egal": "egal_vdem",
            "v2x_egal_codelow": "egal_vdem_low",
            "v2x_egal_codehigh": "egal_vdem_high",
            "v2xeg_eqprotec": "equal_rights_vdem",
            "v2xeg_eqprotec_codelow": "equal_rights_vdem_low",
            "v2xeg_eqprotec_codehigh": "equal_rights_vdem_high",
            "v2xeg_eqaccess": "equal_access_vdem",
            "v2xeg_eqaccess_codelow": "equal_access_vdem_low",
            "v2xeg_eqaccess_codehigh": "equal_access_vdem_high",
            "v2xeg_eqdr": "equal_res_vdem",
            "v2xeg_eqdr_codelow": "equal_res_vdem_low",
            "v2xeg_eqdr_codehigh": "equal_res_vdem_high",
            "v2x_civlib": "civ_libs_vdem",
            "v2x_civlib_codelow": "civ_libs_vdem_low",
            "v2x_civlib_codehigh": "civ_libs_vdem_high",
            "v2x_clphy": "phys_integr_libs_vdem",
            "v2x_clphy_codehigh": "phys_integr_libs_vdem_high",
            "v2x_clphy_codelow": "phys_integr_libs_vdem_low",
            "v2x_clpol": "pol_libs_vdem",
            "v2x_clpol_codehigh": "pol_libs_vdem_high",
            "v2x_clpol_codelow": "pol_libs_vdem_low",
            "v2x_clpriv": "priv_libs_vdem",
            "v2x_clpriv_codehigh": "priv_libs_vdem_high",
            "v2x_clpriv_codelow": "priv_libs_vdem_low",
            "v2x_gender": "wom_emp_vdem",
            "v2x_gender_codehigh": "wom_emp_vdem_high",
            "v2x_gender_codelow": "wom_emp_vdem_low",
            "v2x_gencl": "wom_civ_libs_vdem",
            "v2x_gencl_codehigh": "wom_civ_libs_vdem_high",
            "v2x_gencl_codelow": "wom_civ_libs_vdem_low",
            "v2x_gencs": "wom_civ_soc_vdem",
            "v2x_gencs_codehigh": "wom_civ_soc_vdem_high",
            "v2x_gencs_codelow": "wom_civ_soc_vdem_low",
            "v2x_genpp": "wom_pol_par_vdem",
            "v2x_genpp_codehigh": "wom_pol_par_vdem_high",
            "v2x_genpp_codelow": "wom_pol_par_vdem_low",
            "v2lgfemleg": "wom_parl_vdem",
            "v2exfemhos": "wom_hos_vdem",
            "v2exfemhog": "wom_hog_vdem",
            "v2clsocgrp": "socgr_civ_libs_vdem",
            "v2clsocgrp_codehigh": "socgr_civ_libs_vdem_high",
            "v2clsocgrp_codelow": "socgr_civ_libs_vdem_low",
            "v2pepwrsoc": "socgr_pow_vdem",
            "v2pepwrsoc_codehigh": "socgr_pow_vdem_high",
            "v2pepwrsoc_codelow": "socgr_pow_vdem_low",
            "v2svstterr": "terr_contr_vdem",
            "v2svstterr_codehigh": "terr_contr_vdem_high",
            "v2svstterr_codelow": "terr_contr_vdem_low",
            "v2x_rule": "rule_of_law_vdem",
            "v2x_rule_codehigh": "rule_of_law_vdem_high",
            "v2x_rule_codelow": "rule_of_law_vdem_low",
            "v2clrspct": "public_admin_vdem",
            "v2clrspct_codehigh": "public_admin_vdem_high",
            "v2clrspct_codelow": "public_admin_vdem_low",
            "v2svinlaut": "int_auton_vdem",
            "v2svinlaut_codehigh": "int_auton_vdem_high",
            "v2svinlaut_codelow": "int_auton_vdem_low",
            "v2svdomaut": "dom_auton_vdem",
            "v2svdomaut_codehigh": "dom_auton_vdem_high",
            "v2svdomaut_codelow": "dom_auton_vdem_low",
            "v2x_corr": "corruption_vdem",
            "v2x_corr_codehigh": "corruption_vdem_high",
            "v2x_corr_codelow": "corruption_vdem_low",
            "v2x_pubcorr": "corr_publsec_vdem",
            "v2x_pubcorr_codehigh": "corr_publsec_vdem_high",
            "v2x_pubcorr_codelow": "corr_publsec_vdem_low",
            "v2x_execorr": "corr_exec_vdem",
            "v2x_execorr_codehigh": "corr_exec_vdem_high",
            "v2x_execorr_codelow": "corr_exec_vdem_low",
            "v2lgcrrpt": "corr_leg_vdem",
            "v2lgcrrpt_codehigh": "corr_leg_vdem_high",
            "v2lgcrrpt_codelow": "corr_leg_vdem_low",
            "v2jucorrdc": "corr_jud_vdem",
            "v2jucorrdc_codehigh": "corr_jud_vdem_high",
            "v2jucorrdc_codelow": "corr_jud_vdem_low",
            "e_ti_cpi": "corruption_cpi",
            "v2xnp_pres": "personalism_vdem",
            "v2xnp_pres_codehigh": "personalism_vdem_high",
            "v2xnp_pres_codelow": "personalism_vdem_low",
            "v2xcs_ccsi": "civ_soc_str_vdem",
            "v2xcs_ccsi_codehigh": "civ_soc_str_vdem_high",
            "v2xcs_ccsi_codelow": "civ_soc_str_vdem_low",
            "v2eltrnout": "turnout_vdem",
            "e_wbgi_gee": "goveffective_vdem_wbgi",
        }
    )
    return tb


def standardise_country_names(tb: Table) -> Table:
    """Standardise country names.

    TODO: This should be done by standard function!
    """
    # Replace values in 'country_name' column based on conditions
    tb["country"] = tb["country"].replace(
        {
            "Burma/Myanmar": "Myanmar",
            "Democratic Republic of the Congo": "Democratic Republic of Congo",
            "Ivory Coast": "Cote d'Ivoire",
            "Republic of the Congo": "Congo",
            "The Gambia": "Gambia",
            "Palestine/British Mandate": "Palestine",
            "Timor-Leste": "East Timor",
            "United States of America": "United States",
            "Würtemberg": "Kingdom of Wurttemberg",
            "Czech Republic": "Czechia",
            "German Democratic Republic": "East Germany",
            "Hesse-Kassel": "Hesse Electoral",
            "Hesse-Darmstadt": "Hesse Grand Ducal",
            "South Yemen": "Yemen People's Republic",
            # Nassau?
        }
    )
    return tb


def estimate_gender_hoe_indicator(tb: Table) -> Table:
    """Create variable identifying gender of chief executive."""
    tb["wom_hoe_vdem"] = np.nan
    # If head of state is more powerful than head of government, and head of state is the head of the executive, then update wom_hoe_vdem accordingly
    tb.loc[(tb["v2ex_hosw"] <= 1) & (tb["v2ex_hosw"] > 0.5) & (tb["wom_hos_vdem"].notna()), "wom_hoe_vdem"] = tb[
        "wom_hos_vdem"
    ]
    # Update wom_hoe_vdem based on the power of head of state relative to head of government
    tb.loc[(tb["v2ex_hosw"] <= 0.5) & (tb["wom_hog_vdem"].notna()), "wom_hoe_vdem"] = tb["wom_hog_vdem"]
    # If head of state is also head of government, they are the head of the executive.
    tb.loc[(tb["v2exhoshog"] == 1) & (tb["wom_hos_vdem"].notna()), "wom_hoe_vdem"] = tb["wom_hos_vdem"]
    # If head of government is less powerful than head of state, head of state must be more powerful than head of government.
    tb.loc[(tb["v2ex_hogw"] == 0) & (tb["wom_hos_vdem"].notna()), "wom_hoe_vdem"] = tb["wom_hos_vdem"]

    # Drop columns
    tb = tb.drop(
        columns=[
            "v2ex_hosw",
            "v2ex_hogw",
        ]
    )
    return tb
