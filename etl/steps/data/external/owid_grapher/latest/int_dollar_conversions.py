from enum import Enum

import pandas as pd
from owid.catalog import Dataset
from pandas import DataFrame, Series

from etl.helpers import PathFinder

paths = PathFinder(__file__)

PPP_VERSION = 2021
TARGET_YEAR = 2021

# Maximum deviation that's allowed between the PPP values from PIP and WDI, as a factor. When it's between these limits, we take the PIP value.
PPP_PIP_WDI_MAX_DEVIATION = 0.03


class Source(Enum):
    NONE = "none"
    WDI = "wdi"
    PIP = "pip"
    UNCLEAR = "unclear"  # when it's not clear which source is better, due to large deviations and no clear pattern in the deviations. drop these rows for now, but we might want to review them in the future and decide on a case-by-case basis which source to use.


PPP_ADDITIONAL_COUNTRIES_TO_EXCLUDE = {
    # Countries for which we want to drop the PPP value, even if the sources don't deviate much.
    "Bulgaria": Source.UNCLEAR,  # Bulgaria has introduced the Euro in 2025. Both sources still seem to have data in the Bulgarian lev.
}

PPP_DEVIATIONS_SOURCE_TO_USE = {
    "Afghanistan": Source.WDI,  # only defined by WDI
    "American Samoa": Source.WDI,  # only defined by WDI
    "Andorra": Source.WDI,  # only defined by WDI
    "Antigua and Barbuda": Source.WDI,  # only defined by WDI
    "Argentina": Source.WDI,  # only defined by WDI, PIP only has urban population
    "Argentina (urban)": Source.NONE,  # prefer WDI's value for Argentina, as PIP only has urban population
    "Aruba": Source.WDI,  # only defined by WDI
    "Bahamas": Source.WDI,  # only defined by WDI
    "Bahrain": Source.WDI,  # only defined by WDI
    "Belarus": Source.WDI,  # The Belarusian ruble has been redenominated in 2016, at a rate of 1 BYN = 10,000 BYR. WDI reflects this change.
    "Bermuda": Source.WDI,  # only defined by WDI
    "British Virgin Islands": Source.WDI,  # only defined by WDI
    "Brunei": Source.WDI,  # only defined by WDI
    "Cambodia": Source.WDI,  # only defined by WDI
    "Cayman Islands": Source.WDI,  # only defined by WDI
    "China": Source.WDI,  # only defined by WDI, PIP only has urban & rural population
    "China (rural)": Source.NONE,  # prefer WDI's value for China, as PIP only has urban & rural population
    "China (urban)": Source.NONE,  # prefer WDI's value for China, as PIP only has urban & rural population
    "Croatia": Source.WDI,  # Croatia has introduced the Euro in 2023. WDI reflects this change.
    "Curacao": Source.UNCLEAR,  # Curacao has had a currency change (from Antillean Guilder to Caribbean Guilder) in 2025, and it's unclear which currency WDI's data is given in.
    "Czechia": Source.UNCLEAR,  # deviation of 8%
    "Dominica": Source.WDI,  # only defined by WDI
    "East Timor": Source.UNCLEAR,  # deviation of 26%
    "Egypt": Source.UNCLEAR,  # deviation of 23%
    "Eritrea": Source.WDI,  # only defined by WDI
    "Faroe Islands": Source.WDI,  # only defined by WDI
    "France": Source.PIP,  # deviation of 3.2%, it seems fine to rely on PIP here
    "French Polynesia": Source.WDI,  # only defined by WDI
    "Greenland": Source.WDI,  # only defined by WDI
    "Guam": Source.WDI,  # only defined by WDI
    "Guinea": Source.UNCLEAR,  # deviation of 8%
    "Hong Kong": Source.WDI,  # only defined by WDI
    "Kuwait": Source.WDI,  # only defined by WDI
    "Latvia": Source.UNCLEAR,  # deviation of 5%
    "Liberia": Source.UNCLEAR,  # deviation of 166x; it seems WDI's value is assuming USD as the currency, and PIP is assuming Liberian dollars.
    "Libya": Source.WDI,  # only defined by WDI
    "Macao": Source.WDI,  # only defined by WDI
    "Malta": Source.PIP,  # deviation of 3.5%, it seems fine to rely on PIP here
    "Mauritania": Source.WDI,  # The Mauritanian ouguiya has been redenominated in 2017, at a rate of 1 MRU = 10 MRO. WDI reflects this change, while PIP doesn't.
    "Nauru": Source.UNCLEAR,  # deviation of 17%
    "New Caledonia": Source.WDI,  # only defined by WDI
    "New Zealand": Source.WDI,  # only defined by WDI
    "Northern Mariana Islands": Source.WDI,  # only defined by WDI
    "Oman": Source.WDI,  # only defined by WDI
    "Palau": Source.WDI,  # only defined by WDI
    "Palestine": Source.UNCLEAR,  # deviation of 3.23x; Palestine doesn't have it own sovereign currency, and so it seems likely that the two values are given in different LCU units.
    "Puerto Rico": Source.WDI,  # only defined by WDI
    "Saint Kitts and Nevis": Source.WDI,  # only defined by WDI
    "Saint Vincent and the Grenadines": Source.WDI,  # only defined by WDI
    "San Marino": Source.WDI,  # only defined by WDI
    "Sao Tome and Principe": Source.UNCLEAR,  # deviation of 10%
    "Saudi Arabia": Source.WDI,  # only defined by WDI
    "Sierra Leone": Source.WDI,  # The Sierra Leonean leone has been redenominated in 2022, at a rate of 1 SLE = 1000 SLL. WDI reflects this change, while PIP doesn't.
    "Singapore": Source.WDI,  # only defined by WDI
    "Sint Maarten (Dutch part)": Source.UNCLEAR,  # Sint Maarten has had a currency change (from Antillean Guilder to Caribbean Guilder) in 2025, and it's unclear which currency WDI's data is given in.
    "Somalia": Source.WDI,  # only defined by WDI
    "Sudan": Source.UNCLEAR,  # deviation of 18%
    "Taiwan": Source.PIP,  # only defined by PIP
    "Turks and Caicos Islands": Source.WDI,  # only defined by WDI
    "United States Virgin Islands": Source.WDI,  # only defined by WDI
    "Yemen": Source.PIP,  # only defined by PIP
    "Zimbabwe": Source.UNCLEAR,  # deviation of 21x; Zimbabwe has had a multitude of currencies, and it's not clear which one the sources are using.
}


def load_and_choose_ppp_data(ds_wdi: Dataset, ds_pip: Dataset) -> DataFrame:
    # PPP conversion factor, private consumption (LCU per international $)
    tb_wdi_ppp = (
        ds_wdi["wdi"]
        .query("year == @TARGET_YEAR and pa_nus_prvt_pp.notna()")
        .reset_index()
        .set_index("country")["pa_nus_prvt_pp"]
    )
    # Purchasing Power Parity (PPP) rates (2021 prices)
    tb_pip_ppp = (
        ds_pip["world_bank_pip"]
        .query("ppp_version == @PPP_VERSION and year == @TARGET_YEAR and ppp.notna()")
        .reset_index()
        .groupby("country")[
            "ppp"
        ]  # The PPP value is the same for all rows of a given country, so we can just take the minimum.
        .min()
    )

    tb_joined = tb_pip_ppp.to_frame("pip_ppp").join(tb_wdi_ppp.to_frame("wdi_ppp"), how="outer")
    tb_joined["ppp_factor"] = tb_joined["wdi_ppp"] / tb_joined["pip_ppp"]

    indexes_within_deviation = tb_joined["ppp_factor"].between(
        1 - PPP_PIP_WDI_MAX_DEVIATION, 1 + PPP_PIP_WDI_MAX_DEVIATION
    )

    # Use PIP value where deviation is within limits, otherwise use WDI value
    tb_joined["ppp"] = Series(index=tb_joined.index, dtype="float")
    tb_joined.loc[indexes_within_deviation, "ppp"] = tb_joined.loc[indexes_within_deviation, "pip_ppp"]

    countries_with_deviations = set(tb_joined[tb_joined["ppp"].isna()].index.tolist())

    set_diff_countries_minus_sources_mapping = countries_with_deviations.difference(PPP_DEVIATIONS_SOURCE_TO_USE.keys())
    set_diff_sources_mapping_minus_countries = set(PPP_DEVIATIONS_SOURCE_TO_USE.keys()).difference(
        countries_with_deviations
    )

    assert (
        len(set_diff_countries_minus_sources_mapping) == 0
    ), f"There are some countries with deviations (>={PPP_PIP_WDI_MAX_DEVIATION * 100}%) that are not in the PPP_DEVIATIONS_SOURCE_TO_USE list: {set_diff_countries_minus_sources_mapping}. Please check the values and update the list accordingly."

    assert (
        len(set_diff_sources_mapping_minus_countries) == 0
    ), f"There are some countries in the PPP_DEVIATIONS_SOURCE_TO_USE list that don't have deviations (>={PPP_PIP_WDI_MAX_DEVIATION * 100}%): {set_diff_sources_mapping_minus_countries}. Please check the values and update the list accordingly."

    # Apply the manually chosen source for deviating countries
    for country, source in PPP_DEVIATIONS_SOURCE_TO_USE.items():
        if source == Source.WDI:
            tb_joined.loc[country, "ppp"] = tb_joined.loc[country, "wdi_ppp"]
            assert pd.notna(
                tb_joined.loc[country, "ppp"]
            ), f"WDI PPP value for {country} is NaN, but it was chosen as the source for this country."
        elif source == Source.PIP:
            tb_joined.loc[country, "ppp"] = tb_joined.loc[country, "pip_ppp"]
        # Source.NONE and Source.UNCLEAR remain NaN

    # Drop countries that should be excluded additionally
    for country, source in PPP_ADDITIONAL_COUNTRIES_TO_EXCLUDE.items():
        assert (
            country in tb_joined.index
        ), f"{country} is in the PPP_ADDITIONAL_COUNTRIES_TO_EXCLUDE list, but it's not in the joined PPP table. Please check the country name and update the list accordingly."
        tb_joined.loc[country, "ppp"] = float("nan")

    tb_joined = tb_joined[["ppp"]].dropna()

    return tb_joined


def load_cpi_data(ds_wdi: Dataset) -> DataFrame:
    # Consumer price index (2010 = 100) — keep TARGET_YEAR and the latest available year per country
    tb_cpi_all = (
        ds_wdi["wdi"]
        .query("year >= @TARGET_YEAR and fp_cpi_totl.notna()")
        .reset_index()[["country", "year", "fp_cpi_totl"]]
        .rename(columns={"fp_cpi_totl": "cpi", "year": "cpi_year"})
    )

    # Get data for the target year
    tb_cpi_base = tb_cpi_all.query("cpi_year == @TARGET_YEAR").set_index("country")[["cpi_year", "cpi"]]
    # Get data for the latest available year per country
    tb_cpi_latest = tb_cpi_all.loc[tb_cpi_all.groupby("country")["cpi_year"].idxmax()].set_index("country")[
        ["cpi_year", "cpi"]
    ]

    tb_wdi_cpi = tb_cpi_base.join(tb_cpi_latest, how="inner", lsuffix="_base", rsuffix="_latest")

    # Factor between cpi_year_base and cpi_year_latest
    tb_wdi_cpi["cpi_factor"] = tb_wdi_cpi["cpi_latest"] / tb_wdi_cpi["cpi_base"]
    return tb_wdi_cpi


def run() -> None:
    # WDI indicators
    ds_wdi = paths.load_dataset("wdi")
    # World Bank PIP – poverty (income or consumption consolidated, no spells)
    ds_pip = paths.load_dataset("world_bank_pip")

    tb_ppp = load_and_choose_ppp_data(ds_wdi, ds_pip)
    tb_wdi_cpi = load_cpi_data(ds_wdi)


run()
