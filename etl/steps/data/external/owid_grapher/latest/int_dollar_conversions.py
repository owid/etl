"""Computes "exchange rates" to convert from international dollars in a given year (PPP_YEAR) to local currency units (LCU) in the latest available year, for all countries. The conversion factor is computed as the product of:
1. PPP conversion factor (LCU per international $) for the target year, and
2. CPI adjustment factor (CPI[latest year] / CPI[PPP_YEAR]) to adjust for inflation between the target year and the latest available year.

The PPP conversion factor is taken from the World Bank's PIP dataset or from the World Bank WDI dataset, depending on which source is available and more realistic for each country. The CPI data is taken from the World Bank WDI dataset.

"""

from enum import Enum

import numpy as np
from owid.catalog import Dataset
from pandas import DataFrame

from etl.helpers import PathFinder

paths = PathFinder(__file__)

PPP_YEAR = 2021

# Maximum deviation that's allowed between the PPP values from PIP and WDI, as a factor. When it's between these limits, we take the PIP value.
PPP_PIP_WDI_MAX_DEVIATION = 0.03


class Source(Enum):
    NONE = "none"
    WDI = "wdi"
    PIP = "pip"
    UNCLEAR = "unclear"  # when it's not clear which source is better, due to large deviations or other issues, like recent currency changes or an unclear currency situation. drop these rows for now, but we might want to review them in the future and decide on a case-by-case basis which source to use.


PPP_ADDITIONAL_COUNTRIES_TO_EXCLUDE = {
    # Countries for which we want to drop the PPP value, even if the sources don't deviate much.
    "Bulgaria": Source.UNCLEAR,  # Bulgaria has introduced the Euro in 2025. Both sources still seem to have data in the Bulgarian lev.
}

# For each case where the values from PIP and WDI don't line up (either because one has data but not the other, or because their values deviate by more than PPP_PIP_WDI_MAX_DEVIATION), we manually choose which source to use, or whether to exclude the country altogether.
PPP_DEVIATIONS_SOURCE_TO_USE = {
    "Afghanistan": Source.WDI,  # only defined by WDI
    "American Samoa": Source.WDI,  # only defined by WDI
    "Andorra": Source.WDI,  # only defined by WDI
    "Antigua and Barbuda": Source.WDI,  # only defined by WDI
    "Argentina (urban)": Source.NONE,  # prefer WDI's value for Argentina, as PIP only has urban population
    "Argentina": Source.WDI,  # only defined by WDI, PIP only has urban population
    "Aruba": Source.WDI,  # only defined by WDI
    "Bahamas": Source.WDI,  # only defined by WDI
    "Bahrain": Source.WDI,  # only defined by WDI
    "Belarus": Source.WDI,  # The Belarusian ruble has been redenominated in 2016, at a rate of 1 BYN = 10,000 BYR. WDI reflects this change.
    "Bermuda": Source.WDI,  # only defined by WDI
    "Bolivia (urban)": Source.NONE,  # We don't want urban/rural splits.
    "British Virgin Islands": Source.WDI,  # only defined by WDI
    "Brunei": Source.WDI,  # only defined by WDI
    "Cambodia": Source.WDI,  # only defined by WDI
    "Cayman Islands": Source.WDI,  # only defined by WDI
    "China (rural)": Source.NONE,  # prefer WDI's value for China, as PIP only has urban & rural population
    "China (urban)": Source.NONE,  # prefer WDI's value for China, as PIP only has urban & rural population
    "China": Source.WDI,  # only defined by WDI, PIP only has urban & rural population
    "Colombia (urban)": Source.NONE,  # We don't want urban/rural splits.
    "Croatia": Source.WDI,  # Croatia has introduced the Euro in 2023. WDI reflects this change.
    "Curacao": Source.UNCLEAR,  # Curacao has had a currency change (from Antillean Guilder to Caribbean Guilder) in 2025, and it's unclear which currency WDI's data is given in.
    "Czechia": Source.UNCLEAR,  # deviation of 8%
    "Dominica": Source.WDI,  # only defined by WDI
    "East Timor": Source.UNCLEAR,  # deviation of 26%
    "Ecuador (urban)": Source.NONE,  # We don't want urban/rural splits.
    "Egypt": Source.UNCLEAR,  # deviation of 23%
    "Eritrea": Source.WDI,  # only defined by WDI
    "Ethiopia (rural)": Source.NONE,  # We don't want urban/rural splits.
    "Faroe Islands": Source.WDI,  # only defined by WDI
    "France": Source.PIP,  # deviation of 3.2%, it seems fine to rely on PIP here
    "French Polynesia": Source.WDI,  # only defined by WDI
    "Greenland": Source.WDI,  # only defined by WDI
    "Guam": Source.WDI,  # only defined by WDI
    "Guinea": Source.UNCLEAR,  # deviation of 8%
    "Honduras (urban)": Source.NONE,  # We don't want urban/rural splits.
    "Hong Kong": Source.WDI,  # only defined by WDI
    "Kuwait": Source.WDI,  # only defined by WDI
    "Latvia": Source.UNCLEAR,  # deviation of 5%
    "Liberia": Source.UNCLEAR,  # deviation of 166x; it seems WDI's value is assuming USD as the currency, and PIP is assuming Liberian dollars.
    "Libya": Source.WDI,  # only defined by WDI
    "Macao": Source.WDI,  # only defined by WDI
    "Malta": Source.PIP,  # deviation of 3.5%, it seems fine to rely on PIP here
    "Mauritania": Source.WDI,  # The Mauritanian ouguiya has been redenominated in 2017, at a rate of 1 MRU = 10 MRO. WDI reflects this change, while PIP doesn't.
    "Micronesia (country) (urban)": Source.NONE,  # We don't want urban/rural splits.
    "Nauru": Source.UNCLEAR,  # deviation of 17%
    "New Caledonia": Source.WDI,  # only defined by WDI
    "New Zealand": Source.WDI,  # only defined by WDI
    "Northern Mariana Islands": Source.WDI,  # only defined by WDI
    "Oman": Source.WDI,  # only defined by WDI
    "Palau": Source.WDI,  # only defined by WDI
    "Palestine": Source.UNCLEAR,  # deviation of 3.23x; Palestine doesn't have it own sovereign currency, and so it seems likely that the two values are given in different LCU units.
    "Puerto Rico": Source.WDI,  # only defined by WDI
    "Rwanda (rural)": Source.NONE,  # We don't want urban/rural splits.
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
    "Suriname (urban)": Source.NONE,  # We don't want urban/rural splits.
    "Taiwan": Source.PIP,  # only defined by PIP
    "Turks and Caicos Islands": Source.WDI,  # only defined by WDI
    "United States Virgin Islands": Source.WDI,  # only defined by WDI
    "Uruguay (urban)": Source.NONE,  # We don't want urban/rural splits.
    "Venezuela": Source.PIP,  # only defined by PIP
    "Yemen": Source.PIP,  # only defined by PIP
    "Zimbabwe": Source.UNCLEAR,  # deviation of 21x; Zimbabwe has had a multitude of currencies, and it's not clear which one the sources are using.
}


def load_and_reconcile_ppp_data(ds_wdi: Dataset, ds_pip: Dataset) -> DataFrame:
    # PPP conversion factor, private consumption (LCU per international $)
    tb_wdi_ppp = (
        ds_wdi["wdi"]
        .query("year == @PPP_YEAR and pa_nus_prvt_pp.notna()")
        .reset_index()
        .set_index("country")["pa_nus_prvt_pp"]
    )

    # Purchasing Power Parity (PPP) rates (2021 prices)
    tb_pip_ppp = (
        ds_pip["other_indicators"]
        .rename(columns={"ppp__ppp_version_2021__welfare_type_income_or_consumption": "ppp"})
        .reset_index()
        .groupby("country")[
            "ppp"
        ]  # The PPP value is the same for all rows of a given country, so we can just take the minimum.
        .min()
        .dropna()
    )

    tb_joined = tb_pip_ppp.to_frame("pip_ppp").join(tb_wdi_ppp.to_frame("wdi_ppp"), how="outer")
    tb_joined["ppp_factor"] = tb_joined["wdi_ppp"] / tb_joined["pip_ppp"]

    within_deviation = (
        tb_joined["ppp_factor"].between(1 - PPP_PIP_WDI_MAX_DEVIATION, 1 + PPP_PIP_WDI_MAX_DEVIATION).fillna(False)
    )

    # Countries within deviation limits get PIP as the source; the rest need manual resolution.
    tb_joined["ppp_source"] = np.where(within_deviation, "pip", None)

    # Validate that the manual override list exactly matches the set of deviating countries.
    countries_with_deviations = set(tb_joined[tb_joined["ppp_source"] != "pip"].index)
    missing_from_overrides = countries_with_deviations - PPP_DEVIATIONS_SOURCE_TO_USE.keys()
    stale_in_overrides = PPP_DEVIATIONS_SOURCE_TO_USE.keys() - countries_with_deviations

    assert not missing_from_overrides, f"Countries with deviations (>={PPP_PIP_WDI_MAX_DEVIATION * 100}%) missing from PPP_DEVIATIONS_SOURCE_TO_USE: {missing_from_overrides}"
    assert (
        not stale_in_overrides
    ), f"Countries in PPP_DEVIATIONS_SOURCE_TO_USE that no longer have deviations: {stale_in_overrides}"

    # Apply manual overrides: map Source enum → "pip"/"wdi"/NaN.
    source_to_label = {Source.WDI: "wdi", Source.PIP: "pip"}
    source_overrides = {
        country: source_to_label.get(source) for country, source in PPP_DEVIATIONS_SOURCE_TO_USE.items()
    }
    tb_joined.loc[source_overrides.keys(), "ppp_source"] = [source_overrides[c] for c in source_overrides]

    # Exclude additional countries (e.g. recent currency changes).
    for country in PPP_ADDITIONAL_COUNTRIES_TO_EXCLUDE:
        assert country in tb_joined.index, f"{country} from PPP_ADDITIONAL_COUNTRIES_TO_EXCLUDE not found in data."
        tb_joined.loc[country, "ppp_source"] = np.nan

    # Drop countries with no usable source, then pick the value from the chosen source.
    tb_joined = tb_joined.dropna(subset=["ppp_source"])
    tb_joined["ppp"] = np.where(tb_joined["ppp_source"] == "pip", tb_joined["pip_ppp"], tb_joined["wdi_ppp"])

    assert tb_joined["ppp"].isna().sum() == 0, "Some countries still have missing PPP values after source selection."

    return tb_joined[["ppp", "ppp_source"]]


def load_cpi_data(ds_wdi: Dataset) -> DataFrame:
    # Consumer price index (2010 = 100) — keep PPP_YEAR and the latest available year per country
    tb_cpi_all = (
        ds_wdi["wdi"]
        .query("year >= @PPP_YEAR and fp_cpi_totl.notna()")
        .reset_index()[["country", "year", "fp_cpi_totl"]]
        .rename(columns={"fp_cpi_totl": "cpi", "year": "cpi_year"})
    )

    # Get data for the target year
    tb_cpi_base = tb_cpi_all.query("cpi_year == @PPP_YEAR").set_index("country")[["cpi_year", "cpi"]]
    # Get data for the latest available year per country
    tb_cpi_latest = tb_cpi_all.loc[tb_cpi_all.groupby("country")["cpi_year"].idxmax()].set_index("country")[
        ["cpi_year", "cpi"]
    ]

    tb_wdi_cpi = tb_cpi_base.join(tb_cpi_latest, how="inner", lsuffix="_base", rsuffix="_latest")

    # Factor between cpi_year_base and cpi_year_latest
    tb_wdi_cpi["cpi_factor"] = tb_wdi_cpi["cpi_latest"] / tb_wdi_cpi["cpi_base"]
    return tb_wdi_cpi


def run() -> None:
    #
    # Load dependencies.
    #
    ds_wdi = paths.load_dataset("wdi")
    ds_pip = paths.load_dataset("world_bank_pip")
    ds_regions = paths.load_dataset("regions")

    # Map country names to OWID country codes (e.g. "United Kingdom" → "GBR").
    country_name_to_code = ds_regions["regions"].reset_index().set_index("name")["code"]

    #
    # Process data.
    #

    # Select a single PPP value per country, reconciling PIP and WDI sources.
    tb_ppp = load_and_reconcile_ppp_data(ds_wdi, ds_pip)

    # Get CPI adjustment factor (CPI[latest year] / CPI[PPP_YEAR]) per country.
    tb_cpi = load_cpi_data(ds_wdi)

    # Inner join: only keep countries that have both PPP and CPI data.
    tb = tb_ppp.join(tb_cpi, how="inner")

    # The combined conversion factor converts from international dollars in PPP_YEAR
    # to local currency units (LCU) in the latest CPI year.
    tb["conversion_factor"] = tb["ppp"] * tb["cpi_factor"]

    # Add OWID country code.
    tb["country_code"] = tb.index.map(country_name_to_code)

    #
    # Format output table.
    #
    tb = tb.rename(columns={"ppp": "ppp_factor", "cpi_year_latest": "conversion_factor_year"})
    tb["ppp_year"] = PPP_YEAR
    tb["ppp_factor"] = tb["ppp_factor"].round(3)
    tb["cpi_factor"] = tb["cpi_factor"].round(3)
    tb["conversion_factor"] = tb["conversion_factor"].round(3)
    tb["conversion_factor_year"] = tb["conversion_factor_year"].astype(int)

    # Select and order output columns.
    tb = tb[
        [
            "country_code",
            "ppp_year",
            "ppp_factor",
            "ppp_source",
            "cpi_factor",
            "conversion_factor",
            "conversion_factor_year",
        ]
    ]
    tb.metadata.short_name = paths.short_name

    #
    # Save outputs.
    #
    ds = paths.create_dataset(tables=[tb], formats=["csv", "json"])
    ds.save()
