from owid.catalog import Dataset
from pandas import DataFrame

from etl.helpers import PathFinder

paths = PathFinder(__file__)

PPP_VERSION = 2021
TARGET_YEAR = 2021


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

    print("wdi ppp\n", tb_cpi_base.head(), tb_cpi_latest.head())
    tb_wdi_cpi = tb_cpi_base.join(tb_cpi_latest, how="inner", lsuffix="_base", rsuffix="_latest")

    # Factor between cpi_year_base and cpi_year_latest
    tb_wdi_cpi["cpi_factor"] = tb_wdi_cpi["cpi_latest"] / tb_wdi_cpi["cpi_base"]
    return tb_wdi_cpi


def run() -> None:
    # WDI indicators
    ds_wdi = paths.load_dataset("wdi")
    # PPP conversion factor, private consumption (LCU per international $)
    tb_wdi_ppp = (
        ds_wdi["wdi"]
        .query("year == @TARGET_YEAR and pa_nus_prvt_pp.notna()")
        .reset_index()
        .set_index("country")["pa_nus_prvt_pp"]
    )

    tb_wdi_cpi = load_cpi_data(ds_wdi)

    # World Bank PIP – poverty (income or consumption consolidated, no spells)
    ds_pip = paths.load_dataset("world_bank_pip")
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

    print("wdi ppp\n", tb_wdi_ppp.head())
    print("wdi cpi\n", tb_wdi_cpi.head())
    print("pip poverty\n", tb_pip_ppp.head())


run()
