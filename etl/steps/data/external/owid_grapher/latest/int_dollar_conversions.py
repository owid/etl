from etl.helpers import PathFinder

paths = PathFinder(__file__)

PPP_VERSION = 2021
TARGET_YEAR = 2021


def run() -> None:
    # WDI indicators
    ds_wdi = paths.load_dataset("wdi")
    # PPP conversion factor, private consumption (LCU per international $)
    tb_wdi_ppp = ds_wdi["wdi"].query("year == @TARGET_YEAR").reset_index().set_index("country")["pa_nus_prvt_pp"]
    # Consumer price index (2010 = 100)
    tb_wdi_cpi = ds_wdi["wdi"].query("year >= @TARGET_YEAR").reset_index().set_index("country")[["year", "fp_cpi_totl"]]

    # World Bank PIP – poverty (income or consumption consolidated, no spells)
    ds_pip = paths.load_dataset("world_bank_pip")
    # Purchasing Power Parity (PPP) rates (2021 prices)
    tb_pip_ppp = (
        ds_pip["world_bank_pip"]
        .query("ppp_version == @PPP_VERSION and year == @TARGET_YEAR")
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
