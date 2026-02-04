"""Latest GDP per capita for each entity.

Published as CSV for use by Grapher codebase (entity sorting, peer countries).
Not meant to be imported to MySQL.
"""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    # Load GDP per capita (PPP, constant 2021 $) from World Bank WDI
    ds_gdp = paths.load_dataset("wdi")
    tb_gdp = ds_gdp["wdi"].reset_index()

    # Get latest year per country for GDP
    tb_gdp = tb_gdp[tb_gdp["ny_gdp_pcap_pp_kd"].notna()]
    idx_latest = tb_gdp.groupby("country")["year"].idxmax()
    tb = tb_gdp.loc[idx_latest, ["country", "year", "ny_gdp_pcap_pp_kd"]]

    # Round values (no decimals needed)
    tb["ny_gdp_pcap_pp_kd"] = tb["ny_gdp_pcap_pp_kd"].round(0).astype(int)

    # Rename columns
    tb = tb.rename(columns={"country": "entity", "ny_gdp_pcap_pp_kd": "value"})

    # Set index and name
    tb = tb.set_index(["entity", "year"], verify_integrity=True)
    tb.metadata.short_name = "gdp"

    # Save as CSV
    ds = paths.create_dataset(tables=[tb], formats=["csv", "json"])
    ds.save()
