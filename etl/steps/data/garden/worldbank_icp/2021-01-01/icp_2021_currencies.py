"""Currency units per country from the World Bank ICP 2021 round.

The ICP 2021 metadata directly maps WB ISO3 country codes to the currency used in price
surveys, which is the same currency used in WDI and PIP PPP data — making this the ideal
source for annotating int_dollar_conversions output.

The WB ISO3 codes are mapped to OWID country names via the regions dataset.
"""

from owid.catalog import Table

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    snap = paths.load_snapshot("icp_2021_currencies.json")
    tb = snap.read_json()

    # Fix non-standard WB country codes before joining to OWID regions.
    # RUT: WB internal code for Russia in ICP 2021 (standard ISO3 is RUS).
    # XKX: widely-used code for Kosovo; OWID uses OWID_KOS.
    # BON: Bonaire (Caribbean Netherlands) — not tracked separately in OWID, dropped via dropna below.
    WB_CODE_FIXES = {"RUT": "RUS", "XKX": "OWID_KOS"}
    tb["country_code"] = tb["country_code"].replace(WB_CODE_FIXES)

    # Map WB ISO3 codes to OWID country names via the regions dataset.
    ds_regions = paths.load_dataset("regions")
    tb_regions = ds_regions["regions"].reset_index()[["code", "name"]]
    tb = tb.merge(tb_regions, left_on="country_code", right_on="code", how="left")

    # Drop entries that didn't match any OWID country (e.g. regional aggregates like "WLD", "EAS").
    tb = tb.dropna(subset=["name"])

    tb = tb.rename(columns={"name": "country"})[["country", "currency_code", "currency_name"]]
    tb = tb.set_index("country")
    tb.metadata.short_name = paths.short_name

    ds = paths.create_dataset(tables=[tb], check_variables_metadata=False)
    ds.save()
