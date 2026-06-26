"""Helpers for inequality_comparison: build the combined PIP + WID key-indicators (`keyvars`)
table directly from the new *dimensional* garden datasets (`world_bank_pip`,
`world_inequality_database`).

inequality_comparison used to read these from the wide-flat `world_bank_pip_legacy` /
`world_inequality_database_legacy` datasets (via a separate poverty_inequality_file step). Those
legacy datasets are now kept only for the CSV-based explorers, so we read the dimensional tables
directly. They are already long, so we just select the indicators the comparison needs and map
their dimension values (welfare type, extrapolation, reporting level) onto the keyvars labels —
no intermediate wide reshape.

The comparison only uses the *unitless* inequality variables (Gini, top shares, Palma ratio) plus
the relative-poverty headcount, so no money-denominated series are carried and no PPP/price-year
label is needed. The only PPP reference is a data-derived filter that selects the latest PIP PPP
base (so it tracks PIP rebasing instead of a hardcoded year).
"""

import owid.catalog.processing as pr
from owid.catalog import Dataset, Table
from owid.catalog.core import warnings

# WB regional aggregates. In the legacy PIP table these rows carried a missing welfare_type and a
# literal "<NA>" reporting_level; in the dimensional dataset they live under the combined
# "income or consumption" welfare type (and only from 1990 onwards). They are kept here for
# faithfulness but the comparison discards them (it only matches the unitless inequality series).
PIP_REGIONS = [
    "East Asia and Pacific (WB)",
    "Eastern and Southern Africa (WB)",
    "Europe and Central Asia (WB)",
    "Latin America and Caribbean (WB)",
    "Middle East, North Africa, Afghanistan and Pakistan (WB)",
    "North America (WB)",
    "South Asia (WB)",
    "Sub-Saharan Africa (WB)",
    "Western and Central Africa (WB)",
    "World",
    "World (excluding China)",
    "World (excluding India)",
]

# WID dimensional inequality columns -> keyvars indicator name.
WID_INEQUALITY_INDICATORS = {
    "gini": "gini",
    "share_top_1": "p99p100Share",
    "share_top_10": "p90p100Share",
    "palma_ratio": "palmaRatio",
}
# WID dimensional welfare_type -> (series_code welfare code, human-readable welfare label).
WID_WELFARE = {
    "before tax": ("pretaxNational", "Pretax national income"),
    "after tax": ("posttaxNational", "Post-tax national income"),
}
# WID dimensional extrapolated flag -> (series_code source code, human-readable source label).
WID_SOURCE = {
    "no": ("wid", "WID"),
    "yes": ("widExtrapolated", "WID (including extrapolated datapoints)"),
}

# Indicators expressed as percentages (everything else here — Gini, Palma — is unitless).
SHARE_INDICATORS = {"p90p100Share", "p99p100Share"}


def _mask(condition):
    """Coerce a (possibly nullable/arrow) boolean Series to a plain bool Series (NA -> False).

    The dimensional tables use nullable dtypes, so comparisons yield nullable booleans that raise
    "boolean value of NA is ambiguous" when combined with `&` or used to index. This normalises them.
    """
    return condition.fillna(False).astype(bool)


def build_keyvars(ds_pip: Dataset, ds_wid: Dataset) -> Table:
    """Build the combined PIP + WID key-indicators (`keyvars`) long table consumed by
    inequality_comparison, directly from the dimensional datasets."""
    tb = pr.concat([_pip_keyvars(ds_pip), _wid_keyvars(ds_wid)], ignore_index=True, short_name="keyvars")

    # Drop rows with null values in the value column.
    tb = tb.dropna(subset=["value"]).reset_index(drop=True)

    # Remove provider region aggregates and World, and fold urban/rural entities back into the country.
    region_suffixes = ["\\(PIP\\)", "\\(LIS\\)", "\\(WID\\)"]
    tb = tb[~tb["country"].str.contains("|".join(region_suffixes))].reset_index(drop=True)
    tb = tb[tb["country"] != "World"].reset_index(drop=True)
    tb = tb[~tb["country"].isin(["China (urban)", "China (rural)"])].reset_index(drop=True)
    tb["country"] = tb["country"].str.replace(" (urban)", "", regex=False).str.replace(" (rural)", "", regex=False)

    sanity_check_keyvars(tb)
    return tb


def keyvars_origins(ds_pip: Dataset, ds_wid: Dataset) -> tuple[list, list]:
    """Return (pip_origins, wid_origins) for attaching provenance to the comparison's variables.

    Origins are uniform within each source, so one representative dimensional column suffices.
    """
    pip_origins = ds_pip.read("inequality")["gini"].metadata.origins
    wid_origins = ds_wid.read("inequality")["gini"].metadata.origins
    return pip_origins, wid_origins


def _pip_keyvars(ds_pip: Dataset) -> Table:
    """Select the PIP inequality indicators from the dimensional `complete_series` table and map
    them onto the keyvars layout. gini / palma sit on the dimension-less inequality rows (no PPP);
    the top-decile share and relative-poverty headcount sit on the latest-PPP rows."""
    cs = ds_pip.read("complete_series")

    # Latest PPP base in the data — avoids hardcoding a year that goes stale on the next PIP update.
    # Shares and relative-poverty headcounts are PPP-independent, so this only selects which rows exist.
    ppp_year = int(cs["ppp_version"].dropna().max())
    summary = _mask(cs["decile"].isna()) & _mask(cs["poverty_line"].isna())
    inequality_rows = cs[_mask(cs["ppp_version"].isna()) & summary]
    ppp_rows = cs[_mask(cs["ppp_version"] == ppp_year)]

    slices = [
        _slice(inequality_rows, "gini", "gini", ["country", "year", "welfare_type"]),
        _slice(inequality_rows, "palma_ratio", "palmaRatio", ["country", "year", "welfare_type"]),
        _slice(
            ppp_rows[_mask(ppp_rows["decile"] == "10") & _mask(ppp_rows["poverty_line"].isna())],
            "share",
            "p90p100Share",
            ["country", "year", "welfare_type"],
        ),
        _slice(
            ppp_rows[_mask(ppp_rows["decile"].isna()) & _mask(ppp_rows["poverty_line"] == "50% of the median")],
            "headcount_ratio",
            "headcountRatio50Median",
            ["country", "year", "welfare_type"],
        ),
    ]
    tb = pr.concat(slices, ignore_index=True)

    # Keep income/consumption for countries, and the combined series only for regions.
    is_region = tb["country"].isin(PIP_REGIONS)
    keep = _mask(tb["welfare_type"].isin(["income", "consumption"])) | (
        is_region & _mask(tb["welfare_type"] == "income or consumption")
    )
    tb = tb[keep].reset_index(drop=True)

    # reporting_level from the country-name suffix; regions get "<NA>" / missing welfare (legacy convention).
    country_str = tb["country"].astype(str)
    tb["pipreportinglevel"] = "national"
    tb.loc[country_str.str.endswith("(urban)"), "pipreportinglevel"] = "urban"
    tb.loc[country_str.str.endswith("(rural)"), "pipreportinglevel"] = "rural"
    region_mask = tb["country"].isin(PIP_REGIONS)
    tb.loc[region_mask, "pipreportinglevel"] = "<NA>"
    tb["pipwelfare"] = tb["welfare_type"].astype(object)
    tb.loc[region_mask, "pipwelfare"] = None
    tb = tb.drop(columns=["welfare_type"])

    # Descriptive columns + series_code (intermediate codes: pip / disposable / perCapita).
    tb["source"] = "PIP"
    tb["welfare"] = "Disposable income or consumption"
    tb["resource_sharing"] = "Per capita"
    tb["prices"] = ""
    tb["unit"] = ""
    tb.loc[tb["indicator_name"].isin(SHARE_INDICATORS), "unit"] = "%"
    tb["series_code"] = tb["indicator_name"] + "_pip_disposable_perCapita"

    return tb


def _wid_keyvars(ds_wid: Dataset) -> Table:
    """Select the WID inequality indicators from the dimensional `inequality` and `relative_poverty`
    tables and map them onto the keyvars layout."""
    tb_ineq = ds_wid.read("inequality")
    tb_ineq = tb_ineq[_mask(tb_ineq["welfare_type"].isin(WID_WELFARE))]
    id_cols = ["country", "year", "welfare_type", "extrapolated"]
    with warnings.ignore_warnings([warnings.DifferentValuesWarning]):
        tb_ineq = tb_ineq.melt(
            id_vars=id_cols, value_vars=list(WID_INEQUALITY_INDICATORS), var_name="indicator_name", value_name="value"
        )
    tb_ineq["indicator_name"] = tb_ineq["indicator_name"].map(WID_INEQUALITY_INDICATORS)

    tb_rp = ds_wid.read("relative_poverty")
    tb_rp = tb_rp[_mask(tb_rp["welfare_type"].isin(WID_WELFARE)) & _mask(tb_rp["poverty_line"] == "50% of the median")][
        id_cols + ["headcount_ratio"]
    ].rename(columns={"headcount_ratio": "value"})
    tb_rp["indicator_name"] = "headcountRatio50Median"

    tb = pr.concat([tb_ineq, tb_rp], ignore_index=True)

    # Map dimensions onto the keyvars labels; series_code uses the intermediate codes.
    source_code = tb["extrapolated"].map({k: v[0] for k, v in WID_SOURCE.items()})
    welfare_code = tb["welfare_type"].map({k: v[0] for k, v in WID_WELFARE.items()})
    tb["source"] = tb["extrapolated"].map({k: v[1] for k, v in WID_SOURCE.items()})
    tb["welfare"] = tb["welfare_type"].map({k: v[1] for k, v in WID_WELFARE.items()})
    tb["resource_sharing"] = "Per adult"
    tb["prices"] = ""
    tb["unit"] = ""
    tb.loc[tb["indicator_name"].isin(SHARE_INDICATORS), "unit"] = "%"
    tb["series_code"] = tb["indicator_name"] + "_" + source_code + "_" + welfare_code + "_perAdult"

    return tb.drop(columns=["welfare_type", "extrapolated"])


def _slice(tb: Table, value_col: str, indicator_name: str, id_cols: list[str]) -> Table:
    """Take one indicator column from a dimensional slice and relabel it as a keyvars long row."""
    out = tb[id_cols + [value_col]].rename(columns={value_col: "value"})
    out["indicator_name"] = indicator_name
    return out


def sanity_check_keyvars(tb: Table) -> None:
    assert not tb.empty, "keyvars reconstruction is empty."
    assert not tb["value"].isna().any(), "keyvars has null values after dropna."
    expected = {
        "gini_pip_disposable_perCapita",
        "p90p100Share_pip_disposable_perCapita",
        "palmaRatio_pip_disposable_perCapita",
        "gini_wid_pretaxNational_perAdult",
        "p99p100Share_wid_pretaxNational_perAdult",
        "p90p100Share_wid_pretaxNational_perAdult",
        "palmaRatio_wid_pretaxNational_perAdult",
    }
    missing = expected - set(tb["series_code"].unique())
    assert not missing, f"Missing expected keyvars series_code(s): {missing}"
