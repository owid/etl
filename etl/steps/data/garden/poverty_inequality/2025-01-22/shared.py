"""Adapters that rebuild the legacy-shaped PIP and WID key-indicator tables from the new
*dimensional* garden datasets (`world_bank_pip`, `world_inequality_database`).

`poverty_inequality_file` and `inequality_comparison` used to read the wide-flat
`world_bank_pip_legacy` / `world_inequality_database_legacy` datasets. Those legacy datasets
are now kept only for the CSV-based explorers, so the steps here read the dimensional datasets
instead and reshape the few columns their downstream key-indicator transforms need back into the
legacy layout. The downstream `create_keyvars_file_*` functions are left untouched — these
adapters reproduce their expected inputs value-for-value.
"""

import owid.catalog.processing as pr
from owid.catalog import Dataset, Table

# PPP version used across the poverty/inequality file.
PPP_YEAR_PIP = 2021

# WB regional aggregates. In the legacy PIP table these rows carry a missing welfare_type and a
# literal "<NA>" reporting_level; in the dimensional dataset they live under the combined
# "income or consumption" welfare type (and only from 1990 onwards — see note in build_pip_unsmoothed).
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

# Dimensional welfare_type -> legacy welfare code (only pretax and posttax_nat are consumed).
WID_WELFARE_TO_CODE = {
    "before tax": "pretax",
    "after tax": "posttax_nat",
}


def _mask(condition):
    """Coerce a (possibly nullable/arrow) boolean Series to a plain bool Series (NA -> False).

    The dimensional tables use nullable dtypes, so comparisons yield nullable booleans that raise
    "boolean value of NA is ambiguous" when combined with `&` or used to index. This normalises them.
    """
    return condition.fillna(False).astype(bool)


def build_pip_unsmoothed(ds_pip: Dataset) -> Table:
    """Rebuild `income_consumption_2021_unsmoothed` (the columns create_keyvars_file_pip reads)
    from the dimensional `complete_series` table.

    Each indicator lives on a different row-subset of complete_series:
      - gini / palma_ratio: the dimension-less inequality rows (no PPP, so ppp_version is NaN)
      - mean / median:      2021-PPP country-year summary rows (no decile, no poverty line)
      - decile10_share:     2021-PPP, decile "10", no poverty line -> `share`
      - headcount_ratio_50_median: 2021-PPP relative-poverty rows -> `headcount_ratio`

    Country rows keep welfare_type income/consumption; WB regions live under the combined
    "income or consumption" welfare type and are relabelled to match the legacy table (which marks
    them with a missing welfare_type and a "<NA>" reporting_level). NOTE: the dimensional dataset
    only carries regional aggregates from 1990 onwards, so pre-1990 region rows that existed in the
    legacy table are absent here — they fall outside the inequality_comparison matching windows.
    """
    cs = ds_pip.read("complete_series")
    keys = ["country", "year", "welfare_type"]

    ppp_current = _mask(cs["ppp_version"] == PPP_YEAR_PIP)
    ppp_missing = _mask(cs["ppp_version"].isna())
    decile_missing = _mask(cs["decile"].isna())
    decile_top = _mask(cs["decile"] == "10")
    povline_missing = _mask(cs["poverty_line"].isna())
    povline_rel50 = _mask(cs["poverty_line"] == "50% of the median")
    summary = decile_missing & povline_missing

    tb_ineq = cs[ppp_missing & summary][keys + ["gini", "palma_ratio"]]
    tb_mean_median = cs[ppp_current & summary][keys + ["mean", "median"]]
    tb_decile10 = cs[ppp_current & decile_top & povline_missing][keys + ["share"]].rename(
        columns={"share": "decile10_share"}
    )
    tb_rel_poverty = cs[ppp_current & decile_missing & povline_rel50][keys + ["headcount_ratio"]].rename(
        columns={"headcount_ratio": "headcount_ratio_50_median"}
    )

    tb = tb_ineq
    for piece in [tb_mean_median, tb_decile10, tb_rel_poverty]:
        tb = pr.merge(tb, piece, on=keys, how="outer")

    # Keep income/consumption for countries, and the combined series only for regions.
    is_region = tb["country"].isin(PIP_REGIONS)
    keep = _mask(tb["welfare_type"].isin(["income", "consumption"])) | (
        is_region & _mask(tb["welfare_type"] == "income or consumption")
    )
    tb = tb[keep].reset_index(drop=True)

    # reporting_level from the country-name suffix; regions get "<NA>" / missing welfare like the legacy table.
    country_str = tb["country"].astype(str)
    tb["reporting_level"] = "national"
    tb.loc[country_str.str.endswith("(urban)"), "reporting_level"] = "urban"
    tb.loc[country_str.str.endswith("(rural)"), "reporting_level"] = "rural"
    region_mask = tb["country"].isin(PIP_REGIONS)
    tb.loc[region_mask, "reporting_level"] = "<NA>"
    tb["welfare_type"] = tb["welfare_type"].astype(object)
    tb.loc[region_mask, "welfare_type"] = None

    sanity_check_pip_unsmoothed(tb)
    return tb


def build_wid_main(ds_wid: Dataset) -> Table:
    """Rebuild the wide `world_inequality_database` table (the columns create_keyvars_file_wid reads)
    from the dimensional inequality / incomes / relative_poverty tables.

    For welfare ∈ {pretax, posttax_nat} and both extrapolation states, build:
      p0p100_gini ← inequality.gini            palma_ratio ← inequality.palma_ratio
      p99p100_share ← inequality.share_top_1   p90p100_share ← inequality.share_top_10
      p0p100_avg ← incomes.mean (year)         median ← incomes.median (year)
      p99p100_avg ← incomes.avg (Richest 1%, year)
      headcount_ratio_50_median ← relative_poverty.headcount_ratio (50% of the median)
    """
    tb_ineq = ds_wid.read("inequality")
    tb_inc = ds_wid.read("incomes")
    tb_rp = ds_wid.read("relative_poverty")

    all_rows = _mask(tb_ineq["country"].notna())
    summary_income = _mask(tb_inc["quantile"].isna()) & _mask(tb_inc["period"] == "year")
    top1_income = _mask(tb_inc["quantile"] == "Richest 1%") & _mask(tb_inc["period"] == "year")
    rel_pov_50 = _mask(tb_rp["poverty_line"] == "50% of the median")

    specs = [
        (tb_ineq, all_rows, "gini", "p0p100_gini"),
        (tb_ineq, all_rows, "palma_ratio", "palma_ratio"),
        (tb_ineq, all_rows, "share_top_1", "p99p100_share"),
        (tb_ineq, all_rows, "share_top_10", "p90p100_share"),
        (tb_inc, summary_income, "mean", "p0p100_avg"),
        (tb_inc, summary_income, "median", "median"),
        (tb_inc, top1_income, "avg", "p99p100_avg"),
        (tb_rp, rel_pov_50, "headcount_ratio", "headcount_ratio_50_median"),
    ]

    tb = None
    for source, mask, src_col, target_base in specs:
        piece = _wid_wide(source[mask], src_col, target_base)
        tb = piece if tb is None else pr.merge(tb, piece, on=["country", "year"], how="outer")

    sanity_check_wid_main(tb)
    return tb


def _wid_wide(tb: Table, src_col: str, target_base: str) -> Table:
    """Pivot a long dimensional WID slice into wide `{target_base}_{welfare}{_extrapolated}` columns
    for welfare ∈ {pretax, posttax_nat}, preserving each column's origins."""
    tb = tb[_mask(tb["welfare_type"].isin(WID_WELFARE_TO_CODE))]
    out = None
    for welfare_type, code in WID_WELFARE_TO_CODE.items():
        for extrapolated, suffix in [("no", ""), ("yes", "_extrapolated")]:
            piece = tb[_mask(tb["welfare_type"] == welfare_type) & _mask(tb["extrapolated"] == extrapolated)][
                ["country", "year", src_col]
            ].rename(columns={src_col: f"{target_base}_{code}{suffix}"})
            out = piece if out is None else pr.merge(out, piece, on=["country", "year"], how="outer")
    return out


def sanity_check_pip_unsmoothed(tb: Table) -> None:
    assert not tb.empty, "PIP unsmoothed reconstruction is empty."
    assert not tb.duplicated(subset=["country", "year", "reporting_level", "welfare_type"]).any(), (
        "Duplicate (country, year, reporting_level, welfare_type) in PIP unsmoothed reconstruction."
    )
    assert set(tb["reporting_level"].unique()) <= {"national", "urban", "rural", "<NA>"}, (
        "Unexpected reporting_level value in PIP unsmoothed reconstruction."
    )
    for col in ["gini", "mean", "median", "decile10_share", "palma_ratio", "headcount_ratio_50_median"]:
        assert col in tb.columns, f"Missing expected PIP column {col}."


def sanity_check_wid_main(tb: Table) -> None:
    assert not tb.empty, "WID main reconstruction is empty."
    assert not tb.duplicated(subset=["country", "year"]).any(), "Duplicate (country, year) in WID main reconstruction."
    for base in [
        "p0p100_gini",
        "p0p100_avg",
        "median",
        "p99p100_share",
        "p99p100_avg",
        "p90p100_share",
        "palma_ratio",
        "headcount_ratio_50_median",
    ]:
        for welfare in ["pretax", "posttax_nat"]:
            assert f"{base}_{welfare}" in tb.columns, f"Missing expected WID column {base}_{welfare}."
