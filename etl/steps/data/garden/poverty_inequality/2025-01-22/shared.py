"""Helpers for inequality_comparison: rebuild the legacy-shaped PIP and WID key-indicator tables
from the new *dimensional* garden datasets (`world_bank_pip`, `world_inequality_database`) and
assemble the combined `keyvars` table.

inequality_comparison used to read these from the wide-flat `world_bank_pip_legacy` /
`world_inequality_database_legacy` datasets (via a separate poverty_inequality_file step). Those
legacy datasets are now kept only for the CSV-based explorers, so we read the dimensional datasets
directly and reshape the few columns the key-indicator transforms need back into the legacy
layout — reproducing the previous `keyvars` output value-for-value.
"""

import owid.catalog.processing as pr
from owid.catalog import Dataset, Table
from owid.catalog.core import warnings

# PPP versions used across the poverty/inequality key indicators.
PPP_YEAR_PIP = 2021
PPP_YEAR_WID = 2023

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


def build_keyvars(tb_pip: Table, tb_wid: Table) -> Table:
    """Assemble the combined PIP + WID key-indicators (keyvars) long table consumed by
    inequality_comparison, from the reconstructed PIP/WID tables.

    This is the table poverty_inequality_file used to publish; it is now built in-memory because
    inequality_comparison is its only consumer.
    """
    tb_pip_keyvars = create_keyvars_file_pip(tb_pip)
    tb_wid_keyvars = create_keyvars_file_wid(tb_wid, extrapolated=False)
    tb_wid_keyvars_extrapolated = create_keyvars_file_wid(tb_wid, extrapolated=True)

    tb = pr.concat(
        [tb_pip_keyvars, tb_wid_keyvars, tb_wid_keyvars_extrapolated],
        ignore_index=True,
        short_name="keyvars",
    )

    # Drop rows with null values in the value column.
    tb = tb.dropna(subset=["value"])

    # Remove provider region aggregates and World, and fold urban/rural entities back into the country.
    region_suffixes_list = ["\\(PIP\\)", "\\(LIS\\)", "\\(WID\\)"]
    tb = tb[~tb["country"].str.contains("|".join(region_suffixes_list))].reset_index(drop=True)
    tb = tb[tb["country"] != "World"].reset_index(drop=True)
    tb = tb[~tb["country"].isin(["China (urban)", "China (rural)"])].reset_index(drop=True)
    tb["country"] = tb["country"].str.replace(" (urban)", "", regex=False).str.replace(" (rural)", "", regex=False)

    return tb


def create_keyvars_file_pip(tb: Table) -> Table:
    """
    Process the main table from PIP, to adapt it to a concatenated file with LIS and WID
    """

    tb = tb.copy()

    # Set the list of indicators to use
    indicators_list = [
        "gini",
        "mean",
        "median",
        "decile10_share",
        "palma_ratio",
        "headcount_ratio_50_median",
    ]

    # Select the columns to keep
    tb = tb[["country", "year", "reporting_level", "welfare_type"] + indicators_list]

    with warnings.ignore_warnings([warnings.DifferentValuesWarning]):
        # Make pip table longer
        tb = tb.melt(
            id_vars=["country", "year", "reporting_level", "welfare_type"],
            var_name="indicator_name",
            value_name="value",
        )

    # Rename welfare_type and reporting_level
    tb = tb.rename(columns={"welfare_type": "pipwelfare", "reporting_level": "pipreportinglevel"})

    # Rename welfare and equivalization columns
    tb["indicator_name"] = tb["indicator_name"].replace(
        {
            "palma_ratio": "palmaRatio",
            "headcount_ratio_50_median": "headcountRatio50Median",
            "decile10_share": "p90p100Share",
        }
    )

    # Add descriptive columns
    tb["welfare"] = "disposable"
    tb["resource_sharing"] = "perCapita"
    tb["source"] = "pip"
    tb["prices"] = ""
    tb["prices"] = tb["prices"].where(
        (tb["indicator_name"] != "mean") & (tb["indicator_name"] != "median"),
        f"{PPP_YEAR_PIP}ppp{PPP_YEAR_PIP}",
    )
    tb["prices"] = tb["prices"].astype(str)

    # Add the column series_code, which is the concatenation of welfare, equivalization and indicator_name
    tb["series_code"] = (
        tb["indicator_name"].astype(str)
        + "_"
        + tb["source"].astype(str)
        + "_"
        + tb["welfare"].astype(str)
        + "_"
        + tb["resource_sharing"].astype(str)
        + "_"
        + tb["prices"].astype(str)
    )

    # Remove trailing "_" from series_code
    tb["series_code"] = tb["series_code"].str.rstrip("_")

    # Replace names for descriptive columns
    tb["source"] = tb["source"].replace({"pip": "PIP"})
    tb["prices"] = tb["prices"].replace(
        {f"{PPP_YEAR_PIP}ppp{PPP_YEAR_PIP}": f"{PPP_YEAR_PIP} PPPs, at {PPP_YEAR_PIP} prices"}
    )
    tb["welfare"] = tb["welfare"].replace({"disposable": "Disposable income or consumption"})
    tb["resource_sharing"] = tb["resource_sharing"].replace({"perCapita": "Per capita"})

    # Add unit column
    tb["unit"] = ""
    tb["unit"] = tb["unit"].where(
        (tb["indicator_name"] != "mean") & (tb["indicator_name"] != "median"),
        "dollars",
    )
    tb["unit"] = tb["unit"].where(tb["indicator_name"] != "p90p100Share", "%")
    tb["unit"] = tb["unit"].astype(str)

    return tb


def create_keyvars_file_wid(tb: Table, extrapolated: bool) -> Table:
    """
    Process the main table from WID, to adapt it to a concatenated file with LIS and PIP
    """
    tb = tb.copy()

    # Set the list of indicators to use
    indicators_list = [
        "p0p100_gini_pretax",
        "p0p100_gini_posttax_nat",
        "p0p100_avg_pretax",
        "p0p100_avg_posttax_nat",
        "median_pretax",
        "median_posttax_nat",
        "p99p100_share_pretax",
        "p99p100_share_posttax_nat",
        "p99p100_avg_pretax",
        "p99p100_avg_posttax_nat",
        "p90p100_share_pretax",
        "p90p100_share_posttax_nat",
        "palma_ratio_pretax",
        "palma_ratio_posttax_nat",
        "headcount_ratio_50_median_pretax",
        "headcount_ratio_50_median_posttax_nat",
    ]

    # Add _extrapolated to each member of indicators_list
    if extrapolated:
        indicators_list = [indicator + "_extrapolated" for indicator in indicators_list]

    # Select the columns to keep
    tb = tb[["country", "year"] + indicators_list]

    with warnings.ignore_warnings([warnings.DifferentValuesWarning]):
        # Make wid table longer
        tb = tb.melt(id_vars=["country", "year"], var_name="indicator_welfare", value_name="value")

    # Replace the name posttax_nat with posttax
    tb["indicator_welfare"] = tb["indicator_welfare"].str.replace("posttax_nat", "posttax")

    if extrapolated:
        tb["indicator_welfare"] = tb["indicator_welfare"].str.replace("_extrapolated", "")

    # Split indicator_welfare column into two columns, using the last "_" as separator
    tb[["indicator_name", "welfare"]] = tb["indicator_welfare"].str.rsplit("_", n=1, expand=True)

    # Drop indicator_welfare column
    tb = tb.drop(columns=["indicator_welfare"])

    # Rename welfare column
    tb["welfare"] = tb["welfare"].replace({"pretax": "pretaxNational", "posttax": "posttaxNational"})
    tb["indicator_name"] = tb["indicator_name"].replace(
        {
            "p0p100_gini": "gini",
            "p0p100_avg": "mean",
            "p99p100_share": "p99p100Share",
            "p99p100_avg": "p99p100Average",
            "p90p100_share": "p90p100Share",
            "palma_ratio": "palmaRatio",
            "headcount_ratio_50_median": "headcountRatio50Median",
        }
    )

    # Add descriptive columns
    if extrapolated:
        tb["source"] = "widExtrapolated"
    else:
        tb["source"] = "wid"

    tb["prices"] = ""
    tb["prices"] = tb["prices"].where(
        (tb["indicator_name"] != "mean") & (tb["indicator_name"] != "median"),
        f"2011ppp{PPP_YEAR_WID}",
    )
    tb["prices"] = tb["prices"].astype(str)
    tb["resource_sharing"] = "perAdult"

    # Add the column series_code, which is the concatenation of welfare, equivalization and indicator_name
    tb["series_code"] = (
        tb["indicator_name"].astype(str)
        + "_"
        + tb["source"].astype(str)
        + "_"
        + tb["welfare"].astype(str)
        + "_"
        + tb["resource_sharing"].astype(str)
        + "_"
        + tb["prices"].astype(str)
    )

    # Remove trailing "_" from series_code
    tb["series_code"] = tb["series_code"].str.rstrip("_")

    # Replace names for descriptive columns
    if extrapolated:
        tb["source"] = tb["source"].replace({"widExtrapolated": "WID (including extrapolated datapoints)"})
    else:
        tb["source"] = tb["source"].replace({"wid": "WID"})

    tb["prices"] = tb["prices"].replace({f"2011ppp{PPP_YEAR_WID}": f"2011 PPPs, at {PPP_YEAR_WID} prices"})
    tb["welfare"] = tb["welfare"].replace(
        {"pretaxNational": "Pretax national income", "posttaxNational": "Post-tax national income"}
    )
    tb["resource_sharing"] = tb["resource_sharing"].replace({"perAdult": "Per adult"})

    # Add unit column
    tb["unit"] = ""
    tb["unit"] = tb["unit"].where(
        (tb["indicator_name"] != "mean") & (tb["indicator_name"] != "median"),
        "dollars",
    )
    tb["unit"] = tb["unit"].where(tb["indicator_name"] != "p99p100Share", "%")
    tb["unit"] = tb["unit"].where(tb["indicator_name"] != "p90p100Share", "%")
    tb["unit"] = tb["unit"].astype(str)

    return tb
