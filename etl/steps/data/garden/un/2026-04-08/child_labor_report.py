"""Garden step for ILO-UNICEF 2024 Global Estimates of Child Labour.

Combines three meadow tables (child_labor_by_region, hazardous_work_by_region,
child_labor_trends) into two output tables:

  - child_labor: Main table with child labor and hazardous work shares/numbers
    by country, year, sex, and age. Includes not-in-school rates, household chores,
    and a computed 5-14 age bracket.
  - sector: Distribution of child labor and hazardous work across economic sectors
    (agriculture, industry, services) by country, year, sex, and age.
"""

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define latest year in the report (used for tables without an explicit year column)
LATEST_YEAR = 2024

# Define not in school and sector categories described in the tables, to extract and merge later.
_NOT_IN_SCHOOL = "Children in child labour who are not attending school"
_BY_SECTOR = "Children in child labour by sector of economic activity"

# Define acronyms to disambiguate regions that appear under multiple groupings (e.g. Sub-Saharan Africa appears under both ILO and SDG regions).
_REGION_ACRONYMS = {"ILO regions": "ILO", "SDG regions": "SDG", "UNICEF regions": "UNICEF"}

# Standard join columns used across table operations.
_JOIN_COLS = ["country", "year", "sex", "age"]


def run() -> None:
    #
    # Load inputs.
    #
    ds_meadow = paths.load_dataset("child_labor_report")
    tb_cl = ds_meadow.read("child_labor_by_region")
    tb_hw = ds_meadow.read("hazardous_work_by_region")
    tb_trends = ds_meadow.read("child_labor_trends")

    #
    # Process data.
    #
    # Reshape wide columns → long (sex × age dimensions).
    tb_cl = _to_long(tb_cl)
    tb_hw = _to_long(tb_hw)
    tb_trends = _trends_to_long(tb_trends)

    # Build output tables.
    tb_main = _build_main_table(tb_cl, tb_hw, tb_trends)
    tb_sector = _build_sector_table(tb_cl, tb_hw, tb_trends)

    # Convert number columns from thousands to actual values.
    for tb in [tb_main, tb_sector]:
        num_cols = [c for c in tb.columns if c.startswith("number_")]
        tb[num_cols] = tb[num_cols] * 1000

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb_main, tb_sector], default_metadata=ds_meadow.metadata)
    ds_garden.save()


# ── Reshaping ─────────────────────────────────────────────────────────────────


def _to_long(tb: Table) -> Table:
    """Reshape a wide region table (child_labor_by_region or hazardous_work_by_region) into long format.

    Input columns like `total_5_11_pct` and `boys_12_14_no` are unpivoted and parsed
    into three new dimensions — sex (total/boys/girls), age (5-11/12-14/15-17/5-17),
    and two value columns: `share` (from pct) and `number` (from no).
    """
    tb = tb.reset_index()

    # Unpivot all value columns into a single variable/value pair.
    tb = tb.melt(id_vars=["region_type", "region"], var_name="variable", value_name="value")

    # Parse sex, age, and indicator from the column name pattern {sex}_{age}_{indicator}.
    parsed = tb["variable"].str.extract(r"^(total|boys|girls)_(\d+_\d+)_(pct|no)$")
    tb["sex"] = parsed[0]
    tb["age"] = parsed[1].str.replace("_", "-")
    tb["indicator"] = parsed[2]
    tb = tb.drop(columns="variable")

    # Drop rows that didn't match the pattern (e.g. from non-value columns).
    tb = tb.dropna(subset=["sex", "age", "indicator"])

    # Spread the indicator column back into two value columns (pct → share, no → number).
    tb = tb.pivot(index=["region_type", "region", "sex", "age"], columns="indicator", values="value")
    tb.columns.name = None
    tb = tb.rename(columns={"pct": "share", "no": "number"})
    tb = tb.reset_index()

    return tb


def _trends_to_long(tb: Table) -> Table:
    """Reshape the child_labor_trends table into long format with year as a dimension.

    Input columns like `child_labor_2016_pct` and `hazardous_work_2024_no` are unpivoted
    into a year column and four value columns: share_child_labor, number_child_labor,
    share_hazardous_work, number_hazardous_work.

    After reshaping, disaggregation_type/value are mapped to standard dimensions
    (country, sex, age) via _map_disaggregations, and rows are tagged with _source
    to separate regular trends from chart-derived data (not-in-school, household chores, etc.).
    """
    tb = tb.reset_index()

    # Unpivot all 12 value columns into a single variable/value pair.
    tb = tb.melt(id_vars=["disaggregation_type", "disaggregation_value"], var_name="variable", value_name="value")

    # Parse indicator, year, and metric from the column name pattern {indicator}_{year}_{metric}.
    parsed = tb["variable"].str.extract(r"^(child_labor|hazardous_work)_(\d{4})_(pct|no)$")
    tb["indicator"] = parsed[0]
    tb["year"] = parsed[1]
    tb["metric"] = parsed[2]
    tb = tb.drop(columns="variable")

    # Drop rows that didn't match the pattern.
    tb = tb.dropna(subset=["indicator", "year", "metric"])

    # Combine metric and indicator into a single column name (e.g. share_child_labor, number_hazardous_work).
    tb["metric"] = tb["metric"].map({"pct": "share", "no": "number"})
    tb["col"] = tb["metric"] + "_" + tb["indicator"]
    tb = tb.drop(columns=["metric", "indicator"])

    # Spread into four value columns.
    tb = tb.pivot(index=["disaggregation_type", "disaggregation_value", "year"], columns="col", values="value")
    tb.columns.name = None
    tb = tb.reset_index()
    tb["year"] = tb["year"].astype(int)

    # Map disaggregation_type/value to country, sex, age and tag source type.
    tb = _map_disaggregations(tb)
    tb = tb.drop(columns=["disaggregation_type", "disaggregation_value"])

    return tb


def _map_disaggregations(tb: Table) -> Table:
    """Map disaggregation_type/value to standard dimensions (country, sex, age).

    Also adds _source and _sector tags used downstream to route rows:
      - "trends": regular trend data → merged into the main table.
      - "not_in_school": chart data from pages 8/44 → joined as not-in-school columns.
      - "household_chores": page 8 chart → joined as household chores column.
      - "child_labor_share_5_14": page 8 chart → overrides computed 5-14 shares.
      - "sector_by_region": page 34 chart → added to the sector table.

    Defaults (sex="total", age="5-17") apply to most rows and are overridden
    only for specific disaggregation types (Sex, Age, household chores, etc.).
    """
    dtype = tb["disaggregation_type"]
    dvalue = tb["disaggregation_value"]

    # Defaults: country from value, sex=total, age=5-17, source=trends.
    tb["country"] = dvalue
    tb["sex"] = "total"
    tb["age"] = "5-17"
    tb["_source"] = "trends"
    tb["_sector"] = None

    # World total: country="World".
    m = dtype == "World total"
    tb.loc[m, "country"] = "World"

    # Sex rows: country="World", sex from value.
    m = dtype == "Sex"
    tb.loc[m, "country"] = "World"
    tb.loc[m, "sex"] = dvalue[m].str.lower()

    # Age rows: country="World", age from value (strip " years").
    m = dtype == "Age"
    tb.loc[m, "country"] = "World"
    tb.loc[m, "age"] = dvalue[m].str.replace(" years", "", regex=False)

    # Disambiguate regions that appear under multiple groupings (e.g. Sub-Saharan Africa).
    for label, suffix in _REGION_ACRONYMS.items():
        m = dtype == label
        tb.loc[m, "country"] = dvalue[m] + f" ({suffix})"

    # Not-in-school rows: country="World", sex="total", age from value (strip " years").
    m = dtype == "Not in school"
    tb.loc[m, "country"] = "World"
    tb.loc[m, "age"] = dvalue[m].str.replace(" years", "", regex=False)
    tb.loc[m, "_source"] = "not_in_school"

    # Not-in-school by ILO region rows: country from value, sex="total", age="5-14".
    m_region = dtype == "Not in school by region"
    tb.loc[m_region, "age"] = "5-14"
    tb.loc[m_region, "_source"] = "not_in_school"

    # Household chores rows: country="World", parse sex and age from value (e.g. "Girls 5-11").
    m = dtype == "Including household chores"
    tb.loc[m, "country"] = "World"
    tb.loc[m, "_source"] = "household_chores"
    parsed_hh = dvalue[m].str.extract(r"^(\w+)\s+(.+)$")
    tb.loc[m, "sex"] = parsed_hh[0].str.lower().values
    tb.loc[m, "age"] = parsed_hh[1].values

    # Child labor share 5-14 rows: country="World", age="5-14", sex from value.
    m = dtype == "Child labor share 5-14"
    tb.loc[m, "country"] = "World"
    tb.loc[m, "age"] = "5-14"
    tb.loc[m, "sex"] = dvalue[m].str.lower()
    tb.loc[m, "_source"] = "child_labor_share_5_14"

    # Sector by SDG region rows: parse sector from disaggregation_type, country from value.
    m = dtype.str.startswith("Sector ", na=False)
    tb.loc[m, "_source"] = "sector_by_region"
    tb.loc[m, "_sector"] = dtype[m].str.replace("Sector ", "", regex=False)

    return tb


# ── Helpers ───────────────────────────────────────────────────────────────────


def _country_from_region(tb: Table) -> Table:
    """Replace region_type + region with a single country column.

    Appends a grouping suffix (ILO/SDG/UNICEF) to disambiguate regions that appear
    under multiple classification systems (e.g. "Sub-Saharan Africa (ILO)").
    """
    tb = tb.copy()
    tb.loc[tb["region_type"] == "World total", "region"] = "World"
    for label, suffix in _REGION_ACRONYMS.items():
        mask = tb["region_type"] == label
        tb.loc[mask, "region"] = tb.loc[mask, "region"] + f" ({suffix})"
    return tb.drop(columns=["region_type"]).rename(columns={"region": "country"})


def _compute_age_5_14(tb: Table) -> Table:
    """Compute the 5-14 bracket by summing 5-11 + 12-14 numbers and back-calculating shares.

    Shares are back-calculated as: share_5_14 = number_5_14 / (pop_5_11 + pop_12_14),
    where pop = number / (share / 100).
    """
    tb_5_11 = tb[tb["age"] == "5-11"].sort_values(_JOIN_COLS).reset_index(drop=True)
    tb_12_14 = tb[tb["age"] == "12-14"].sort_values(_JOIN_COLS).reset_index(drop=True)

    num_cols = [c for c in tb.columns if c.startswith("number_")]
    share_cols = [c for c in tb.columns if c.startswith("share_")]

    tb_5_14 = tb_5_11[["country", "year", "sex"]].copy()

    # Sum numbers.
    for col in num_cols:
        tb_5_14[col] = tb_5_11[col].values + tb_12_14[col].values

    # Compute shares from inferred population.
    for col in share_cols:
        num_col = col.replace("share_", "number_")
        if num_col in num_cols:
            pop_5_11 = tb_5_11[num_col].values / (tb_5_11[col].values / 100)
            pop_12_14 = tb_12_14[num_col].values / (tb_12_14[col].values / 100)
            tb_5_14[col] = tb_5_14[num_col].values / (pop_5_11 + pop_12_14) * 100

    tb_5_14["age"] = "5-14"
    return tb_5_14


def _left_join_extra(tb: Table, extra: Table, src_col: str, dst_col: str) -> Table:
    """Left-join a single column from extra onto tb, filling NaNs in existing dst_col if present.

    Used to merge chart-derived shares (not-in-school, household chores) into the
    main table without overwriting values already populated from the annex.
    """
    extra = extra[_JOIN_COLS + [src_col]].dropna(subset=[src_col]).rename(columns={src_col: dst_col})
    tb = tb.merge(extra, on=_JOIN_COLS, how="left", suffixes=("", "_extra"))
    extra_col = f"{dst_col}_extra"
    if extra_col in tb.columns:
        tb[dst_col] = tb[dst_col].fillna(tb[extra_col])
        tb = tb.drop(columns=[extra_col])
    return tb


# ── Table builders ────────────────────────────────────────────────────────────


def _build_main_table(tb_cl: Table, tb_hw: Table, tb_trends: Table) -> Table:
    """Build the main child_labor output table.

    Combines three data sources:
      - Region tables (tb_cl, tb_hw): 2024 cross-sectional data by region × sex × age.
      - Trends table (tb_trends): time-series data (2000–2024) at aggregate level,
        plus chart-derived data (not-in-school, household chores, exact 5-14 shares).

    The pipeline:
      1. Extract not-in-school rows from annex — will become extra columns.
      2. Filter to region rows, build country names with grouping suffixes.
      3. Merge child_labor + hazardous_work side-by-side, assign year=2024.
      4. Separate chart-derived rows from regular trends by _source tag.
      5. Concat region data with trends; deduplicate (region data takes priority).
      6. Left-join annex not-in-school columns (World/2024 only).
      7. Compute 5-14 age bracket (sum numbers, back-calculate shares).
      8. Left-join chart not-in-school shares (pages 8/44).
      9. Left-join household chores shares (page 8).
      10. Harmonize country names.
    """
    tb_cl = tb_cl.copy()
    tb_hw = tb_hw.copy()

    # Verify expected region_type categories exist.
    expected_types = {_NOT_IN_SCHOOL, _BY_SECTOR, "World total"} | set(_REGION_ACRONYMS)
    for tb, name in [(tb_cl, "child_labor"), (tb_hw, "hazardous_work")]:
        actual_types = set(tb["region_type"].unique())
        assert actual_types == expected_types, (
            f"{name} has unexpected region_type values: {actual_types - expected_types}"
        )

    # 1. Extract not-in-school rows.
    cl_nis = tb_cl[tb_cl["region_type"] == _NOT_IN_SCHOOL].drop(columns=["region_type", "region"])
    hw_nis = tb_hw[tb_hw["region_type"] == _NOT_IN_SCHOOL].drop(columns=["region_type", "region"])

    # 2. Keep only region rows and build country column.
    keep = ~tb_cl["region_type"].isin([_NOT_IN_SCHOOL, _BY_SECTOR])
    tb_cl = _country_from_region(tb_cl[keep])
    tb_hw = _country_from_region(tb_hw[keep])

    # 3. Suffix value columns and merge child_labor + hazardous_work.
    tb_cl = tb_cl.rename(columns={"share": "share_child_labor", "number": "number_child_labor"})
    tb_hw = tb_hw.rename(columns={"share": "share_hazardous_work", "number": "number_hazardous_work"})
    tb = tb_cl.merge(tb_hw, on=["country", "sex", "age"], how="outer")
    tb["year"] = LATEST_YEAR

    # 4. Separate special rows (page 8/34 chart data) from regular trends rows.
    tb_trends = tb_trends.copy()
    source = tb_trends["_source"]
    tb_nis_chart = tb_trends[source == "not_in_school"].drop(columns=["_source", "_sector"])
    tb_hh = tb_trends[source == "household_chores"].drop(columns=["_source", "_sector"])
    tb_cl514 = tb_trends[source == "child_labor_share_5_14"].drop(columns=["_source", "_sector"])
    tb_trends = tb_trends[source == "trends"].drop(columns=["_source", "_sector"])

    # 5. Concat with trends (2016, 2020, 2024 aggregate-level rows).
    # Region data goes first so its rows are kept over trends duplicates (region data is more detailed).
    assert str(LATEST_YEAR) in tb_trends["year"].astype(str).values, f"LATEST_YEAR={LATEST_YEAR} not found in trends"
    tb = pr.concat([tb, tb_trends], ignore_index=True)
    tb = tb.drop_duplicates(subset=_JOIN_COLS, keep="first")

    # 6. Left-join not-in-school columns (only populated for country="World", year=2024).
    for nis, prefix in [(cl_nis, "child_labor"), (hw_nis, "hazardous_work")]:
        nis = nis.rename(
            columns={
                "share": f"share_{prefix}_not_in_school",
                "number": f"number_{prefix}_not_in_school",
            }
        )
        nis["country"] = "World"
        nis["year"] = LATEST_YEAR
        value_cols = [c for c in nis.columns if c not in _JOIN_COLS]
        tb = tb.merge(nis[_JOIN_COLS + value_cols], on=_JOIN_COLS, how="left")

    # 7. Compute 5-14 age bracket by summing 5-11 and 12-14.
    tb_5_14 = _compute_age_5_14(tb)
    # Overwrite child labor shares for 5-14 with report values (calculated shares have rounding errors).
    for _, row in tb_cl514.iterrows():
        mask = (
            (tb_5_14["country"] == row["country"]) & (tb_5_14["year"] == row["year"]) & (tb_5_14["sex"] == row["sex"])
        )
        tb_5_14.loc[mask, "share_child_labor"] = row["share_child_labor"]
    tb = pr.concat([tb, tb_5_14], ignore_index=True)

    # 8. Left-join not-in-school shares from chart data (pages 8 and 44).
    for src_col, dst_col in [
        ("share_child_labor", "share_child_labor_not_in_school"),
        ("share_hazardous_work", "share_hazardous_work_not_in_school"),
    ]:
        tb = _left_join_extra(tb, tb_nis_chart, src_col, dst_col)

    # 9. Add household chores column from page 8 chart data.
    tb["share_child_labor_incl_household_chores"] = None
    tb["share_child_labor_incl_household_chores"].metadata = tb["share_child_labor"].metadata.copy()
    tb = _left_join_extra(tb, tb_hh, "share_child_labor", "share_child_labor_incl_household_chores")

    # 10. Harmonize country names
    tb = paths.regions.harmonize_names(
        tb=tb,
        warn_on_unused_countries=False,
        warn_on_unknown_excluded_countries=False,
    )

    tb = tb.format(_JOIN_COLS, short_name="child_labor")

    return tb


def _build_sector_table(tb_cl: Table, tb_hw: Table, tb_trends: Table) -> Table:
    """Build the sector output table with child labor and hazardous work by economic sector.

    Combines two data sources:
      - Region tables (tb_cl, tb_hw): World-level sector distribution from annex
        (agriculture, industry, services) for both child labor and hazardous work.
      - Trends table (tb_trends): SDG-regional sector distribution from page 34 chart
        (child labor only).

    Share columns are rescaled to sum to exactly 100% per group.
    """
    tables = []
    for tb, prefix in [(tb_cl, "child_labor"), (tb_hw, "hazardous_work")]:
        assert _BY_SECTOR in tb["region_type"].values, f"'{_BY_SECTOR}' not found in {prefix}"
        tb = tb[tb["region_type"] == _BY_SECTOR].drop(columns=["region_type"]).copy()
        tb = tb.rename(
            columns={
                "region": "sector",
                "share": f"share_{prefix}",
                "number": f"number_{prefix}",
            }
        )
        tb["country"] = "World"
        tb["year"] = LATEST_YEAR
        tables.append(tb)

    tb = tables[0].merge(tables[1], on=["country", "year", "sector", "sex", "age"], how="outer")

    # Add sector-by-SDG-region data from page 34 chart.
    tb_sector_chart = tb_trends[tb_trends["_source"] == "sector_by_region"].copy()
    if len(tb_sector_chart) > 0:
        tb_sector_chart = tb_sector_chart.rename(columns={"_sector": "sector"})
        tb_sector_chart = tb_sector_chart[["country", "year", "sex", "age", "sector", "share_child_labor"]]
        # Keep only rows with actual data (chart only has 2024).
        tb_sector_chart = tb_sector_chart.dropna(subset=["share_child_labor"])
        tb = pr.concat([tb, tb_sector_chart], ignore_index=True)

    # Rescale share columns so they sum to exactly 100 per (country, year, sex, age) group.
    share_cols = [c for c in tb.columns if c.startswith("share_")]
    group_cols = ["country", "year", "sex", "age"]
    for col in share_cols:
        totals = tb.groupby(group_cols)[col].transform("sum")
        mask = totals.notna() & (totals != 0)
        tb.loc[mask, col] = tb.loc[mask, col] / totals[mask] * 100

    # Harmonize country names
    tb = paths.regions.harmonize_names(
        tb=tb,
        warn_on_unused_countries=False,
        warn_on_unknown_excluded_countries=False,
    )

    tb = tb.format(["country", "year", "sector", "sex", "age"], short_name="sector")

    return tb
