"""Load a meadow dataset and create a garden dataset."""

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


def _to_long(tb: Table) -> Table:
    """Reshape a wide region table into long format with sex and age dimensions.

    Columns like `total_5_11_pct` become rows with sex="total", age="5-11", and two
    value columns: `share` (from pct) and `number` (from no).
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
    """Reshape the trends table into long format with year as dimension.

    Columns like `child_labor_2016_pct` become year="2016" with four value columns:
    `share_child_labor`, `share_hazardous_work`, `number_child_labor`, `number_hazardous_work`.
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

    # Map disaggregation_type/value to country, sex, age.
    tb["country"] = tb["disaggregation_value"]
    tb["sex"] = "total"
    tb["age"] = "5-17"

    # World total: country="World".
    tb.loc[tb["disaggregation_type"] == "World total", "country"] = "World"

    # Sex rows: country="World", sex from value.
    is_sex = tb["disaggregation_type"] == "Sex"
    tb.loc[is_sex, "country"] = "World"
    tb.loc[is_sex, "sex"] = tb.loc[is_sex, "disaggregation_value"].str.lower()

    # Age rows: country="World", age from value (strip " years").
    is_age = tb["disaggregation_type"] == "Age"
    tb.loc[is_age, "country"] = "World"
    tb.loc[is_age, "age"] = tb.loc[is_age, "disaggregation_value"].str.replace(" years", "", regex=False)

    # Disambiguate regions that appear under multiple groupings (e.g. Sub-Saharan Africa).
    for label, suffix in _REGION_ACRONYMS.items():
        mask = tb["disaggregation_type"] == label
        tb.loc[mask, "country"] = tb.loc[mask, "country"] + f" ({suffix})"

    # Not-in-school rows: country="World", sex="total", age from value (strip " years").
    is_nis = tb["disaggregation_type"] == "Not in school"
    tb.loc[is_nis, "country"] = "World"
    tb.loc[is_nis, "age"] = tb.loc[is_nis, "disaggregation_value"].str.replace(" years", "", regex=False)

    # Household chores rows: country="World", parse sex and age from value (e.g. "Girls 5-11").
    is_hh = tb["disaggregation_type"] == "Including household chores"
    tb.loc[is_hh, "country"] = "World"
    parsed_hh = tb.loc[is_hh, "disaggregation_value"].str.extract(r"^(\w+)\s+(.+)$")
    tb.loc[is_hh, "sex"] = parsed_hh[0].str.lower().values
    tb.loc[is_hh, "age"] = parsed_hh[1].values

    # Child labor share 5-14 rows: country="World", age="5-14", sex from value.
    is_cl514 = tb["disaggregation_type"] == "Child labor share 5-14"
    tb.loc[is_cl514, "country"] = "World"
    tb.loc[is_cl514, "age"] = "5-14"
    tb.loc[is_cl514, "sex"] = tb.loc[is_cl514, "disaggregation_value"].str.lower()

    # Sector by SDG region rows: parse sector from disaggregation_type, country from value.
    is_sector = tb["disaggregation_type"].str.startswith("Sector ", na=False)
    tb.loc[is_sector, "country"] = tb.loc[is_sector, "disaggregation_value"]
    tb.loc[is_sector, "sex"] = "total"
    tb.loc[is_sector, "age"] = "5-17"
    tb.loc[is_sector, "_sector"] = tb.loc[is_sector, "disaggregation_type"].str.replace("Sector ", "", regex=False)

    # Tag source type so _build_main_table can separate special rows.
    tb["_source"] = "trends"
    tb.loc[is_nis, "_source"] = "not_in_school"
    tb.loc[is_hh, "_source"] = "household_chores"
    tb.loc[is_cl514, "_source"] = "child_labor_share_5_14"
    tb.loc[is_sector, "_source"] = "sector_by_region"

    tb = tb.drop(columns=["disaggregation_type", "disaggregation_value"])
    tb["year"] = tb["year"].astype(int)

    return tb


def _apply_country_names(tb: Table) -> None:
    """Replace region_type + region with a single country column (in-place)."""
    tb.loc[tb["region_type"] == "World total", "region"] = "World"
    for label, suffix in _REGION_ACRONYMS.items():
        mask = tb["region_type"] == label
        tb.loc[mask, "region"] = tb.loc[mask, "region"] + f" ({suffix})"
    tb.drop(columns=["region_type"], inplace=True)
    tb.rename(columns={"region": "country"}, inplace=True)


def _build_main_table(tb_cl: Table, tb_hw: Table, tb_trends: Table) -> Table:
    """Build the main child_labor table by combining region data, trends, and not-in-school.

    Steps:
      1. Extract not-in-school rows (World-only) — will become extra columns.
      2. Filter to region rows only, build country names with acronyms.
      3. Merge child_labor + hazardous_work side-by-side, add year=2024.
      4. Concat with trends (2016/2020/2024 aggregate rows).
      5. Left-join not-in-school columns for World/2024 rows.
    """
    tb_cl = tb_cl.copy()
    tb_hw = tb_hw.copy()

    # Verify expected region_type categories exist.
    expected_types = {_NOT_IN_SCHOOL, _BY_SECTOR, "World total"} | set(_REGION_ACRONYMS)
    for tb, name in [(tb_cl, "child_labor"), (tb_hw, "hazardous_work")]:
        actual_types = set(tb["region_type"].unique())
        assert (
            actual_types == expected_types
        ), f"{name} has unexpected region_type values: {actual_types - expected_types}"

    # 1. Extract not-in-school rows.
    cl_nis = tb_cl[tb_cl["region_type"] == _NOT_IN_SCHOOL].drop(columns=["region_type", "region"])
    hw_nis = tb_hw[tb_hw["region_type"] == _NOT_IN_SCHOOL].drop(columns=["region_type", "region"])

    # 2. Keep only region rows and build country column.
    tb_cl = tb_cl[~tb_cl["region_type"].isin([_NOT_IN_SCHOOL, _BY_SECTOR])].copy()
    tb_hw = tb_hw[~tb_hw["region_type"].isin([_NOT_IN_SCHOOL, _BY_SECTOR])].copy()
    _apply_country_names(tb_cl)
    _apply_country_names(tb_hw)

    # 3. Suffix value columns and merge child_labor + hazardous_work.
    tb_cl = tb_cl.rename(columns={"share": "share_child_labor", "number": "number_child_labor"})
    tb_hw = tb_hw.rename(columns={"share": "share_hazardous_work", "number": "number_hazardous_work"})
    tb = tb_cl.merge(tb_hw, on=["country", "sex", "age"], how="outer")
    tb["year"] = LATEST_YEAR

    # 4. Separate special rows (page 8/34 chart data) from regular trends rows.
    tb_trends = tb_trends.copy()
    tb_nis_chart = tb_trends[tb_trends["_source"] == "not_in_school"].drop(columns=["_source", "_sector"])
    tb_hh = tb_trends[tb_trends["_source"] == "household_chores"].drop(columns=["_source", "_sector"])
    tb_cl514 = tb_trends[tb_trends["_source"] == "child_labor_share_5_14"].drop(columns=["_source", "_sector"])
    tb_trends = tb_trends[tb_trends["_source"] == "trends"].drop(columns=["_source", "_sector"])

    # 5. Concat with trends (2016, 2020, 2024 aggregate-level rows).
    # Region data goes first so its rows are kept over trends duplicates (region data is more detailed).
    assert str(LATEST_YEAR) in tb_trends["year"].astype(str).values, f"LATEST_YEAR={LATEST_YEAR} not found in trends"
    tb = pr.concat([tb, tb_trends], ignore_index=True)
    tb = tb.drop_duplicates(subset=["country", "year", "sex", "age"], keep="first")

    # 6. Left-join not-in-school columns (only populated for country="World", year=2024).
    join_cols = ["country", "year", "sex", "age"]
    for nis, prefix in [(cl_nis, "child_labor"), (hw_nis, "hazardous_work")]:
        nis = nis.rename(
            columns={
                "share": f"share_{prefix}_not_in_school",
                "number": f"number_{prefix}_not_in_school",
            }
        )
        nis["country"] = "World"
        nis["year"] = LATEST_YEAR
        value_cols = [c for c in nis.columns if c not in join_cols]
        tb = tb.merge(nis[join_cols + value_cols], on=join_cols, how="left")

    # 7. Compute 5-14 age bracket by summing 5-11 and 12-14.
    tb_5_11 = tb[tb["age"] == "5-11"].sort_values(["country", "year", "sex"]).reset_index(drop=True)
    tb_12_14 = tb[tb["age"] == "12-14"].sort_values(["country", "year", "sex"]).reset_index(drop=True)
    num_cols = [c for c in tb.columns if c.startswith("number_")]
    share_cols = [c for c in tb.columns if c.startswith("share_")]
    tb_5_14 = tb_5_11[["country", "year", "sex"]].copy()
    # Sum numbers.
    for col in num_cols:
        tb_5_14[col] = tb_5_11[col].values + tb_12_14[col].values
    # Compute shares: share_5_14 = number_5_14 / (pop_5_11 + pop_12_14), where pop = number / (share / 100).
    for col in share_cols:
        num_col = col.replace("share_", "number_")
        if num_col in num_cols:
            pop_5_11 = tb_5_11[num_col].values / (tb_5_11[col].values / 100)
            pop_12_14 = tb_12_14[num_col].values / (tb_12_14[col].values / 100)
            tb_5_14[col] = tb_5_14[num_col].values / (pop_5_11 + pop_12_14) * 100
    tb_5_14["age"] = "5-14"
    # Overwrite child labor shares for 5-14 with report values (calculated shares have rounding errors).
    if len(tb_cl514) > 0:
        for _, row in tb_cl514.iterrows():
            mask = (
                (tb_5_14["country"] == row["country"])
                & (tb_5_14["year"] == row["year"])
                & (tb_5_14["sex"] == row["sex"])
            )
            tb_5_14.loc[mask, "share_child_labor"] = row["share_child_labor"]
    tb = pr.concat([tb, tb_5_14], ignore_index=True)

    # 8. Left-join not-in-school shares from page 8 chart (for 5-14 and 15-17 age groups).
    if len(tb_nis_chart) > 0:
        nis_shares = tb_nis_chart[["country", "year", "sex", "age", "share_child_labor"]].rename(
            columns={"share_child_labor": "share_child_labor_not_in_school"}
        )
        tb = tb.merge(nis_shares, on=join_cols, how="left", suffixes=("", "_chart"))
        # Fill in chart values where annex values are missing.
        if "share_child_labor_not_in_school_chart" in tb.columns:
            tb["share_child_labor_not_in_school"] = tb["share_child_labor_not_in_school"].fillna(
                tb["share_child_labor_not_in_school_chart"]
            )
            tb = tb.drop(columns=["share_child_labor_not_in_school_chart"])

    # 9. Add household chores column from page 8 chart data.
    tb["share_child_labor_incl_household_chores"] = None
    tb["share_child_labor_incl_household_chores"].metadata = tb["share_child_labor"].metadata.copy()
    if len(tb_hh) > 0:
        hh = tb_hh[["country", "year", "sex", "age", "share_child_labor"]].rename(
            columns={"share_child_labor": "share_child_labor_incl_household_chores"}
        )
        tb = tb.merge(hh, on=join_cols, how="left", suffixes=("", "_hh"))
        if "share_child_labor_incl_household_chores_hh" in tb.columns:
            tb["share_child_labor_incl_household_chores"] = tb["share_child_labor_incl_household_chores"].fillna(
                tb["share_child_labor_incl_household_chores_hh"]
            )
            tb = tb.drop(columns=["share_child_labor_incl_household_chores_hh"])

    # 10. Harmonize country names
    tb = paths.regions.harmonize_names(
        tb=tb,
        warn_on_unused_countries=False,
        warn_on_unknown_excluded_countries=False,
    )

    tb = tb.format(["country", "year", "sex", "age"], short_name="child_labor")

    return tb


def _build_sector_table(tb_cl: Table, tb_hw: Table, tb_trends: Table) -> Table:
    """Build the sector table from sector rows of both region tables.

    Keeps only rows where region_type is "by sector of economic activity",
    renames region → sector, merges child_labor + hazardous_work side-by-side.
    Also adds sector-by-SDG-region data from page 34 chart (via trends).
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
        tb_sector_chart = tb_sector_chart.rename(
            columns={"_sector": "sector", "share_child_labor": "share_child_labor"}
        )
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
