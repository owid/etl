import owid.catalog.processing as pr
from owid.catalog import Dataset, Table

from etl.data_helpers.misc import check_known_columns, check_values_in_column

# Expected columns
COLUMNS_YEAR_EXPECTED = [
    "_2010",
    "_2011",
    "_2012",
    "_2013",
    "_2014",
    "_2015",
    "_2016",
    "_2017",
    "_2018",
    "_2019",
    "_2020",
    "_2021",
    "_2022",
    "_2023",
    "_2024",
    "_2025",
    "_2026",
]
COLUMNS_EXPECTED = [
    "entity",
    "time",
    "time_unit",
    "age",
    "baseline_proj",
    "baseline_proj_21",
    "baseline_proj_22",
    "baseline_proj_23",
] + COLUMNS_YEAR_EXPECTED
# Index columns
COLUMNS_IDX = [
    "entity",
    "time",
    "time_unit",
    "age",
]
# Rename mapping for year columns
COLUMN_YEAR_RENAME = {col: col.replace("_", "") for col in COLUMNS_YEAR_EXPECTED}


def build_df(ds_hmd: Dataset, ds_wmd: Dataset, ds_kobak: Dataset) -> Table:
    # Load estimates
    df_estimates = _build_estimates_df(ds_hmd, ds_wmd)
    # Load projections
    df_proj = _build_projections_df(ds_kobak)
    # Merge estimates and projections
    df = _merge_dfs(df_estimates, df_proj)
    # API checks
    _api_check(df)
    # Rename year columns
    df = df.rename(columns=COLUMN_YEAR_RENAME)
    return df


def _build_estimates_df(ds_hmd: Dataset, ds_wmd: Dataset) -> Table:
    """Some country data comes from HMD, other from WMD.

    We give priority to HMD data: If a country has data in both datasets, we keep the HMD data.
    """
    # Build dataframe
    tb_hmd = ds_hmd.read("hmd_stmf", reset_index=True)
    tb_hmd = tb_hmd.rename(columns={"week": "time"}).assign(**{"time_unit": "weekly"})
    tb_wmd = ds_wmd.read("wmd", reset_index=True)
    tb_wmd = tb_wmd[-tb_wmd["entity"].isin(set(tb_hmd["entity"]))]
    df_estimates = pr.concat([tb_hmd, tb_wmd], ignore_index=True)
    # Run checks
    if (ds := df_estimates[COLUMNS_IDX].value_counts()).max() > 1:
        raise ValueError(f"Unexpected duplicates {ds[ds > 1]}")
    return df_estimates


def _build_projections_df(ds_kobak: Dataset) -> Table:
    df_kobak = ds_kobak.read("xm_karlinsky_kobak", reset_index=True)
    df_kobak_age = ds_kobak.read("xm_karlinsky_kobak_by_age", reset_index=True)
    df_proj = pr.concat([df_kobak, df_kobak_age], ignore_index=True)
    if (ds := df_proj[COLUMNS_IDX].value_counts()).max() > 1:
        raise ValueError(f"Unexpected duplicates {ds[ds > 1]}")
    return df_proj


def _merge_dfs(df_estimates: Table, df_proj: Table) -> Table:
    # Merge estimates and projections
    df = pr.merge(df_proj, df_estimates, on=COLUMNS_IDX, how="outer")
    return df


def _api_check(df: Table) -> None:
    # Check columns are as expected
    check_known_columns(df, COLUMNS_EXPECTED)
    # Check values in `age`
    check_values_in_column(df, "age", ["all_ages", "0_14", "15_64", "65_74", "75_84", "85p"])
    # Check `time` and `time_unit`
    check_values_in_column(df, "time", list(range(1, 54)))
    check_values_in_column(df, "time_unit", ["monthly", "weekly"])
    # If `time_unit`=="monthly", time should be in range(1, 13).
    check_values_in_column(df[df["time_unit"] == "monthly"], "time", list(range(1, 13)))
    # If `time_unit`=="weekly", time should be in range(1, 54).
    check_values_in_column(df[df["time_unit"] == "weekly"], "time", list(range(1, 54)))
