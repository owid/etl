import pandas as pd
from owid.catalog import Dataset

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


def build_df(ds_hmd: Dataset, ds_wmd: Dataset, ds_kobak: Dataset) -> pd.DataFrame:
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


def _build_estimates_df(ds_hmd: Dataset, ds_wmd: Dataset) -> pd.DataFrame:
    # Build dataframe
    df_hmd = pd.DataFrame(ds_hmd["hmd_stmf"])
    df_hmd = df_hmd.rename(columns={"week": "time"}).assign(**{"time_unit": "weekly"})
    df_wmd = pd.DataFrame(ds_wmd["wmd"])
    df_wmd = df_wmd[-df_wmd["entity"].isin(set(df_hmd["entity"]))]
    df_estimates = pd.concat([df_hmd, df_wmd], ignore_index=True)
    # Run checks
    if (ds := df_estimates[COLUMNS_IDX].value_counts()).max() > 1:
        raise ValueError(f"Unexpected duplicates {ds[ds>1]}")
    return df_estimates


def _build_projections_df(ds_kobak: Dataset) -> pd.DataFrame:
    df_kobak = pd.DataFrame(ds_kobak["xm_karlinsky_kobak"])
    df_kobak_age = pd.DataFrame(ds_kobak["xm_karlinsky_kobak_by_age"])
    df_proj = pd.concat([df_kobak, df_kobak_age], ignore_index=True)
    if (ds := df_proj[COLUMNS_IDX].value_counts()).max() > 1:
        raise ValueError(f"Unexpected duplicates {ds[ds>1]}")
    return df_proj


def _merge_dfs(df_estimates: pd.DataFrame, df_proj: pd.DataFrame) -> pd.DataFrame:
    # Merge estimates and projections
    df = df_proj.merge(df_estimates, on=COLUMNS_IDX, how="outer")
    return df


def _api_check(df: pd.DataFrame) -> None:
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
