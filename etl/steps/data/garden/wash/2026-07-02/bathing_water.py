"""Load a snapshot and create a garden dataset."""

import pandas as pd

from etl.helpers import PathFinder

paths = PathFinder(__file__)

FILE_NAME = "eea_t_bathing-water-status_p_1990-2025_v01_r00/bw_assessment_eea_datahub_1990_2025.xlsx"

COASTAL_TYPES = {"coastalBathingWater", "transitionalBathingWater"}
INLAND_TYPES = {"lakeBathingWater", "riverBathingWater"}
# Exclude unclassified sites from totals; include legacy "Good or Sufficient" in totals but not in excellent.
CLASSIFIED_QUALITIES = {"1 - Excellent", "2 - Good", "3 - Sufficient", "4 - Poor", "3 - Good or Sufficient"}
EXCELLENT_QUALITY = "1 - Excellent"


def _agg(df: pd.DataFrame, water_types: set, quality_filter: set | None = None) -> pd.DataFrame:
    """Count bathing water sites by country and year, optionally filtered by quality."""
    mask = df["bathingWaterType"].isin(water_types)
    if quality_filter is not None:
        mask &= df["quality"].isin(quality_filter)
    return (
        df[mask]
        .groupby(["country", "year"], observed=True)["bathingWaterIdentifier"]
        .count()
        .reset_index()
    )


def make_nr_c(df: pd.DataFrame) -> pd.DataFrame:
    """Number of classified coastal bathing water sites."""
    return _agg(df, COASTAL_TYPES, CLASSIFIED_QUALITIES).rename(
        columns={"bathingWaterIdentifier": "sdg_14_40_nr_c"}
    )


def make_nr_ex_c(df: pd.DataFrame) -> pd.DataFrame:
    """Number of coastal bathing water sites with excellent quality."""
    return _agg(df, COASTAL_TYPES, {EXCELLENT_QUALITY}).rename(
        columns={"bathingWaterIdentifier": "sdg_14_40_nr_ex_c"}
    )


def make_pct_ex_c(df: pd.DataFrame) -> pd.DataFrame:
    """Percentage of coastal bathing water sites with excellent quality."""
    total = make_nr_c(df).rename(columns={"sdg_14_40_nr_c": "_total"})
    excellent = make_nr_ex_c(df).rename(columns={"sdg_14_40_nr_ex_c": "_excellent"})
    merged = total.merge(excellent, on=["country", "year"], how="left")
    merged["sdg_14_40_pct_ex_c"] = merged["_excellent"] / merged["_total"] * 100
    return merged[["country", "year", "sdg_14_40_pct_ex_c"]]


def make_nr_in(df: pd.DataFrame) -> pd.DataFrame:
    """Number of classified inland bathing water sites."""
    return _agg(df, INLAND_TYPES, CLASSIFIED_QUALITIES).rename(
        columns={"bathingWaterIdentifier": "sdg_14_40_nr_in"}
    )


def make_nr_ex_in(df: pd.DataFrame) -> pd.DataFrame:
    """Number of inland bathing water sites with excellent quality."""
    return _agg(df, INLAND_TYPES, {EXCELLENT_QUALITY}).rename(
        columns={"bathingWaterIdentifier": "sdg_14_40_nr_ex_in"}
    )


def make_pct_ex_in(df: pd.DataFrame) -> pd.DataFrame:
    """Percentage of inland bathing water sites with excellent quality."""
    total = make_nr_in(df).rename(columns={"sdg_14_40_nr_in": "_total"})
    excellent = make_nr_ex_in(df).rename(columns={"sdg_14_40_nr_ex_in": "_excellent"})
    merged = total.merge(excellent, on=["country", "year"], how="left")
    merged["sdg_14_40_pct_ex_in"] = merged["_excellent"] / merged["_total"] * 100
    return merged[["country", "year", "sdg_14_40_pct_ex_in"]]


def run() -> None:
    snap = paths.load_snapshot("bathing_water.zip")

    with snap.extracted() as archive:
        tb = archive.read(FILE_NAME, sheet_name="bw_assessment_datahub_1990_2025")

    df = tb[["countryCode", "bathingWaterIdentifier", "bathingWaterType", "season", "quality"]].rename(
        columns={"countryCode": "country", "season": "year"}
    )

    indicator_tables = [
        make_nr_c(df),
        make_nr_ex_c(df),
        make_pct_ex_c(df),
        make_nr_in(df),
        make_nr_ex_in(df),
        make_pct_ex_in(df),
    ]

    combined = indicator_tables[0]
    for t in indicator_tables[1:]:
        combined = combined.merge(t, on=["country", "year"], how="outer")

    tb = snap.read_from_df(combined)
    tb = paths.regions.harmonize_names(tb, country_col="country", countries_file=paths.country_mapping_path)
    tb = tb.format(["country", "year"])

    ds_garden = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)
    ds_garden.save()
