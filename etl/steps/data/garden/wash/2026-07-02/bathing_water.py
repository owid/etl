"""Load a snapshot and create a garden dataset."""

import pandas as pd
from owid.catalog import processing as pr
from structlog import get_logger

from etl.helpers import PathFinder

log = get_logger()

paths = PathFinder(__file__)

FILE_NAME = "eea_t_bathing-water-status_p_1990-2025_v01_r00/bw_assessment_eea_datahub_1990_2025.xlsx"

COASTAL_TYPES = {"coastalBathingWater", "transitionalBathingWater"}
INLAND_TYPES = {"lakeBathingWater", "riverBathingWater"}
# SDG 14.40 denominator = all registered sites including "0 - Not classified".
# Only the excellent numerator filters on quality.
EXCELLENT_QUALITY = "1 - Excellent"

EU27_CODES = {
    "AT",
    "BE",
    "BG",
    "HR",
    "CY",
    "CZ",
    "DK",
    "EE",
    "FI",
    "FR",
    "DE",
    "EL",
    "HU",
    "IE",
    "IT",
    "LV",
    "LT",
    "LU",
    "MT",
    "NL",
    "PL",
    "PT",
    "RO",
    "SK",
    "SI",
    "ES",
    "SE",
}
EU27_NAME = "European Union (27)"


def _agg(df: pd.DataFrame, water_types: set, quality_filter: set | None = None) -> pd.DataFrame:
    """Count bathing water sites by country and year, optionally filtered by quality."""
    mask = df["bathingWaterType"].isin(water_types)
    if quality_filter is not None:
        mask &= df["quality"].isin(quality_filter)
    return df[mask].groupby(["country", "year"], observed=True)["bathingWaterIdentifier"].count().reset_index()


def make_nr_c(df: pd.DataFrame) -> pd.DataFrame:
    """Total number of coastal bathing water sites (all quality statuses)."""
    return _agg(df, COASTAL_TYPES).rename(columns={"bathingWaterIdentifier": "sdg_14_40_nr_c"})


def make_nr_ex_c(df: pd.DataFrame) -> pd.DataFrame:
    """Number of coastal bathing water sites with excellent quality."""
    return _agg(df, COASTAL_TYPES, {EXCELLENT_QUALITY}).rename(columns={"bathingWaterIdentifier": "sdg_14_40_nr_ex_c"})


def make_pct_ex_c(df: pd.DataFrame) -> pd.DataFrame:
    """Percentage of coastal bathing water sites with excellent quality."""
    total = make_nr_c(df).rename(columns={"sdg_14_40_nr_c": "_total"})
    excellent = make_nr_ex_c(df).rename(columns={"sdg_14_40_nr_ex_c": "_excellent"})
    merged = total.merge(excellent, on=["country", "year"], how="left")
    merged["_excellent"] = merged["_excellent"].fillna(0)
    merged["sdg_14_40_pct_ex_c"] = merged["_excellent"] / merged["_total"] * 100
    return merged[["country", "year", "sdg_14_40_pct_ex_c"]]


def make_nr_in(df: pd.DataFrame) -> pd.DataFrame:
    """Total number of inland bathing water sites (all quality statuses)."""
    return _agg(df, INLAND_TYPES).rename(columns={"bathingWaterIdentifier": "sdg_14_40_nr_in"})


def make_nr_ex_in(df: pd.DataFrame) -> pd.DataFrame:
    """Number of inland bathing water sites with excellent quality."""
    return _agg(df, INLAND_TYPES, {EXCELLENT_QUALITY}).rename(columns={"bathingWaterIdentifier": "sdg_14_40_nr_ex_in"})


def make_pct_ex_in(df: pd.DataFrame) -> pd.DataFrame:
    """Percentage of inland bathing water sites with excellent quality."""
    total = make_nr_in(df).rename(columns={"sdg_14_40_nr_in": "_total"})
    excellent = make_nr_ex_in(df).rename(columns={"sdg_14_40_nr_ex_in": "_excellent"})
    merged = total.merge(excellent, on=["country", "year"], how="left")
    merged["_excellent"] = merged["_excellent"].fillna(0)
    merged["sdg_14_40_pct_ex_in"] = merged["_excellent"] / merged["_total"] * 100
    return merged[["country", "year", "sdg_14_40_pct_ex_in"]]


def make_eu27_aggregate(combined: pd.DataFrame) -> pd.DataFrame:
    """Sum counts across EU27 member states per year, then derive percentages."""
    eu = combined[combined["country"].isin(EU27_CODES)].copy()
    count_cols = ["sdg_14_40_nr_c", "sdg_14_40_nr_ex_c", "sdg_14_40_nr_in", "sdg_14_40_nr_ex_in"]
    agg = eu.groupby("year")[count_cols].sum().reset_index()
    agg["sdg_14_40_pct_ex_c"] = agg["sdg_14_40_nr_ex_c"] / agg["sdg_14_40_nr_c"] * 100
    agg["sdg_14_40_pct_ex_in"] = agg["sdg_14_40_nr_ex_in"] / agg["sdg_14_40_nr_in"] * 100
    agg["country"] = EU27_NAME
    return agg


def sanity_check_inputs(df: pd.DataFrame) -> None:
    # All EU27 member codes must be present in the raw data — the aggregate would silently use a partial set otherwise.
    missing_eu27 = EU27_CODES - set(df["country"].unique())
    assert not missing_eu27, f"EU27 member codes missing from source: {missing_eu27}"

    # The excellent-quality label we filter on must exist in the data.
    assert EXCELLENT_QUALITY in df["quality"].values, f"Quality label '{EXCELLENT_QUALITY}' not found in source data."

    # All four expected water-type categories must be present.
    known_types = COASTAL_TYPES | INLAND_TYPES
    missing_types = known_types - set(df["bathingWaterType"].unique())
    assert not missing_types, f"Expected water type categories missing from source: {missing_types}"


def sanity_check_outputs(combined: pd.DataFrame) -> None:
    # No duplicate (country, year) pairs.
    assert not combined.duplicated(subset=["country", "year"]).any(), "Duplicate (country, year) rows in output."

    # Percentages must be in [0, 100].
    for col in ["sdg_14_40_pct_ex_c", "sdg_14_40_pct_ex_in"]:
        vals = combined[col].dropna()
        assert (vals >= 0).all() and (vals <= 100).all(), f"{col} has values outside [0, 100]."

    # Count columns must be non-negative.
    for col in ["sdg_14_40_nr_c", "sdg_14_40_nr_ex_c", "sdg_14_40_nr_in", "sdg_14_40_nr_ex_in"]:
        vals = combined[col].dropna()
        assert (vals >= 0).all(), f"{col} has negative values."

    # EU27 aggregate count must be ≥ the largest individual member state for each count column.
    eu27_rows = combined[combined["country"] == EU27_NAME]
    member_rows = combined[combined["country"].isin(EU27_CODES)]
    for col in ["sdg_14_40_nr_c", "sdg_14_40_nr_ex_c", "sdg_14_40_nr_in", "sdg_14_40_nr_ex_in"]:
        eu27_max = eu27_rows[col].max()
        member_max = member_rows[col].max()
        if pd.notna(eu27_max) and pd.notna(member_max):
            assert eu27_max >= member_max, (
                f"EU27 aggregate for {col} ({eu27_max:.0f}) is smaller than the largest member state ({member_max:.0f})."
            )

    # The most recent year encoded in the source file path must be present in the output.
    expected_latest_year = int(FILE_NAME.split("_")[-1].split(".")[0].split("-")[-1])
    assert expected_latest_year in combined["year"].values, (
        f"Expected latest year {expected_latest_year} not found in output."
    )

    # Soft signal: flag countries that dropped to zero in the latest year.
    latest_year = combined["year"].max()
    zero_pct_c = combined.loc[
        (combined["year"] == latest_year) & (combined["sdg_14_40_pct_ex_c"] == 0), "country"
    ].tolist()
    if zero_pct_c:
        log.warning(f"Countries with 0% excellent coastal quality in {latest_year}: {zero_pct_c}")


def run() -> None:
    snap = paths.load_snapshot("bathing_water.zip")

    with snap.extracted() as archive:
        tb = archive.read(FILE_NAME, sheet_name="bw_assessment_datahub_1990_2025")

    df = tb[["countryCode", "bathingWaterIdentifier", "bathingWaterType", "season", "quality"]].rename(
        columns={"countryCode": "country", "season": "year"}
    )

    sanity_check_inputs(df)

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

    # A country/year with sites but zero excellent ones has no row from make_nr_ex_*,
    # so the outer merge leaves the count as NaN. Fill with 0 where the total is known.
    combined.loc[combined["sdg_14_40_nr_c"].notna(), "sdg_14_40_nr_ex_c"] = combined.loc[
        combined["sdg_14_40_nr_c"].notna(), "sdg_14_40_nr_ex_c"
    ].fillna(0)
    combined.loc[combined["sdg_14_40_nr_in"].notna(), "sdg_14_40_nr_ex_in"] = combined.loc[
        combined["sdg_14_40_nr_in"].notna(), "sdg_14_40_nr_ex_in"
    ].fillna(0)

    eu27 = make_eu27_aggregate(combined)
    combined = pr.concat([combined, eu27], ignore_index=True)

    sanity_check_outputs(combined)

    tb = snap.read_from_df(combined)
    tb = paths.regions.harmonize_names(tb, country_col="country", countries_file=paths.country_mapping_path)
    tb = tb.format(["country", "year"])

    ds_garden = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)
    ds_garden.save()
