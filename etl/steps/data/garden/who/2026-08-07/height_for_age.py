"""Splice the WHO under-fives standards and the 5-19 reference into one height-for-age table.

WHO publishes height-for-age in two separate products on two different age grids: the
Child Growth Standards (2006) by day of age up to day 1856, and the Growth Reference
(2007) by month of age from month 61. WHO's own conversion is 1 month = 30.4375 days, so
month 61 falls on day 1857 and the two grids abut with no overlap and no gap.

Both grids are kept at their native resolution rather than resampled onto a common one:
after age 5 the curves are close to linear, so interpolating would invent points without
adding information.
"""

import numpy as np
from owid.catalog import Table
from owid.catalog import processing as pr
from scipy.stats import norm

from etl.helpers import PathFinder

paths = PathFinder(__file__)

# WHO's stated conversion between months and days of age.
DAYS_PER_MONTH = 30.4375

# Published percentile columns, mapped to the percentile they represent. Note that WHO
# labels the 0.1st percentile "P01" and the 99.9th "P999" -- not the 1st and 99.9th.
PERCENTILES = {
    "p01": 0.1,
    "p1": 1,
    "p3": 3,
    "p5": 5,
    "p10": 10,
    "p15": 15,
    "p25": 25,
    "p50": 50,
    "p75": 75,
    "p85": 85,
    "p90": 90,
    "p95": 95,
    "p97": 97,
    "p99": 99,
    "p999": 99.9,
}

# Published z-score columns, mapped to the number of standard deviations they represent.
ZSCORES = {
    "sd5neg": -5,
    "sd4neg": -4,
    "sd3neg": -3,
    "sd2neg": -2,
    "sd1neg": -1,
    "sd0": 0,
    "sd1": 1,
    "sd2": 2,
    "sd3": 3,
    "sd4": 4,
}

COLUMNS_RENAME = {
    "l": "lms_l_skewness",
    "m": "lms_m_median",
    "s": "lms_s_coefficient_of_variation",
    **{col: f"height_percentile_{str(pct).replace('.', '_')}" for col, pct in PERCENTILES.items()},
    **{col: f"height_sd_{'minus_' if z < 0 else 'plus_' if z > 0 else ''}{abs(z)}" for col, z in ZSCORES.items()},
}

SEXES_RENAME = {"boys": "Boys", "girls": "Girls"}

# The two documented steps down in the curves, as (day before, day after, description).
# Both are in the source data and are not artifacts of our processing.
DISCONTINUITIES = [
    (730, 731, "measurement switches from recumbent length to standing height at age 2"),
    (1856, 1857, "the under-fives standards give way to the 5-19 reference at age 5"),
]

# Tolerance for reproducing WHO's published values from L, M and S. The published tables
# are rounded to three decimals, so agreement should be within half of the last digit.
TOLERANCE = 0.001


def run() -> None:
    #
    # Load inputs.
    #
    ds_meadow = paths.load_dataset("height_for_age")
    tb_percentiles_under_5 = ds_meadow.read("percentiles_under_5")
    tb_zscores_under_5 = ds_meadow.read("zscores_under_5")
    tb_percentiles_5_to_19 = ds_meadow.read("percentiles_5_to_19")
    tb_zscores_5_to_19 = ds_meadow.read("zscores_5_to_19")

    sanity_check_inputs(tb_percentiles_under_5, tb_zscores_under_5, tb_percentiles_5_to_19, tb_zscores_5_to_19)

    #
    # Process data.
    #
    # Combine percentiles and z-scores within each age group, then put both age groups on
    # a common age-in-days axis.
    tb_under_5 = combine_measures(tb_percentiles_under_5, tb_zscores_under_5, age_column="day")
    tb_under_5["age_days"] = tb_under_5["day"].astype(int)
    tb_under_5 = tb_under_5.drop(columns=["day"])

    tb_5_to_19 = combine_measures(tb_percentiles_5_to_19, tb_zscores_5_to_19, age_column="month")
    tb_5_to_19["age_days"] = (tb_5_to_19["month"] * DAYS_PER_MONTH).round().astype(int)
    tb_5_to_19 = tb_5_to_19.drop(columns=["month"])

    tb = pr.concat([tb_under_5, tb_5_to_19], ignore_index=True)

    # Derive the standard deviation uniformly. WHO ships it only in the 5-19 files, where
    # sanity_check_inputs confirms it equals M * S.
    tb["standard_deviation"] = tb["m"] * tb["s"]

    tb = tb.rename(columns=COLUMNS_RENAME)
    tb["sex"] = tb["sex"].astype(str).replace(SEXES_RENAME).astype("category")
    tb["age_years"] = tb["age_days"] / (12 * DAYS_PER_MONTH)

    # -5 SD is published only for the 5-19 reference, so concatenating appends it out of
    # order; restate the column order explicitly.
    tb = tb[
        ["sex", "age_days", "age_years"]
        + ["lms_l_skewness", "lms_m_median", "lms_s_coefficient_of_variation", "standard_deviation"]
        + [COLUMNS_RENAME[column] for column in PERCENTILES]
        + [COLUMNS_RENAME[column] for column in ZSCORES]
    ]

    tb = tb.sort_values(["sex", "age_days"]).reset_index(drop=True)

    deduplicate_origins(tb)

    sanity_check_outputs(tb)

    tb = tb.format(["sex", "age_days"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)
    ds_garden.save()


def combine_measures(tb_percentiles: Table, tb_zscores: Table, age_column: str) -> Table:
    """Merge the percentile and z-score tables for one age group on (age, sex).

    Both carry L, M and S; they are checked for agreement and kept only once.
    """
    keys = [age_column, "sex"]
    shared = ["l", "m", "s"]

    merged = pr.merge(tb_percentiles, tb_zscores[keys + shared], on=keys, how="outer", suffixes=("", "_z"))
    for column in shared:
        assert np.allclose(merged[column], merged[f"{column}_z"]), (
            f"{column.upper()} disagrees between the percentile and z-score files for {age_column}."
        )
    merged = merged.drop(columns=[f"{column}_z" for column in shared])

    zscore_columns = [column for column in ZSCORES if column in tb_zscores.columns]
    merged = pr.merge(merged, tb_zscores[keys + zscore_columns], on=keys, how="outer")

    # WHO ships StDev only in the 5-19 files; drop it in favor of a uniformly derived column.
    if "stdev" in merged.columns:
        assert np.allclose(merged["stdev"], merged["m"] * merged["s"], atol=TOLERANCE), (
            "Published StDev does not equal M * S."
        )
        merged = merged.drop(columns=["stdev"])

    return merged


def deduplicate_origins(tb: Table) -> None:
    """Collapse the eight per-file origins down to one per WHO product.

    Each WHO product is split across four files (two sexes x percentiles and z-scores),
    which differ only in ``url_download``. Carrying all four through to the indicators
    would list each product four times, so keep one origin per product and clear the
    file-specific download URL -- ``url_main`` points at the page listing every file, and
    the snapshots keep the full per-file record.
    """
    for column in tb.columns:
        origins = []
        for origin in tb[column].metadata.origins:
            if (origin.producer, origin.title, origin.date_published) in [
                (o.producer, o.title, o.date_published) for o in origins
            ]:
                continue
            origin = origin.copy()
            origin.url_download = None
            origins.append(origin)
        # Oldest product first, so citations read chronologically.
        tb[column].metadata.origins = sorted(origins, key=lambda o: o.date_published or "")


def sanity_check_inputs(
    tb_percentiles_under_5: Table,
    tb_zscores_under_5: Table,
    tb_percentiles_5_to_19: Table,
    tb_zscores_5_to_19: Table,
) -> None:
    """Check the four meadow tables against what WHO documents about them."""
    tables = {
        "percentiles_under_5": (tb_percentiles_under_5, "day", 0, 1856),
        "zscores_under_5": (tb_zscores_under_5, "day", 0, 1856),
        "percentiles_5_to_19": (tb_percentiles_5_to_19, "month", 61, 228),
        "zscores_5_to_19": (tb_zscores_5_to_19, "month", 61, 228),
    }

    for name, (tb, age_column, age_min, age_max) in tables.items():
        assert not tb.isna().any().any(), f"Unexpected missing values in {name}."

        # Height-for-age is normally distributed at every age, which is what makes
        # percentiles and z-scores interchangeable. Everything below relies on it.
        assert (tb["l"] == 1).all(), f"L is not 1 everywhere in {name}; the distribution is no longer normal."

        for sex, tb_sex in tb.groupby("sex", observed=True):
            ages = tb_sex[age_column].sort_values().to_numpy()
            assert ages[0] == age_min and ages[-1] == age_max, (
                f"{name} ({sex}) spans {ages[0]}-{ages[-1]} {age_column}s, expected {age_min}-{age_max}."
            )
            assert (np.diff(ages) == 1).all(), f"Gaps in the {age_column} grid of {name} ({sex})."

        # Every published column must be reproducible from L, M and S. This is what pins
        # down the column labels -- in particular that "P01" is the 0.1st percentile and
        # "P999" the 99.9th, rather than the 1st and 99.9th.
        standard_deviation = tb["m"] * tb["s"]
        labels = {**{col: norm.ppf(pct / 100) for col, pct in PERCENTILES.items()}, **ZSCORES}
        for column, z in labels.items():
            if column not in tb.columns:
                continue
            deviation = (tb[column] - (tb["m"] + z * standard_deviation)).abs().max()
            assert deviation < TOLERANCE, (
                f"Column {column!r} of {name} is off by {deviation:.4f} cm from M + {z:.3f} * M * S, "
                f"so it does not hold the value its name implies."
            )


def sanity_check_outputs(tb: Table) -> None:
    """Check the spliced table."""
    percentile_columns = [COLUMNS_RENAME[column] for column in PERCENTILES]

    assert set(tb["sex"]) == {"Boys", "Girls"}, f"Unexpected sexes: {set(tb['sex'])}."
    assert len(tb) == 2 * 2025, f"Expected 4050 rows (2025 ages x 2 sexes), got {len(tb)}."

    # -5 SD is published only for the 5-19 reference, so it is missing below day 1857.
    expected_missing = {"height_sd_minus_5"}
    unexpected = set(tb.columns[tb.isna().any()]) - expected_missing
    assert not unexpected, f"Unexpected missing values in {sorted(unexpected)}."
    assert tb.columns[tb.isna().all()].empty, "A column is entirely missing."

    for sex, tb_sex in tb.groupby("sex", observed=True):
        ages = tb_sex["age_days"].to_numpy()
        assert len(set(ages)) == len(ages), f"Duplicate ages for {sex}."
        assert (np.diff(ages) > 0).all(), f"Ages are not sorted for {sex}."
        assert ages[0] == 0 and ages[-1] == 6940, f"{sex} spans days {ages[0]}-{ages[-1]}, expected 0-6940."

        # The percentile ladder must never cross.
        ladder = tb_sex[percentile_columns].to_numpy()
        assert (np.diff(ladder, axis=1) >= 0).all(), f"Percentile curves cross each other for {sex}."

        # -2 SD is the stunting threshold; it sits at the 2.3rd percentile, below the 3rd.
        assert (tb_sex["height_sd_minus_2"] < tb_sex["height_percentile_3"]).all(), (
            f"The -2 SD line is not below the 3rd percentile for {sex}."
        )

        # The median rises with age everywhere except at the two documented steps. Finding
        # any other backward step means the splice went wrong.
        median = tb_sex["height_percentile_50"].to_numpy()
        backward = np.flatnonzero(np.diff(median) < 0)
        found = {(int(ages[i]), int(ages[i + 1])) for i in backward}
        expected = {(before, after) for before, after, _ in DISCONTINUITIES}
        assert found == expected, (
            f"Median height for {sex} steps down between {sorted(found)}, expected exactly {sorted(expected)}."
        )
        for before, after, description in DISCONTINUITIES:
            step = float(median[ages == after][0] - median[ages == before][0])
            assert -1 < step < 0, f"Step of {step:.3f} cm for {sex} where {description} is out of range."

    # Anchor values, read off WHO's published tables.
    anchors = {
        ("Boys", 0): 49.884,
        ("Boys", 1857): 110.265,
        ("Boys", 6940): 176.543,
        ("Girls", 0): 49.148,
        ("Girls", 1857): 109.602,
        ("Girls", 6940): 163.155,
    }
    for (sex, age_days), expected_height in anchors.items():
        actual = float(tb.loc[(tb["sex"] == sex) & (tb["age_days"] == age_days), "height_percentile_50"].iloc[0])
        assert abs(actual - expected_height) < TOLERANCE, (
            f"Median height for {sex} at day {age_days} is {actual:.3f} cm, expected {expected_height} cm."
        )
