"""Create a garden dataset of population by city size bin for grapher testing.

Produces wide-format population counts for many different binning configurations
so they can be compared directly in grapher. Both country and regional aggregates.

Bins included (all for cities ≥ 50k, i.e. urban centres only):
  Granular  : 300k–500k, 500k–1m, 1m–3m, 3m–5m, 5m–10m, >10m
  Combined  : 50k–500k, 50k–1m, 1m–5m, 3m–10m
"""

import owid.catalog.processing as pr

from etl.data_helpers import geo
from etl.helpers import PathFinder

paths = PathFinder(__file__)

START_OF_PROJECTIONS = 2025

REGIONS = [
    "North America",
    "South America",
    "Europe",
    "Africa",
    "Asia",
    "Oceania",
    "Low-income countries",
    "Upper-middle-income countries",
    "Lower-middle-income countries",
    "High-income countries",
    "World",
]

# All bin definitions: name -> (list of meadow columns to sum)
# Meadow columns: pop_below_300k, pop_300k_500k, pop_500k_1m, pop_1m_3m,
#                 pop_3m_5m, pop_5m_10m, pop_above_10m
BINS = {
    # Granular bins (300k+ only — 50k–300k not shown standalone)
    "pop_300k_500k": ["pop_300k_500k"],
    "pop_500k_1m": ["pop_500k_1m"],
    "pop_1m_3m": ["pop_1m_3m"],
    "pop_3m_5m": ["pop_3m_5m"],
    "pop_5m_10m": ["pop_5m_10m"],
    "pop_above_10m": ["pop_above_10m"],
    # Combined bins
    "pop_50k_500k": ["pop_below_300k", "pop_300k_500k"],
    "pop_50k_1m": ["pop_below_300k", "pop_300k_500k", "pop_500k_1m"],
    "pop_1m_5m": ["pop_1m_3m", "pop_3m_5m"],
    "pop_3m_10m": ["pop_3m_5m", "pop_5m_10m"],
}


def run() -> None:
    ds_meadow = paths.load_dataset("ghsl_urban_centers")
    tb = ds_meadow.read("ghsl_urban_centers")
    tb = tb.reset_index()

    ds_regions = paths.load_dataset("regions")
    ds_income_groups = paths.load_dataset("income_groups")

    # Harmonise country names.
    tb = paths.regions.harmonize_names(tb)

    # Keep country-level rows only (top-100 rows have names like "Paris (France)").
    tb = tb[~tb["country"].str.contains(r"\(", na=False)].copy()

    # Keep only the city-size aggregate columns plus country/year.
    meadow_bin_cols = [
        "pop_below_300k",
        "pop_300k_500k",
        "pop_500k_1m",
        "pop_1m_3m",
        "pop_3m_5m",
        "pop_5m_10m",
        "pop_above_10m",
    ]
    tb = tb[["country", "year"] + [c for c in meadow_bin_cols if c in tb.columns]].copy()

    # Drop rows where all bin cols are NaN (capital/largest-city rows with no city-size data).
    tb = tb.dropna(subset=meadow_bin_cols, how="all")

    # Rename meadow source columns with _src suffix to avoid name collisions with output bins.
    src_rename = {col: f"{col}_src" for col in meadow_bin_cols}
    tb = tb.rename(columns=src_rename)
    src_cols = list(src_rename.values())

    # Fill remaining NaN source values with 0 (country has no cities in that size class).
    for col in src_cols:
        tb[col] = tb[col].fillna(0)

    # Remap BINS source names to the renamed _src versions.
    bins_src = {bin_name: [f"{c}_src" for c in source_cols] for bin_name, source_cols in BINS.items()}
    origins = tb["pop_below_300k_src"].metadata.origins

    # ── Compute output bins ──────────────────────────────────────────────────
    for bin_name, source_cols in bins_src.items():
        valid = [c for c in source_cols if c in tb.columns]
        tb[bin_name] = tb[valid].sum(axis=1)
        tb[bin_name].metadata.origins = origins

    # Drop the raw meadow source columns.
    tb = tb.drop(columns=src_cols)

    # ── Add regional aggregates ──────────────────────────────────────────────
    bin_cols = list(BINS.keys())
    tb = geo.add_regions_to_table(
        tb,
        aggregations={col: "sum" for col in bin_cols},
        regions=REGIONS,
        ds_regions=ds_regions,
        ds_income_groups=ds_income_groups,
        min_num_values_per_year=1,
    )

    # ── Split estimates / projections ────────────────────────────────────────
    past = tb[tb["year"] < START_OF_PROJECTIONS].copy()
    future = tb[tb["year"] >= START_OF_PROJECTIONS - 5].copy()  # 5-yr overlap

    for col in bin_cols:
        past[f"{col}_estimates"] = tb.loc[tb["year"] < START_OF_PROJECTIONS, col]
        future[f"{col}_projections"] = tb.loc[tb["year"] >= START_OF_PROJECTIONS - 5, col]
        past = past.drop(columns=[col], errors="ignore")
        future = future.drop(columns=[col], errors="ignore")

    tb = pr.merge(past, future, on=["country", "year"], how="outer")

    # ── Format and save ──────────────────────────────────────────────────────
    tb = tb.format(["country", "year"], short_name="ghsl_city_size_bins")

    ds_garden = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata)
    ds_garden.save()
