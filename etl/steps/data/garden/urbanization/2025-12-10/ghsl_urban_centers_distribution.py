"""Create city-size distribution table for bespoke Grapher visualization.

Data model
----------
Index : (country, year)
    country  – harmonised country name or region/income-group aggregate
    year     – bin upper-bound integer (the x-axis in Grapher)
               1-2-5 log scale: 100_000, 200_000, 500_000, 1_000_000, …, 100_000_000

Variables (one per 5-year time step)
    pop_share_YYYY_estimates   – % of urban-centre population in this bin (historical)
    pop_share_YYYY_projections – % of urban-centre population in this bin (projected)

Grapher chart type
------------------
Designed to render like "Monthly average surface temperatures by decade":
    x-axis   = bin upper-bound  (the 'year' column in Grapher)
    lines    = each pop_share_YYYY variable (one calendar year per indicator)
    entities = countries / regions (switchable in the chart)

Each x-axis tick is the *upper* population limit of the bin:
    x = 100_000  → cities with 50k ≤ pop < 100k
    x = 200_000  → cities with 100k ≤ pop < 200k
    …
    x = 100_000_000 → cities with pop ≥ 50M  (sentinel value)
"""

import numpy as np
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

# 1-2-3-5 log-spaced bin edges (population lower bounds).
# Adds a "3" step between each decade for finer resolution (~1.5–2× ratios vs 2.5× for 1-2-5).
# Each bin's x-axis value is its upper bound.
# Last bin (50M+) uses 100_000_000 as a sentinel integer x-value.
_EDGES = [
    50_000,
    100_000,
    200_000,
    300_000,
    500_000,
    1_000_000,
    2_000_000,
    3_000_000,
    5_000_000,
    10_000_000,
    20_000_000,
    30_000_000,
    50_000_000,
]
BINS = []
for _i, _lo in enumerate(_EDGES):
    _hi = _EDGES[_i + 1] if _i + 1 < len(_EDGES) else float("inf")
    _x = _EDGES[_i + 1] if _i + 1 < len(_EDGES) else 100_000_000
    BINS.append({"lo": _lo, "hi": _hi, "x": _x})

BIN_X_VALUES = [b["x"] for b in BINS]


def assign_bin_vectorized(urban_pop):
    """Assign each city population to a bin x-value using vectorized operations."""
    result = np.full(len(urban_pop), np.nan)
    for b in BINS:
        mask = (urban_pop >= b["lo"]) & (urban_pop < b["hi"])
        result[mask] = b["x"]
    return result


def run() -> None:
    # ── Load inputs ────────────────────────────────────────────────────────────
    ds_meadow = paths.load_dataset("ghsl_urban_centers")
    ds_regions = paths.load_dataset("regions")
    ds_income_groups = paths.load_dataset("income_groups")

    # Raw city-level table: (country, urban_center_name, year) → urban_pop
    tb_raw = ds_meadow.read("ghsl_urban_centers_raw").reset_index()

    # Grab origins from the main table whose urban_pop has them set by the meadow step.
    tb_main = ds_meadow.read("ghsl_urban_centers")
    _origins = tb_main["urban_pop"].metadata.origins

    # ── Harmonize country names ────────────────────────────────────────────────
    # Reuse the mapping file from the parent step (same source data, same countries).
    countries_file = paths.directory / "ghsl_urban_centers.countries.json"
    excluded_countries_file = paths.directory / "ghsl_urban_centers.excluded_countries.json"
    tb_raw = paths.regions.harmonize_names(
        tb_raw,
        countries_file=countries_file,
        excluded_countries_file=excluded_countries_file,
    )

    # ── Assign bins ────────────────────────────────────────────────────────────
    tb_raw["bin_x"] = assign_bin_vectorized(tb_raw["urban_pop"].to_numpy(dtype=float))
    tb_raw = tb_raw.dropna(subset=["bin_x"])
    tb_raw["bin_x"] = tb_raw["bin_x"].astype(int)

    # ── Aggregate: sum city populations per (country, year, bin_x) ────────────
    tb_agg = tb_raw.groupby(["country", "year", "bin_x"], as_index=False)["urban_pop"].sum()

    # ── Pivot bins into columns so geo.add_regions_to_table can aggregate ─────
    tb_wide = tb_agg.pivot_table(
        index=["country", "year"], columns="bin_x", values="urban_pop", aggfunc="sum"
    ).reset_index()
    tb_wide.columns = ["country", "year"] + [f"bin_{x}" for x in BIN_X_VALUES]
    bin_cols = [f"bin_{x}" for x in BIN_X_VALUES]

    # Fill NaN with 0 (country had no cities in that bin for that year)
    tb_wide[bin_cols] = tb_wide[bin_cols].fillna(0)

    # ── Add regional / income-group aggregates ─────────────────────────────────
    tb_wide = geo.add_regions_to_table(
        tb_wide,
        aggregations={col: "sum" for col in bin_cols},
        regions=REGIONS,
        ds_regions=ds_regions,
        ds_income_groups=ds_income_groups,
        min_num_values_per_year=1,
    )

    # ── Convert to shares; keep raw population too ────────────────────────────
    tb_wide["_total"] = tb_wide[bin_cols].sum(axis=1)
    for col in bin_cols:
        x = int(col.split("_", 1)[1])
        tb_wide[f"share_{x}"] = (tb_wide[col] / tb_wide["_total"]) * 100
        tb_wide[f"pop_{x}"] = tb_wide[col]  # raw population (people)
    tb_wide = tb_wide.drop(columns=bin_cols + ["_total"])
    share_cols = [f"share_{x}" for x in BIN_X_VALUES]
    pop_cols = [f"pop_{x}" for x in BIN_X_VALUES]

    # ── Melt both metrics to long format ──────────────────────────────────────
    tb_long_share = tb_wide.melt(
        id_vars=["country", "year"],
        value_vars=share_cols,
        var_name="bin_col",
        value_name="pop_share",
    )
    tb_long_share["bin_x"] = tb_long_share["bin_col"].str.removeprefix("share_").astype(int)
    tb_long_share = tb_long_share.drop(columns=["bin_col"])

    tb_long_pop = tb_wide.melt(
        id_vars=["country", "year"],
        value_vars=pop_cols,
        var_name="bin_col",
        value_name="pop",
    )
    tb_long_pop["bin_x"] = tb_long_pop["bin_col"].str.removeprefix("pop_").astype(int)
    tb_long_pop = tb_long_pop.drop(columns=["bin_col"])

    tb_long = pr.merge(tb_long_share, tb_long_pop, on=["country", "year", "bin_x"], how="outer")

    # ── Split into estimates / projections ────────────────────────────────────
    past = tb_long[tb_long["year"] < START_OF_PROJECTIONS].copy()
    future = tb_long[tb_long["year"] >= START_OF_PROJECTIONS - 5].copy()

    def pivot_years(df, suffix):
        pt_share = df.pivot_table(index=["country", "bin_x"], columns="year", values="pop_share")
        pt_share.columns = [f"pop_share_{y}_{suffix}" for y in pt_share.columns]
        pt_pop = df.pivot_table(index=["country", "bin_x"], columns="year", values="pop")
        pt_pop.columns = [f"pop_{y}_{suffix}" for y in pt_pop.columns]
        return pr.merge(pt_share.reset_index(), pt_pop.reset_index(), on=["country", "bin_x"], how="outer")

    tb_est = pivot_years(past, "estimates")
    tb_proj = pivot_years(future, "projections")

    tb = pr.merge(tb_est, tb_proj, on=["country", "bin_x"], how="outer")

    # ── Set metadata on all generated indicator columns ──────────────────────
    origins = _origins
    for col in tb.columns:
        if col.startswith("pop_share_"):
            parts = col.split("_")
            year, suffix = parts[2], parts[3]
            tb[col].metadata.origins = origins
            tb[col].metadata.unit = "%"
            tb[col].metadata.short_unit = "%"
            tb[col].metadata.title = f"Share of urban-centre population by city size ({year}, {suffix})"
            tb[col].metadata.description_short = (
                f"Share of urban-centre population living in cities of each size category, {year} ({suffix})."
            )
        elif col.startswith("pop_") and not col.startswith("pop_share"):
            parts = col.split("_")
            year, suffix = parts[1], parts[2]
            tb[col].metadata.origins = origins
            tb[col].metadata.unit = "people"
            tb[col].metadata.short_unit = ""
            tb[col].metadata.title = f"Population living in urban centres by city size ({year}, {suffix})"
            tb[col].metadata.description_short = (
                f"Total population living in urban centres of each size category, {year} ({suffix})."
            )

    # ── Format: bin_x becomes 'year' (the Grapher x-axis) ────────────────────
    tb = tb.rename(columns={"bin_x": "year"})
    tb = tb.format(["country", "year"], short_name="ghsl_urban_centers_distribution")

    # ── Save ──────────────────────────────────────────────────────────────────
    ds_garden = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata)
    ds_garden.save()
