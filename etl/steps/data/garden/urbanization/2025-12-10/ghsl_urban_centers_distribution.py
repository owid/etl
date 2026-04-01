"""Rank-size (Zipf) chart: city share of total population by rank, years as lines.

Data model
----------
Index : (country, year) where year = city rank (1 = most populous)
Vars  : city_pop_share_YYYY_estimates / city_pop_share_YYYY_projections

Interpretation
--------------
"In 2020, Lima (rank 1 in Peru) was home to 30% of Peru's total population."
"In 2020, Paris (rank 1 in France) was home to 14% of France's total population."

Lines shifting upward over time show cities growing relative to the total population.
Countries with a steep drop from rank 1 to rank 2 have a dominant primate city.

Configure in Grapher admin
--------------------------
- Set y-axis to log scale
- Add horizontal reference lines at 0.75%, 4.5%, 15% (approx. 500k/3M/10M for a
  country of 67M like France) — or describe class boundaries in subtitle

Grapher chart type
------------------
x-axis   = city rank (the 'year' column, 1–50)
lines    = each calendar year (one indicator per year)
entities = countries / regions
"""

import numpy as np
import pandas as pd
import owid.catalog.processing as pr

from etl.data_helpers import geo
from etl.helpers import PathFinder

paths = PathFinder(__file__)

START_OF_PROJECTIONS = 2025
MAX_RANK = 50

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


def _expand_to_regions(tb, ds_regions, ds_income_groups):
    """Duplicate city rows so each region entity gets all cities from its member countries."""
    rows = [tb]
    # World = all cities
    world_tb = tb.copy()
    world_tb["country"] = "World"
    rows.append(world_tb)
    # Other regions
    for region in REGIONS:
        if region == "World":
            continue
        try:
            members = geo.list_members_of_region(region, ds_regions=ds_regions, ds_income_groups=ds_income_groups)
            region_tb = tb[tb["country"].isin(members)].copy()
            if len(region_tb):
                region_tb["country"] = region
                rows.append(region_tb)
        except Exception:
            pass
    return pd.concat(rows, ignore_index=True)


def run() -> None:
    ds_meadow = paths.load_dataset("ghsl_urban_centers")
    ds_meadow_ctry = paths.load_dataset("ghsl_countries")
    ds_regions = paths.load_dataset("regions")
    ds_income_groups = paths.load_dataset("income_groups")

    tb_raw = ds_meadow.read("ghsl_urban_centers_raw").reset_index()
    _origins = ds_meadow.read("ghsl_urban_centers")["urban_pop"].metadata.origins

    # ── Total population by (country, year) ───────────────────────────────────
    tb_ctry = ds_meadow_ctry.read("ghsl_countries", safe_types=False).reset_index()
    tb_total_pop = (
        tb_ctry.groupby(["country", "year"], observed=True)["population"]
        .sum()
        .reset_index()
        .rename(columns={"population": "total_pop"})
    )
    tb_total_pop["total_pop"] = pd.to_numeric(tb_total_pop["total_pop"], errors="coerce")

    # ── Harmonize country names ────────────────────────────────────────────────
    countries_file = paths.directory / "ghsl_urban_centers.countries.json"
    excluded_countries_file = paths.directory / "ghsl_urban_centers.excluded_countries.json"
    tb_raw = paths.regions.harmonize_names(
        tb_raw,
        countries_file=countries_file,
        excluded_countries_file=excluded_countries_file,
    )
    tb_total_pop = paths.regions.harmonize_names(
        tb_total_pop,
        countries_file=countries_file,
        excluded_countries_file=excluded_countries_file,
    )

    # ── Expand cities and total pop to regions ────────────────────────────────
    tb_cities_exp = _expand_to_regions(
        tb_raw[["country", "year", "urban_pop"]].copy(),
        ds_regions,
        ds_income_groups,
    )

    # Regional total pop = sum across member countries
    tb_total_pop_wide = tb_total_pop.rename(columns={"total_pop": "_total_pop"})
    tb_total_pop_wide = geo.add_regions_to_table(
        tb_total_pop_wide,
        aggregations={"_total_pop": "sum"},
        regions=REGIONS,
        ds_regions=ds_regions,
        ds_income_groups=ds_income_groups,
        min_num_values_per_year=1,
    )

    # ── Rank cities within each (country, year) ────────────────────────────────
    tb_cities_exp["rank"] = (
        tb_cities_exp.groupby(["country", "year"])["urban_pop"].rank(method="first", ascending=False).astype(int)
    )
    tb_long = (
        tb_cities_exp[tb_cities_exp["rank"] <= MAX_RANK]
        .rename(columns={"year": "cal_year", "rank": "year"})[["country", "cal_year", "year", "urban_pop"]]
        .copy()
    )

    # ── Merge total pop and compute share of total population ─────────────────
    tb_long = tb_long.merge(
        tb_total_pop_wide.rename(columns={"year": "cal_year"}),
        on=["country", "cal_year"],
        how="left",
    )
    tb_long["city_pop_share"] = (tb_long["urban_pop"] / tb_long["_total_pop"]) * 100

    # Cumulative share: top-N cities combined as % of total population
    tb_long = tb_long.sort_values(["country", "cal_year", "year"])
    tb_long["city_pop_share"] = tb_long.groupby(["country", "cal_year"])["city_pop_share"].cumsum()

    # ── Split estimates / projections, pivot calendar years into columns ───────
    past = tb_long[tb_long["cal_year"] < START_OF_PROJECTIONS]
    future = tb_long[tb_long["cal_year"] >= START_OF_PROJECTIONS - 5]

    def pivot(df, suffix):
        pt = df.pivot_table(index=["country", "year"], columns="cal_year", values="city_pop_share")
        pt.columns = [f"city_pop_share_{y}_{suffix}" for y in pt.columns]
        return pt.reset_index()

    tb = pr.merge(pivot(past, "estimates"), pivot(future, "projections"), on=["country", "year"], how="outer")

    # ── Metadata ──────────────────────────────────────────────────────────────
    for col in tb.columns:
        if col.startswith("city_pop_share_"):
            parts = col.split("_")
            yr, sfx = parts[3], parts[4]
            tb[col].metadata.origins = _origins
            tb[col].metadata.unit = "%"
            tb[col].metadata.short_unit = "%"
            tb[col].metadata.title = f"Cumulative share of total population in top-N cities ({yr}, {sfx})"
            tb[col].metadata.description_short = (
                f"Share of the total population living in the top N largest cities combined, "
                f"{yr} ({sfx}). At rank 5, this is the share of the total population living in the 5 largest cities."
            )

    tb = tb.format(["country", "year"], short_name="ghsl_urban_centers_distribution")

    ds_garden = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata)
    ds_garden.save()
