"""City-tier population: Top-1 / Top-2–5 / Top-6–50 cities — raw population and share of total.

Data model
----------
Index : (country, year)  — standard time series, year = calendar year
Vars  : tier1_pop_estimates / projections          (people)
        tier1_pop_share_estimates / projections    (% of total population)
        tier2_pop_*  /  tier3_pop_*  (same)

Tiers
-----
  tier1 = rank-1 city (largest)
  tier2 = rank 2–5 cities combined
  tier3 = rank 6–50 cities combined
"""

import pandas as pd
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


def _expand_to_regions(tb, ds_regions, ds_income_groups):
    """Duplicate city rows so each region entity gets all cities from its member countries."""
    rows = [tb]
    world_tb = tb.copy()
    world_tb["country"] = "World"
    rows.append(world_tb)
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

    tb_total_pop_wide = tb_total_pop.rename(columns={"total_pop": "_total_pop"})
    tb_total_pop_wide = geo.add_regions_to_table(
        tb_total_pop_wide,
        aggregations={"_total_pop": "sum"},
        regions=REGIONS,
        ds_regions=ds_regions,
        ds_income_groups=ds_income_groups,
        min_num_values_per_year=1,
    )

    # ── Rank cities and assign tiers ──────────────────────────────────────────
    tb_cities_exp["rank"] = (
        tb_cities_exp.groupby(["country", "year"])["urban_pop"].rank(method="first", ascending=False).astype(int)
    )

    def _tier(r):
        if r == 1:
            return "tier1"
        elif r <= 5:
            return "tier2"
        elif r <= 50:
            return "tier3"
        return None

    tb_cities_exp["tier"] = tb_cities_exp["rank"].map(_tier)
    tb_tier = tb_cities_exp.dropna(subset=["tier"])

    # ── Sum population per (country, year, tier) ──────────────────────────────
    tb_agg = tb_tier.groupby(["country", "year", "tier"])["urban_pop"].sum().reset_index()

    # ── Urban total (all top-50 cities) per (country, year) ───────────────────
    tb_urban_total = (
        tb_tier.groupby(["country", "year"])["urban_pop"]
        .sum()
        .reset_index()
        .rename(columns={"urban_pop": "_urban_total"})
    )

    # ── Merge total pop + urban total and compute shares ──────────────────────
    tb_agg = tb_agg.merge(
        tb_total_pop_wide[["country", "year", "_total_pop"]],
        on=["country", "year"],
        how="left",
    )
    tb_agg = tb_agg.merge(tb_urban_total, on=["country", "year"], how="left")
    tb_agg["pop_share"] = tb_agg["urban_pop"] / tb_agg["_total_pop"] * 100
    tb_agg["urban_share"] = tb_agg["urban_pop"] / tb_agg["_urban_total"] * 100

    # ── Pivot raw pop, total-pop share, and urban share into columns ──────────
    tb_pop = tb_agg.pivot_table(index=["country", "year"], columns="tier", values="urban_pop").reset_index()
    tb_pop.columns.name = None
    tb_pop = tb_pop.rename(columns={t: f"{t}_pop" for t in ("tier1", "tier2", "tier3")})

    tb_share = tb_agg.pivot_table(index=["country", "year"], columns="tier", values="pop_share").reset_index()
    tb_share.columns.name = None
    tb_share = tb_share.rename(columns={t: f"{t}_pop_share" for t in ("tier1", "tier2", "tier3")})

    tb_urban_share = tb_agg.pivot_table(index=["country", "year"], columns="tier", values="urban_share").reset_index()
    tb_urban_share.columns.name = None
    tb_urban_share = tb_urban_share.rename(columns={t: f"{t}_urban_share" for t in ("tier1", "tier2", "tier3")})

    tb_wide = tb_pop.merge(tb_share, on=["country", "year"], how="outer")
    tb_wide = tb_wide.merge(tb_urban_share, on=["country", "year"], how="outer")

    # ── Split estimates / projections ─────────────────────────────────────────
    past = tb_wide[tb_wide["year"] < START_OF_PROJECTIONS].copy()
    future = tb_wide[tb_wide["year"] >= START_OF_PROJECTIONS - 5].copy()

    def add_suffix(df, suffix):
        return df.rename(columns={c: f"{c}_{suffix}" for c in df.columns if c not in ("country", "year")})

    tb = pr.merge(add_suffix(past, "estimates"), add_suffix(future, "projections"), on=["country", "year"], how="outer")

    # ── Metadata ──────────────────────────────────────────────────────────────
    TIER_LABELS = {
        "tier1": ("largest city", "the single largest city"),
        "tier2": ("2nd–5th largest cities", "the 2nd to 5th largest cities combined"),
        "tier3": ("6th largest and above", "the 6th largest city and above"),
    }
    for tier, (short_label, long_label) in TIER_LABELS.items():
        for sfx in ("estimates", "projections"):
            # raw population
            col_pop = f"{tier}_pop_{sfx}"
            if col_pop in tb.columns:
                tb[col_pop].metadata.origins = _origins
                tb[col_pop].metadata.unit = "people"
                tb[col_pop].metadata.short_unit = ""
                tb[col_pop].metadata.title = f"Population in {short_label} ({sfx})"
                tb[col_pop].metadata.description_short = f"Total population living in {long_label} ({sfx})."
            # share of total population
            col_share = f"{tier}_pop_share_{sfx}"
            if col_share in tb.columns:
                tb[col_share].metadata.origins = _origins
                tb[col_share].metadata.unit = "%"
                tb[col_share].metadata.short_unit = "%"
                tb[col_share].metadata.title = f"Share of total population in {short_label} ({sfx})"
                tb[
                    col_share
                ].metadata.description_short = f"Share of the total population living in {long_label} ({sfx})."
            # share of urban (city) population
            col_urban = f"{tier}_urban_share_{sfx}"
            if col_urban in tb.columns:
                tb[col_urban].metadata.origins = _origins
                tb[col_urban].metadata.unit = "%"
                tb[col_urban].metadata.short_unit = "%"
                tb[col_urban].metadata.title = f"Share of city population in {short_label} ({sfx})"
                tb[
                    col_urban
                ].metadata.description_short = (
                    f"Share of the total tracked city population living in {long_label} ({sfx})."
                )

    tb = tb.format(["country", "year"], short_name="ghsl_city_tiers")

    ds_garden = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata)
    ds_garden.save()
