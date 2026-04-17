"""City primacy ratio: dominance of a country's largest city over its next-largest cities.

Data model
----------
Index : (country, year)  — standard time series, year = calendar year
Vars  : primacy_ratio_2city_estimates / projections   (rank1 / rank2)
        primacy_ratio_4city_estimates / projections   (rank1 / (rank2+rank3+rank4))

Definitions
-----------
  2-city primacy ratio = population of largest city / population of 2nd largest city
  4-city primacy ratio = population of largest city / combined population of 2nd–4th largest cities

A ratio > 1 means the largest city is bigger than the comparison group.
Higher values indicate greater urban primacy (dominance of one primate city).
"""

import owid.catalog.processing as pr
import pandas as pd

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
    ds_regions = paths.load_dataset("regions")
    ds_income_groups = paths.load_dataset("income_groups")

    tb_raw = ds_meadow.read("ghsl_urban_centers_raw").reset_index()
    _origins = ds_meadow.read("ghsl_urban_centers")["urban_pop"].metadata.origins

    # ── Harmonize country names ────────────────────────────────────────────────
    countries_file = paths.directory / "ghsl_urban_centers.countries.json"
    excluded_countries_file = paths.directory / "ghsl_urban_centers.excluded_countries.json"
    tb_raw = paths.regions.harmonize_names(
        tb_raw,
        countries_file=countries_file,
        excluded_countries_file=excluded_countries_file,
    )

    # ── Expand cities to regions ───────────────────────────────────────────────
    tb_cities_exp = _expand_to_regions(
        tb_raw[["country", "year", "urban_pop"]].copy(),
        ds_regions,
        ds_income_groups,
    )

    # ── Rank cities within each (country, year) ────────────────────────────────
    tb_cities_exp["rank"] = (
        tb_cities_exp.groupby(["country", "year"])["urban_pop"].rank(method="first", ascending=False).astype(int)
    )

    # Keep only top-4 cities (all we need for the 4-city ratio)
    tb_top4 = tb_cities_exp[tb_cities_exp["rank"] <= 4].copy()

    # ── Pivot so each rank is a column ────────────────────────────────────────
    tb_pivot = tb_top4.pivot_table(index=["country", "year"], columns="rank", values="urban_pop").reset_index()
    tb_pivot.columns.name = None
    tb_pivot.columns = ["country", "year"] + [f"rank{i}" for i in range(1, 5)]

    # Drop rows where rank1 is missing (country has no cities)
    tb_pivot = tb_pivot.dropna(subset=["rank1"])

    # ── Compute primacy ratios ─────────────────────────────────────────────────
    # 2-city: rank1 / rank2
    tb_pivot["primacy_ratio_2city"] = tb_pivot["rank1"] / tb_pivot["rank2"]

    # 4-city: rank1 / (rank2 + rank3 + rank4)
    denominator_4city = tb_pivot[["rank2", "rank3", "rank4"]].sum(axis=1, min_count=1)
    tb_pivot["primacy_ratio_4city"] = tb_pivot["rank1"] / denominator_4city

    tb_wide = tb_pivot[["country", "year", "primacy_ratio_2city", "primacy_ratio_4city"]].copy()

    # ── Split estimates / projections ─────────────────────────────────────────
    past = tb_wide[tb_wide["year"] < START_OF_PROJECTIONS].copy()
    future = tb_wide[tb_wide["year"] >= START_OF_PROJECTIONS - 5].copy()

    def add_suffix(df, suffix):
        return df.rename(columns={c: f"{c}_{suffix}" for c in df.columns if c not in ("country", "year")})

    tb = pr.merge(
        add_suffix(past, "estimates"),
        add_suffix(future, "projections"),
        on=["country", "year"],
        how="outer",
    )

    # ── Metadata ──────────────────────────────────────────────────────────────
    RATIO_LABELS = {
        "primacy_ratio_2city": (
            "2-city primacy ratio",
            "the population of the largest city divided by the population of the 2nd largest city",
        ),
        "primacy_ratio_4city": (
            "4-city primacy ratio",
            "the population of the largest city divided by the combined population of the 2nd, 3rd, and 4th largest cities",
        ),
    }
    for col_base, (short_label, long_desc) in RATIO_LABELS.items():
        for sfx in ("estimates", "projections"):
            col = f"{col_base}_{sfx}"
            if col in tb.columns:
                tb[col].metadata.origins = _origins
                tb[col].metadata.unit = ""
                tb[col].metadata.short_unit = ""
                tb[col].metadata.title = f"{short_label.capitalize()} ({sfx})"
                tb[col].metadata.description_short = (
                    f"The primacy ratio measures how dominant a country's largest city is relative "
                    f"to its other cities. This is {long_desc} ({sfx})."
                )

    tb = tb.format(["country", "year"], short_name="ghsl_primacy_ratio")

    ds_garden = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata)
    ds_garden.save()
