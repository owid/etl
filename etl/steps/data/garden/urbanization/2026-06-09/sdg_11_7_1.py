"""Garden step for SDG 11.7.1 - Open Space in Cities.

Creates a table with country-level data: population-weighted country averages
across all cities, using city population data from the GHSL Urban Centers database.
"""

import unicodedata

import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Table

from etl.helpers import PathFinder

paths = PathFinder(__file__)

# Manual name overrides: (country, parsed_sdg_city_name) -> ghsl_city_name
# Add entries here when automatic matching fails for a city.
CITY_NAME_MAPPING: dict[tuple[str, str], str] = {
    ("Algeria", "El Djazair"): "Algiers",
    ("Iran", "Tehran"): "Tehrān",
    ("Kuwait", "Al Kuwayt"): "Kuwait City",
    ("Laos", "Viengchan"): "Vientiane",
    ("Morocco", "Dar El Beida"): "Casablanca",
    ("Morocco", "Fes"): "Fez",
    ("Morocco", "Tanger"): "Tangier",
    ("Russia", "Perm"): "Perm'",
    ("Saudi Arabia", "Ar Riyad"): "Ar-Riyāḑ (Riyadh)",
    ("Saudi Arabia", "Jiddah"): "Jeddah",
    ("Saudi Arabia", "Makkah"): "Mecca",
    ("Somalia", "Hargeysa"): "Hargeisa",
    ("Syria", "Hamah"): "Hama",
    ("Vietnam", "Ha Noi"): "Hanoi",
    ("Yemen", "Sana A"): "Sanaa",
    ("Yemen", "Adan"): "Aden",
}


def normalize_city_name(name: str) -> str:
    """Normalize city names: strip diacritics, lowercase, handle parenthetical alternatives."""
    if pd.isna(name):
        return ""
    name = "".join(c for c in unicodedata.normalize("NFD", str(name)) if unicodedata.category(c) != "Mn")
    name = name.lower()
    name = " ".join(name.split())
    if "(" in name and ")" in name:
        main_name = name.split("(")[0].strip()
        alt_name = name.split("(")[1].split(")")[0].strip()
        return f"{main_name}|{alt_name}"
    return name


def snap_to_ghsl_year(year: int, ghsl_years: list[int]) -> int:
    """Return the nearest GHSL 5-year data point for a given SDG data year."""
    return min(ghsl_years, key=lambda g: abs(g - year))


def match_cities_with_ghsl(tb_sdg: Table, tb_ghsl: pd.DataFrame, ghsl_years: list[int]) -> Table:
    """Match SDG cities with GHSL population data using multiple strategies.

    Strategies (in order):
    1. Exact match on normalized names
    2. Alternative name matching (from parentheses)
    3. Substring matching (SDG name in GHSL name, or vice versa)
    4. Compound name matching (first part before comma/hyphen/slash)
    5. Prefix matching (first 5 characters)
    """
    tb_sdg = tb_sdg.copy()
    tb_ghsl = tb_ghsl.copy()

    # Snap each SDG year to the nearest GHSL year for population lookup
    tb_sdg["ghsl_year"] = tb_sdg["year"].apply(lambda y: snap_to_ghsl_year(int(y), ghsl_years))
    tb_sdg["city_normalized"] = tb_sdg["city"].apply(normalize_city_name)
    tb_ghsl["city_normalized"] = tb_ghsl["ucname"].apply(normalize_city_name)

    tb_ghsl_unique = tb_ghsl.drop_duplicates(subset=["city_normalized", "year"], keep="first")
    tb_ghsl_for_merge = tb_ghsl_unique[["city_normalized", "year", "pop"]].rename(columns={"year": "ghsl_year"})

    # Strategy 1: Exact match
    merged = pr.merge(tb_sdg, tb_ghsl_for_merge, on=["city_normalized", "ghsl_year"], how="left")

    # Strategy 2: Alternative names from parentheses
    unmatched_mask = merged["pop"].isna()
    for idx, row in merged[unmatched_mask & merged["city_normalized"].str.contains("|", regex=False)].iterrows():
        for part in row["city_normalized"].split("|"):
            ghsl_match = tb_ghsl[(tb_ghsl["city_normalized"] == part.strip()) & (tb_ghsl["year"] == row["ghsl_year"])]
            if len(ghsl_match) > 0:
                merged.loc[idx, "pop"] = ghsl_match.iloc[0]["pop"]
                break

    # Strategies 3–5: Advanced matching for remaining unmatched cities
    for idx, row in merged[merged["pop"].isna()].iterrows():
        sdg_norm = row["city_normalized"]
        country = row["country"]
        ghsl_year = row["ghsl_year"]

        ghsl_country = tb_ghsl[(tb_ghsl["country"] == country) & (tb_ghsl["year"] == ghsl_year)]
        if len(ghsl_country) == 0:
            continue

        matched = False
        for _, ghsl_row in ghsl_country.iterrows():
            ghsl_norm = ghsl_row["city_normalized"]
            if pd.isna(ghsl_norm) or ghsl_norm == "":
                continue

            # Strategy 3a: SDG name substring of GHSL name
            if sdg_norm in ghsl_norm and len(sdg_norm) >= 4:
                merged.loc[idx, "pop"] = ghsl_row["pop"]
                matched = True
                break
            # Strategy 3b: GHSL name substring of SDG name
            if ghsl_norm in sdg_norm and len(ghsl_norm) >= 4:
                merged.loc[idx, "pop"] = ghsl_row["pop"]
                matched = True
                break
            # Strategy 4: First component before delimiter
            for delimiter in [",", "-", "/", "_"]:
                if delimiter in sdg_norm:
                    first_part = sdg_norm.split(delimiter)[0].strip()
                    if first_part == ghsl_norm and len(first_part) >= 4:
                        merged.loc[idx, "pop"] = ghsl_row["pop"]
                        matched = True
                        break
            if matched:
                break
            # Strategy 5: Prefix matching (first 5 characters)
            if len(sdg_norm) >= 5 and len(ghsl_norm) >= 5 and sdg_norm[:5] == ghsl_norm[:5]:
                merged.loc[idx, "pop"] = ghsl_row["pop"]
                matched = True
                break

    merged = merged.drop(columns=["city_normalized", "ghsl_year"])
    return merged


def run() -> None:
    #
    # Load inputs.
    #
    ds_meadow = paths.load_dataset("sdg_11_7_1")
    tb = ds_meadow.read("sdg_11_7_1", reset_index=True)

    ds_ghsl = paths.load_dataset("ghsl_urban_centers", namespace="urbanization", version="2025-12-10")
    tb_ghsl = ds_ghsl.read("ghsl_urban_centers_raw", reset_index=True)
    tb_ghsl = tb_ghsl.rename(columns={"urban_center_name": "ucname", "urban_pop": "pop"})

    #
    # Process data.
    #

    # Parse city names from SDG city codes (e.g. "BG_SOFIJA" -> "Sofija")
    tb["city"] = tb["cities"].apply(
        lambda c: str(c)[3:].replace("_", " ").title() if pd.notna(c) and "_" in str(c) else str(c)
    )
    tb = tb.drop(columns=["cities"])

    # Harmonize country names
    tb = paths.regions.harmonize_names(tb=tb)

    # Apply manual city name overrides
    for (country, sdg_name), ghsl_name in CITY_NAME_MAPPING.items():
        ghsl_match = tb_ghsl[(tb_ghsl["country"] == country) & (tb_ghsl["ucname"] == ghsl_name)]
        if len(ghsl_match) > 0:
            for _, ghsl_row in ghsl_match.iterrows():
                new_row = ghsl_row.copy()
                new_row["ucname"] = sdg_name
                tb_ghsl = pd.concat([tb_ghsl, pd.DataFrame([new_row])], ignore_index=True)

    # Match cities with GHSL population data
    ghsl_years = sorted([int(y) for y in tb_ghsl["year"].unique()])
    tb = match_cities_with_ghsl(tb, tb_ghsl, ghsl_years)

    #
    # Create country-level population-weighted averages.
    #
    tb_with_pop = tb[tb["pop"].notna()].copy()

    if len(tb_with_pop) > 0:
        tb_weighted = (
            tb_with_pop.groupby(["country", "year"], observed=True)
            .apply(
                lambda x: pd.Series(
                    {
                        "open_space_share": (x["value"] * x["pop"]).sum() / x["pop"].sum()
                        if x["pop"].sum() > 0
                        else None,
                    }
                ),
                include_groups=False,
            )
            .reset_index()
        )
    else:
        tb_weighted = (
            tb.groupby(["country", "year"], observed=True)
            .agg({"value": "mean"})
            .reset_index()
            .rename(columns={"value": "open_space_share"})
        )

    # Restore origins from the source column (lost during groupby/apply).
    tb_weighted["open_space_share"] = tb_weighted["open_space_share"].copy_metadata(tb["value"])

    tb_countries = tb_weighted.format(["country", "year"], short_name="sdg_11_7_1_country")

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb_countries])
    ds_garden.save()
