"""Preprocess AQLI PM2.5 data (national + GADM1/GADM2) into small JSON files for an interactive map.

Not wired into the DAG -- see national_subnational_map.py for context on why this reads straight
from the raw snapshot ZIPs (national: aqli/2026-03-30, subnational + shapes: aqli/2026-09-07)
instead of a garden dataset.

Writes:
  ai/aqli_pm_map/world.json            -- national PM2.5 choropleth (latest year)
  ai/aqli_pm_map/countries/<iso3>.json -- that country's GADM1 + GADM2 PM2.5 choropleth

A naive export of the full-resolution shapefiles as GeoJSON is far too heavy for a web page (the
full GADM2 layer alone is ~100 MB even simplified). Two things keep file sizes small instead:
  - Geometries are simplified and snapped to a coordinate grid (shapely.set_precision) -- coarser
    for the world overview (seen at a glance, zoomed out), finer for the per-country drill-down.
  - GADM1/GADM2 detail is split into one small file per country, fetched on demand, rather than
    one file holding every country's subnational geometry.

Run with:
    .venv/bin/python etl/steps/viz/bespoke/aqli/latest/prepare_pm_map_data.py
"""

import json
import re
import time
import unicodedata

import geopandas as gpd
import pandas as pd
import shapely

from etl.paths import BASE_DIR
from etl.snapshot import Snapshot

YEAR = 2024
NATIONAL_SNAPSHOT_URI = "aqli/2026-03-30/air_quality_life_index.zip"
SUBNATIONAL_SNAPSHOT_VERSION = "2026-09-07"

OUT_DIR = BASE_DIR / "ai" / "aqli_pm_map"
COUNTRIES_DIR = OUT_DIR / "countries"

# Degrees. ~1 degree of latitude is ~111km, so a 0.01 grid snaps to ~1km (fine for a whole-globe
# overview) and a 0.001 grid snaps to ~100m (fine for a single country's states/districts).
WORLD_SIMPLIFY_TOLERANCE = 0.05
WORLD_PRECISION_GRID = 0.01
COUNTRY_SIMPLIFY_TOLERANCE = 0.005
COUNTRY_PRECISION_GRID = 0.001


# Letters that don't decompose into base+combining-mark under NFKD, so plain accent-stripping
# leaves them untouched (or -- worse -- the punctuation-collapse step below deletes them outright
# instead of transliterating). Found by diffing shapefile vs CSV region names country by country:
# Vietnamese d-with-stroke, Icelandic/Faroese eth/thorn, Nordic ae/o-with-stroke, Polish l-with-
# stroke, Maltese h-with-stroke, Turkish g-with-breve/dotless i, Khmer transliteration's oe-ligature.
_TRANSLITERATIONS = {
    "đ": "d",
    "Đ": "D",
    "ð": "d",
    "Ð": "D",
    "þ": "th",
    "Þ": "Th",
    "æ": "ae",
    "Æ": "AE",
    "ø": "o",
    "Ø": "O",
    "ł": "l",
    "Ł": "L",
    "ħ": "h",
    "Ħ": "H",
    "ğ": "g",
    "Ğ": "G",
    "ı": "i",
    "İ": "I",
    "œ": "oe",
    "Œ": "OE",
    "ß": "ss",
}


def _strip_accents(name: str) -> str:
    # The GADM1/GADM2 shapefiles and the AQLI CSVs spell some region names with different
    # diacritics (e.g. "México" vs "Mexico") -- normalize before joining/matching on name.
    name = "".join(_TRANSLITERATIONS.get(c, c) for c in name)
    return "".join(c for c in unicodedata.normalize("NFKD", name) if not unicodedata.combining(c))


def _normalize_name(name) -> str:
    # Join key for matching region names across the shapefiles and the AQLI CSVs: strip accents,
    # then fold case and punctuation/whitespace so e.g. "Nord-Trøndelag" and "Nord Trondelag"
    # compare equal.
    # pd.isna(), not `isinstance(name, float)` -- the CSV's missing name_2 (small single-tier
    # countries like Andorra, where GADM2 doesn't subdivide further than GADM1) comes through as
    # pandas' pd.NA, a distinct type that isinstance(..., float) doesn't catch. That let str(pd.NA)
    # ("<NA>") through to become the literal key "na", while the shapefile's matching None name2
    # correctly normalized to "" -- silently breaking every single-tier country's GADM2 match.
    if name is None or pd.isna(name):
        return ""
    s = _strip_accents(str(name)).lower().strip()
    s = re.sub(r"[^a-z0-9]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    if s == "na":
        # The shapefile sometimes spells "no subdivision here" as the literal text "NA" rather
        # than a real null (e.g. Uruguay's name2, Marshall Islands' name1) -- fold it into the same
        # empty key as a true null, so it matches the CSV's genuinely-missing value instead of
        # failing to match a literal "na" string that isn't a real place name.
        return ""
    return s


# Hand-curated overrides for region renames/splits/merges that no amount of spelling
# normalization can resolve -- confirmed by inspecting each remaining mismatch individually.
# Keyed by (country, gadm level) -> {shapefile name (as it appears in the shapefile): CSV name to
# match it against}. Only add entries here for a genuine identity match; a shapefile region with no
# corresponding CSV data at all (e.g. a few small Pacific-island districts) is left unmatched on
# purpose -- that's a real data gap, not a naming bug.
NAME_CROSSWALK = {
    ("India", "gadm1"): {
        "Punjab": "Punjab(India)",
        # The 2020 merger of these two union territories isn't reflected in the shapefile, which
        # still carries them as separate polygons -- both get the merged CSV value.
        "Dadra and Nagar Haveli": "Daman and Diu and Dadra and Nagar Haveli",
        "Daman and Diu": "Daman and Diu and Dadra and Nagar Haveli",
    },
    ("India", "gadm2_name1"): {
        "Dadra and Nagar Haveli": "Daman and Diu and Dadra and Nagar Haveli",
        "Daman and Diu": "Daman and Diu and Dadra and Nagar Haveli",
    },
    ("Nepal", "gadm1"): {
        # Nepal's provinces were renamed from numbers to names in 2021; the shapefile predates that.
        "Province 1": "Koshi",
    },
    ("Nepal", "gadm2_name1"): {
        "Province 1": "Koshi",
    },
}


def _crosswalk(name: str, country: str, level: str) -> str:
    return NAME_CROSSWALK.get((country, level), {}).get(name, name)


def _slim_geometry(geom, tolerance: float, grid_size: float):
    geom = geom.simplify(tolerance, preserve_topology=True)
    return shapely.set_precision(geom, grid_size)


def _round_floats(obj, ndigits: int):
    # json.dumps prints coordinates at full float precision (15+ digits) regardless of how
    # simplified or grid-snapped the geometry is -- that's what actually bloats these files, far
    # more than vertex count. Rounding first is what shortens the printed string: Python's float
    # repr always prints the shortest string that round-trips, so round(x, 4) reliably prints as
    # e.g. "45.2345" instead of "45.234499999999997".
    if isinstance(obj, float):
        return round(obj, ndigits)
    if isinstance(obj, list):
        return [_round_floats(x, ndigits) for x in obj]
    if isinstance(obj, dict):
        return {k: _round_floats(v, ndigits) for k, v in obj.items()}
    return obj


def load_national_pm() -> pd.DataFrame:
    snap = Snapshot(NATIONAL_SNAPSHOT_URI)
    with snap.extracted() as archive:
        tb = archive.read("gadm0_2024_narrow.csv")
    tb = pd.DataFrame(tb).rename(columns={"name0": "country"})
    tb = tb.loc[tb["year"] == YEAR, ["iso_alpha3", "country", "pm"]].copy()
    tb["pm"] = tb["pm"].round(1)
    return tb


def load_subnational_pm(level: str) -> pd.DataFrame:
    snap = Snapshot(f"aqli/{SUBNATIONAL_SNAPSHOT_VERSION}/air_quality_life_index_{level}.zip")
    with snap.extracted() as archive:
        tb = archive.read(f"{level}_aqli_2024_narrow.csv")
    tb = pd.DataFrame(tb)
    tb = tb[tb["year"] == YEAR].copy()
    tb["pm"] = tb["pm"].round(1)
    return tb


def load_gadm_shapes() -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    # Load the full GADM1 and GADM2 shapefiles once, sharing a single zip extraction. This used to
    # be two functions -- a `load_gadm1_shapes()` called once, and a `load_gadm2_shapes(country)`
    # that took a `where=` filter and was called once per country (252 times). Each call to the
    # latter re-extracted the *entire* ~2GB archive (both shapefiles) into a fresh temp dir just to
    # read one country's slice, since `Snapshot.extracted()` decompresses on every call regardless
    # of how much of the archive is actually used -- 252 redundant extractions of a 2GB zip was most
    # of the pipeline's runtime. Extracting once and reading each shapefile fully (a ~2s read for
    # all 48,155 GADM2 features -- cheap since there's no per-call file-open/query overhead to repeat
    # 252 times) and filtering in memory per country instead removes that redundancy entirely.
    snap = Snapshot(f"aqli/{SUBNATIONAL_SNAPSHOT_VERSION}/air_quality_life_index_shapefiles.zip")
    with snap.extracted() as archive:
        gdf1 = gpd.read_file(archive.path / "gadm1" / "aqli_gadm1_final_june302023.shp").copy()
        gdf2 = gpd.read_file(archive.path / "gadm2" / "aqli_gadm2_final_june302023.shp").copy()
    return gdf1, gdf2


def build_world_json(tb_national: pd.DataFrame, gdf1_all: gpd.GeoDataFrame) -> dict:
    # Simplify each GADM1 polygon *before* dissolving, not after: unary_union's cost scales with
    # input vertex count, so simplifying first turns a ~15-minute dissolve into ~2 minutes.
    # Plain per-polygon simplify() (Douglas-Peucker) picks a different vertex subset for each
    # polygon independently, so two GADM1 neighbors' shared border stops matching exactly -- the
    # dissolve then can't fully merge across it, leaving thin sliver gaps that render as squiggly
    # lines inside the dissolved country. shapely.coverage_simplify() simplifies the whole set of
    # polygons as one edge-matched coverage, keeping shared borders identical on both sides.
    gdf1_simplified = gdf1_all[["name0", "geometry"]].copy()
    gdf1_simplified["geometry"] = shapely.coverage_simplify(
        gdf1_simplified["geometry"].values, tolerance=WORLD_SIMPLIFY_TOLERANCE
    )
    gdf0 = gdf1_simplified.dissolve(by="name0", as_index=False)
    gdf0["geometry"] = gdf0["geometry"].apply(lambda g: shapely.set_precision(g, WORLD_PRECISION_GRID))
    gdf0["country_key"] = gdf0["name0"].apply(_normalize_name)

    tb = tb_national.copy()
    tb["country_key"] = tb["country"].apply(_normalize_name)

    gdf0 = gdf0.merge(tb, on="country_key", how="left")[["name0", "iso_alpha3", "pm", "geometry"]]
    gdf0 = gdf0.rename(columns={"name0": "country"})
    # 2 decimal places matches the 0.01-degree precision grid the geometry was already snapped to.
    return _round_floats(json.loads(gdf0.to_json()), ndigits=2)


def build_country_json(
    country: str,
    shapefile_name0: str,
    iso3: str,
    tb1: pd.DataFrame,
    tb2: pd.DataFrame,
    gdf1_all: gpd.GeoDataFrame,
    gdf2_all: gpd.GeoDataFrame,
) -> dict:
    gdf1 = gdf1_all.loc[gdf1_all["name0"] == shapefile_name0, ["name1", "geometry"]].copy()
    gdf1["geometry"] = gdf1["geometry"].apply(
        lambda g: _slim_geometry(g, COUNTRY_SIMPLIFY_TOLERANCE, COUNTRY_PRECISION_GRID)
    )
    gdf1["match_key"] = gdf1["name1"].apply(lambda n: _normalize_name(_crosswalk(n, country, "gadm1")))

    tb1_country = tb1.loc[tb1["country"] == country, ["name_1", "pm"]].copy()
    tb1_country["match_key"] = tb1_country["name_1"].apply(_normalize_name)
    # A crosswalk entry can point several shapefile regions at the same CSV row (e.g. two union
    # territories the shapefile still carries separately, merged in the CSV since 2020) -- keep
    # only one CSV row per match_key so that's a fan-out from the geometry side, not a duplicate
    # join on the data side.
    tb1_country = tb1_country.drop_duplicates(subset="match_key")

    gdf1 = gdf1.merge(tb1_country[["match_key", "pm"]], on="match_key", how="left")
    gadm1_geojson = json.loads(gdf1[["name1", "pm", "geometry"]].to_json())

    gdf2 = gdf2_all.loc[gdf2_all["name0"] == shapefile_name0, ["name1", "name2", "geometry"]].copy()
    gdf2["geometry"] = gdf2["geometry"].apply(
        lambda g: _slim_geometry(g, COUNTRY_SIMPLIFY_TOLERANCE, COUNTRY_PRECISION_GRID)
    )
    gdf2["match_key"] = (
        gdf2["name1"].apply(lambda n: _normalize_name(_crosswalk(n, country, "gadm2_name1")))
        + "||"
        + gdf2["name2"].apply(lambda n: _normalize_name(_crosswalk(n, country, "gadm2_name2")))
    )

    tb2_country = tb2.loc[tb2["country"] == country, ["name_1", "name_2", "pm"]].copy()
    tb2_country["match_key"] = (
        tb2_country["name_1"].apply(_normalize_name) + "||" + tb2_country["name_2"].apply(_normalize_name)
    )
    tb2_country = tb2_country.drop_duplicates(subset="match_key")

    gdf2 = gdf2.merge(tb2_country[["match_key", "pm"]], on="match_key", how="left")
    gadm2_geojson = json.loads(gdf2[["name1", "name2", "pm", "geometry"]].to_json())

    # 3 decimal places matches the 0.001-degree precision grid the geometry was already snapped to.
    return {
        "country": country,
        "iso3": iso3,
        "gadm1": _round_floats(gadm1_geojson, ndigits=3),
        "gadm2": _round_floats(gadm2_geojson, ndigits=3),
    }


def _format_duration(seconds: float) -> str:
    minutes, secs = divmod(int(seconds), 60)
    return f"{minutes}m{secs:02d}s" if minutes else f"{secs}s"


def main() -> None:
    COUNTRIES_DIR.mkdir(parents=True, exist_ok=True)
    run_start = time.monotonic()

    print("Loading national/subnational PM data and GADM1+GADM2 shapes (one zip extraction)...")
    step_start = time.monotonic()
    tb_national = load_national_pm()
    tb1 = load_subnational_pm("gadm1")
    tb2 = load_subnational_pm("gadm2")
    gdf1_all, gdf2_all = load_gadm_shapes()
    gdf1_all["name0_key"] = gdf1_all["name0"].apply(_normalize_name)
    print(f"  done in {_format_duration(time.monotonic() - step_start)}")

    print("Building world.json (dissolving GADM1 into national borders -- the slowest single step)...")
    step_start = time.monotonic()
    world = build_world_json(tb_national, gdf1_all)
    world_path = OUT_DIR / "world.json"
    world_path.write_text(json.dumps(world, separators=(",", ":")))
    print(
        f"world.json: {world_path.stat().st_size / 1e6:.2f} MB, {len(world['features'])} countries "
        f"({_format_duration(time.monotonic() - step_start)})"
    )

    countries = sorted(tb_national["country"].unique())
    written, skipped, total_bytes = 0, [], 0
    loop_start = time.monotonic()
    for i, country in enumerate(countries, 1):
        key = _normalize_name(country)
        matches = gdf1_all.loc[gdf1_all["name0_key"] == key, "name0"]
        if matches.empty:
            skipped.append(country)
            continue
        shapefile_name0 = matches.iloc[0]
        iso3 = tb_national.loc[tb_national["country"] == country, "iso_alpha3"].iloc[0]

        data = build_country_json(country, shapefile_name0, iso3, tb1, tb2, gdf1_all, gdf2_all)
        out_path = COUNTRIES_DIR / f"{iso3}.json"
        out_path.write_text(json.dumps(data, separators=(",", ":")))
        size = out_path.stat().st_size
        total_bytes += size
        written += 1

        elapsed = time.monotonic() - loop_start
        rate = i / elapsed  # countries/sec, averaged over the whole run so far
        eta = (len(countries) - i) / rate
        print(
            f"[{i}/{len(countries)}] {country} ({iso3}): {size / 1e3:.0f} KB "
            f"-- elapsed {_format_duration(elapsed)}, ETA {_format_duration(eta)}"
        )

    print(f"\nTotal run time: {_format_duration(time.monotonic() - run_start)}")
    if skipped:
        print(f"Skipped (no GADM1 shape match): {skipped}")
    print(
        f"Done. {written} countries written, total {total_bytes / 1e6:.1f} MB, avg {total_bytes / written / 1e3:.0f} KB/country"
    )


if __name__ == "__main__":
    main()
