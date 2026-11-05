"""Prepare country boundaries, published averages, and the full fine-grid timeline.

Prototype only; writes to ai/aqli_gridded. Run this, then serve_country_explorer.py.
By default source grid blocks load on demand. Use --download-all for offline preparation.
"""

import argparse
import json
import os
import ssl
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from http.client import IncompleteRead
from pathlib import Path

import geopandas as gpd
import h5py
import numpy as np
import pandas as pd
import rasterio
from explore_country_grid import CACHE_DIR, S3_BASE, SHAPEFILE_SNAPSHOT_URI, grid_filename
from prepare_webapp_data import BRACKETS, COLORS, OUT_DIR, SCALE_ID, YEARS
from rasterio.transform import from_origin
from rasterio.windows import Window
from shapely import make_valid

from etl.snapshot import Snapshot

HERE = Path(__file__).parent
BOUNDARIES = CACHE_DIR / "country_boundaries.gpkg"
OUTLINES = CACHE_DIR / "country_outlines.gpkg"
SUMMARY_NAME = "GlobalPM25-V6GL03-Annual-1998-2024-wThresFrac.csv"
# Explicit aliases only: no fuzzy joining country statistics to boundaries.
ALIASES = {
    "Bonaire, Sint Eustatius and Saba": "Bonaire; Sint Eustatius and Saba",
    "Cabo Verde": "Cape Verde",
    "Czechia": "Czech Republic",
    "México": "Mexico",
    "Palestine": "Palestina",
    "Republic of the Congo": "Republic of Congo",
    "Réunion": "Reunion",
    "Saint Helena, Ascension and Tris": "Saint Helena",
    "Virgin Islands, U.S.": "Virgin Islands; U.S.",
    "Åland": "Aland",
}


def validate_grid(path: Path) -> None:
    with h5py.File(path) as grid:
        assert grid["PM25"].shape == (13000, 36000), f"Unexpected fine grid: {path}"
        assert np.allclose(np.diff(grid["lat"][:]), 0.01, atol=1e-5)
        assert np.allclose(np.diff(grid["lon"][:]), 0.01, atol=2e-5)


def retry_network(operation, label: str):
    for attempt in range(6):
        try:
            return operation()
        except (urllib.error.URLError, TimeoutError, IncompleteRead, ConnectionError, ssl.SSLError) as error:
            if attempt == 5:
                raise
            print(f"{label}: retry {attempt + 1}/5 after {error}", flush=True)
            time.sleep(min(2**attempt, 16))


def fetch_year(year: int) -> None:
    name = grid_filename("GL", year, True)
    path = CACHE_DIR / name
    if not path.exists():
        print(f"Downloading fine grid {year}…", flush=True)
        temporary = path.with_suffix(".nc.part")
        url = f"{S3_BASE}/FineResolution/GL/Annual/{name}"

        def metadata():
            with urllib.request.urlopen(urllib.request.Request(url, method="HEAD"), timeout=60) as response:
                return int(response.headers["Content-Length"]), response.headers["ETag"]

        length, etag = retry_network(metadata, f"{year} headers")
        checkpoints = CACHE_DIR / f"{name}.checkpoints"
        checkpoints.mkdir(exist_ok=True)
        block_size = 16 * 1024**2
        # Only reuse ranges whose completed write was checkpointed for this exact source.
        # Old partial downloads lack these markers and must be revalidated by downloading.
        resume = temporary.exists() and temporary.stat().st_size == length
        with open(temporary, "r+b" if resume else "w+b") as output:
            output.truncate(length)

            def fetch_range(start: int) -> None:
                end = min(start + block_size, length) - 1
                marker = checkpoints / f"{start}.json"
                expected = {"etag": etag, "length": length, "start": start, "end": end}
                if resume and marker.exists() and json.loads(marker.read_text()) == expected:
                    return

                def transfer():
                    request = urllib.request.Request(url, headers={"Range": f"bytes={start}-{end}", "If-Match": etag})
                    with urllib.request.urlopen(request, timeout=120) as response:
                        assert response.status == 206, "Server did not honor byte ranges"
                        assert response.headers["Content-Range"] == f"bytes {start}-{end}/{length}"
                        position = start
                        while chunk := response.read(1024**2):
                            written = os.pwrite(output.fileno(), chunk, position)
                            assert written == len(chunk), "Incomplete disk write"
                            position += written
                        if position != end + 1:
                            raise IncompleteRead(b"", end + 1 - position)
                    os.fsync(output.fileno())
                    marker.write_text(json.dumps(expected))

                retry_network(transfer, f"{year} range {start}-{end}")

            with ThreadPoolExecutor(max_workers=4) as pool:
                list(pool.map(fetch_range, range(0, length, block_size)))
        validate_grid(temporary)
        temporary.replace(path)
    validate_grid(path)
    print(f"Fine grid {year} ready", flush=True)


def prepare_raster(year: int) -> None:
    """Store one bracket ID per native cell; 255 marks missing data."""
    path = CACHE_DIR / SCALE_ID / f"brackets_{year}.tif"
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        return
    source = CACHE_DIR / grid_filename("GL", year, True)
    temporary = path.with_suffix(".tif.part")
    with (
        h5py.File(source) as grid,
        rasterio.open(
            temporary,
            "w",
            driver="GTiff",
            width=36000,
            height=13000,
            count=1,
            dtype="uint8",
            crs="EPSG:4326",
            transform=from_origin(-180, 70, 0.01, 0.01),
            nodata=255,
            tiled=True,
            blockxsize=256,
            blockysize=256,
            compress="deflate",
            predictor=2,
            BIGTIFF="IF_SAFER",
        ) as target,
    ):
        # One source chunk row at a time avoids repeatedly decompressing huge NetCDF chunks.
        step = grid["PM25"].chunks[0]
        for start in range(0, 13000, step):
            end = min(start + step, 13000)
            values = grid["PM25"][start:end, :][::-1].copy()
            classes = np.searchsorted(BRACKETS, values, side="right").astype(np.uint8)
            classes[(values < 0) | ~np.isfinite(values)] = 255
            target.write(classes, 1, window=Window(0, 13000 - end, 36000, end - start))
    temporary.replace(path)
    print(f"Bracket grid {year} ready", flush=True)


def prepare() -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    if not BOUNDARIES.exists():
        print("Dissolving AQLI boundaries into countries…", flush=True)
        with Snapshot(SHAPEFILE_SNAPSHOT_URI).extracted() as archive:
            frame = gpd.read_file(
                archive.path / "gadm1" / "aqli_gadm1_final_june302023.shp", columns=["name0", "geometry"]
            )
        frame.geometry = frame.geometry.map(make_valid)
        frame = frame.dissolve(by="name0").reset_index()
        # Retain original geometry for country masks; simplify only the display outlines below.
        frame.to_file(BOUNDARIES, driver="GPKG")
    if not OUTLINES.exists():
        frame = gpd.read_file(BOUNDARIES)
        frame.geometry = frame.geometry.simplify(0.015, preserve_topology=True)
        frame.to_file(OUTLINES, driver="GPKG")
    frame = gpd.read_file(OUTLINES)
    assert frame.name0.is_unique and frame.geometry.notna().all()
    summary_path = CACHE_DIR / SUMMARY_NAME
    if not summary_path.exists():
        urllib.request.urlretrieve(f"{S3_BASE}/RegionSummaries/{SUMMARY_NAME}", summary_path)
    summary = pd.read_csv(summary_path)
    assert not summary.duplicated(["Region", "Year"]).any()
    assert set(summary.Year) == set(YEARS)
    regions = set(summary.Region)
    features = []
    missing = []
    for index, row in frame.sort_values("name0").reset_index(drop=True).iterrows():
        name = row.name0
        match = name if name in regions else ALIASES.get(name, name)
        records = summary[summary.Region == match]
        if records.empty:
            missing.append(name)
        else:
            assert set(records.Year) == set(YEARS), f"Incomplete timeline: {name}"
        values = {}
        for _, record in records.iterrows():
            value = record["Population-Weighted PM2.5 [ug/m3]"]
            # The producer's Paracel Islands averages are negative in this release.
            # Report this known invalid series explicitly instead of coloring it as clean air.
            assert pd.isna(value) or value >= 0 or name == "Paracel Islands", f"Invalid mean for {name}"
            values[str(int(record.Year))] = None if pd.isna(value) or value < 0 else float(value)
        geometry = row.geometry
        # The largest component is the initial view, avoiding a world-spanning fit for
        # overseas territories and antimeridian fragments. All components remain on the map.
        parts = list(geometry.geoms) if geometry.geom_type == "MultiPolygon" else [geometry]
        main = max(parts, key=lambda part: part.area)
        west, south, east, north = main.bounds
        features.append(
            {
                "type": "Feature",
                "geometry": geometry.__geo_interface__,
                "properties": {
                    "id": int(index),
                    "name": name,
                    "means": values,
                    "note": "Published country averages are invalid (negative)." if name == "Paracel Islands" else "",
                    "bounds": [[south, west], [north, east]],
                },
            }
        )
    assert len(features) > 150, "Unexpected country coverage"
    data = {
        "type": "FeatureCollection",
        "features": features,
        "years": YEARS,
        "colors": COLORS,
        "brackets": BRACKETS,
        "tile_path": f"tiles/{SCALE_ID}",
        "unmatched_summary_countries": missing,
    }
    (OUT_DIR / "countries.json").write_text(json.dumps(data, allow_nan=False, separators=(",", ":")))

    print(f"Prepared {len(features)} countries. No published average match: {missing}", flush=True)
    if any(not (OUT_DIR / "overlays" / f"{feature['properties']['id']}.json").exists() for feature in features):
        from prepare_country_overlays import main as prepare_overlays

        prepare_overlays()

    cities = []
    for feature in features:
        props = feature["properties"]
        overlay = json.loads((OUT_DIR / "overlays" / f"{props['id']}.json").read_text())
        for i, city in enumerate(overlay["cities"]):
            if -60 <= city["lat"] <= 70:
                cities.append({**city, "id": f"{props['id']}-{i}", "country_id": props["id"], "country": props["name"]})
    cities.sort(key=lambda city: city["population"], reverse=True)
    (OUT_DIR / "cities.json").write_text(json.dumps({"cities": cities}, ensure_ascii=False))
    from publish_prototype_pages import publish_pages

    publish_pages()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--download-all", action="store_true")
    args = parser.parse_args()
    prepare()
    if args.download_all:
        with ThreadPoolExecutor(max_workers=3) as pool:
            list(pool.map(fetch_year, reversed(YEARS)))
        # Bound memory and disk pressure during the lossless raster conversion.
        for year in reversed(YEARS):
            prepare_raster(year)
