"""Local prototype server: native-resolution PM2.5 tiles masked to the selected country.

Run prepare_country_explorer.py first. Tiles read small windows from annual fine NetCDFs;
no global array or country-sized browser canvas is allocated. Cached PNGs live under ai/.
"""

import argparse
import io
import json
import math
import re
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import BoundedSemaphore, Lock
from urllib.parse import parse_qs, urlparse

import geopandas as gpd
import h5py
import numpy as np
import rasterio
from explore_country_grid import CACHE_DIR, S3_BASE, grid_filename
from PIL import Image
from prepare_country_explorer import BOUNDARIES
from prepare_webapp_data import BRACKETS, COLORS, OUT_DIR, SCALE_ID, YEARS
from rasterio.features import rasterize
from rasterio.transform import from_bounds
from rasterio.warp import Resampling, reproject
from shapely.geometry import Point, box
from shapely.strtree import STRtree

HALF_WORLD = 20037508.342789244
TILE_SIZE = 256
PALETTE = np.array([tuple(bytes.fromhex(color.lstrip("#"))) + (255,) for color in COLORS], dtype=np.uint8)
RENDER_SLOTS = BoundedSemaphore(4)
GEOMETRY_LOCK = Lock()
REMOTE_LOCK = Lock()


@lru_cache(maxsize=27)
def remote_info(url: str):
    with urllib.request.urlopen(urllib.request.Request(url, method="HEAD"), timeout=60) as response:
        return int(response.headers["Content-Length"]), response.headers["ETag"].strip('"')


class RangeReader(io.RawIOBase):
    """Seekable HTTP file with persistent, verified 1 MB blocks for h5py."""

    block_size = 1024**2

    def __init__(self, url: str):
        self.url = url
        self.length, etag = remote_info(url)
        self.folder = CACHE_DIR / "remote_blocks" / etag
        self.folder.mkdir(parents=True, exist_ok=True)
        self.position = 0

    def readable(self):
        return True

    def seekable(self):
        return True

    def tell(self):
        return self.position

    def seek(self, offset, whence=0):
        self.position = offset + (0 if whence == 0 else self.position if whence == 1 else self.length)
        if self.position < 0:
            raise ValueError("Negative file position")
        return self.position

    def block(self, index):
        path = self.folder / f"{index}.bin"
        start = index * self.block_size
        end = min(start + self.block_size, self.length) - 1
        if path.exists():
            content = path.read_bytes()
        else:
            request = urllib.request.Request(self.url, headers={"Range": f"bytes={start}-{end}"})
            with urllib.request.urlopen(request, timeout=90) as response:
                if response.status != 206 or response.headers["Content-Range"] != f"bytes {start}-{end}/{self.length}":
                    raise OSError("Source did not honor requested byte range")
                content = response.read()
            if len(content) != end - start + 1:
                raise OSError("Incomplete source block")
            temporary = path.with_suffix(".part")
            temporary.write_bytes(content)
            temporary.replace(path)
        if len(content) != end - start + 1:
            raise OSError(f"Invalid cached source block: {path}")
        return content

    def read(self, size=-1):
        end = self.length if size < 0 else min(self.position + size, self.length)
        if end <= self.position:
            return b""
        first, last = self.position // self.block_size, (end - 1) // self.block_size
        with ThreadPoolExecutor(max_workers=4) as pool:
            content = b"".join(pool.map(self.block, range(first, last + 1)))
        content = content[self.position % self.block_size : self.position % self.block_size + end - self.position]
        self.position = end
        return content

    def readinto(self, buffer):
        content = self.read(len(buffer))
        buffer[: len(content)] = content
        return len(content)


@lru_cache(maxsize=8)
def country_geometry(country_id: int):
    name = COUNTRIES[country_id]["name"]
    frame = gpd.read_file(BOUNDARIES, where=f"name0 = '{name.replace(chr(39), chr(39) * 2)}'")
    assert len(frame) == 1, f"Missing or duplicate country: {name}"
    original = frame.geometry.iloc[0]
    projected = frame.to_crs(3857).geometry.iloc[0]
    parts = list(projected.geoms) if projected.geom_type == "MultiPolygon" else [projected]
    return original, STRtree(parts)


def grid_path(year: int) -> Path:
    path = CACHE_DIR / f"fine_{year}.tif"
    if not path.exists():
        raise FileNotFoundError(f"Fine grid for {year} is not downloaded. Run prepare_country_explorer.py.")
    return path


def sample_grid(year: int, latitudes: np.ndarray, longitudes: np.ndarray) -> np.ndarray:
    """Nearest native cells, with explicit coverage masking (no edge clamping into data)."""
    rows = np.floor((latitudes + 60) / 0.01).astype(int)
    cols = np.floor((longitudes + 180) / 0.01).astype(int)
    valid_y = (rows >= 0) & (rows < 13000)
    valid_x = (cols >= 0) & (cols < 36000)
    result = np.full((len(rows), len(cols)), np.nan, dtype=np.float32)
    if not valid_y.any() or not valid_x.any():
        return result
    rr, cc = rows[valid_y], cols[valid_x]
    if (CACHE_DIR / f"fine_{year}.tif").exists():
        with rasterio.open(grid_path(year)) as grid:
            coordinates = [(float(lon), float(lat)) for lat in latitudes[valid_y] for lon in longitudes[valid_x]]
            values = np.array([value[0] for value in grid.sample(coordinates)], dtype=np.float32).reshape(
                len(rr), len(cc)
            )
    else:
        name = grid_filename("GL", year, True)
        local = CACHE_DIR / name
        url = f"{S3_BASE}/FineResolution/GL/Annual/{name}"
        # Serialize sparse-cache updates and HDF5 file-object access. Only requested byte
        # blocks are downloaded; subsequent countries and years reuse them on disk.
        with REMOTE_LOCK:
            source = open(local, "rb") if local.exists() else RangeReader(url)
            with source, h5py.File(source) as grid:
                assert grid["PM25"].shape == (13000, 36000), "Unexpected fine grid shape"
                unique_rows, inverse = np.unique(rr, return_inverse=True)
                values = grid["PM25"][unique_rows, int(cc.min()) : int(cc.max()) + 1][:, cc - cc.min()][inverse]
    values[values < 0] = np.nan
    result[np.ix_(valid_y, valid_x)] = values
    return result


def render_tile(country_id: int, year: int, z: int, x: int, y: int) -> bytes:
    size = 2 * HALF_WORLD / (2**z)
    west, north = -HALF_WORLD + x * size, HALF_WORLD - y * size
    bounds = (west, north - size, west + size, north)
    _, tree = country_geometry(country_id)
    candidates = tree.query(box(*bounds), predicate="intersects")
    rgba = np.zeros((TILE_SIZE, TILE_SIZE, 4), dtype=np.uint8)
    if len(candidates):
        mask = rasterize(
            ((tree.geometries[i], 1) for i in candidates),
            out_shape=(TILE_SIZE, TILE_SIZE),
            transform=from_bounds(*bounds, TILE_SIZE, TILE_SIZE),
            fill=0,
            dtype="uint8",
        ).astype(bool)
        if mask.any():
            if (CACHE_DIR / SCALE_ID / f"brackets_{year}.tif").exists():
                classes = np.full((TILE_SIZE, TILE_SIZE), 255, dtype=np.uint8)
                with rasterio.open(CACHE_DIR / SCALE_ID / f"brackets_{year}.tif") as source:
                    reproject(
                        source=rasterio.band(source, 1),
                        destination=classes,
                        dst_transform=from_bounds(*bounds, TILE_SIZE, TILE_SIZE),
                        dst_crs="EPSG:3857",
                        dst_nodata=255,
                        resampling=Resampling.nearest,
                        warp_mem_limit=32,
                    )
            else:
                offsets = (np.arange(TILE_SIZE) + 0.5) / TILE_SIZE
                longitudes = (west + offsets * size) / HALF_WORLD * 180
                latitudes = np.rad2deg(np.arctan(np.sinh((north - offsets * size) / HALF_WORLD * math.pi)))
                # Avoid fetching source chunks for portions of a tile outside the country.
                rows, cols = np.flatnonzero(mask.any(axis=1)), np.flatnonzero(mask.any(axis=0))
                values = np.full((TILE_SIZE, TILE_SIZE), np.nan, dtype=np.float32)
                values[np.ix_(rows, cols)] = sample_grid(year, latitudes[rows], longitudes[cols])
                classes = np.searchsorted(BRACKETS, values, side="right").astype(np.uint8)
                classes[~np.isfinite(values)] = 255
            valid = mask & (classes != 255)
            rgba[valid] = PALETTE[classes[valid]]
    stream = io.BytesIO()
    Image.fromarray(rgba).save(stream, format="PNG")
    return stream.getvalue()


class Handler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(OUT_DIR), **kwargs)

    def end_headers(self):
        # Page configuration must stay in sync with the current tile palette and URL.
        # Versioned tiles retain their explicit long-lived cache policy.
        if not urlparse(self.path).path.startswith("/tiles/"):
            self.send_header("Cache-Control", "no-store")
        super().end_headers()

    def do_GET(self):
        url = urlparse(self.path)
        match = re.fullmatch(rf"/tiles/{SCALE_ID}/(\d+)/(\d+)/(\d+)/(\d+)/(\d+)\.png", url.path)
        try:
            if match:
                country, year, z, x, y = map(int, match.groups())
                if (
                    country not in COUNTRIES
                    or year not in YEARS
                    or not 0 <= z <= 10
                    or not (0 <= x < 2**z and 0 <= y < 2**z)
                ):
                    self.send_error(400, "Invalid tile coordinates")
                    return
                cached = OUT_DIR / "tiles" / SCALE_ID / str(country) / str(year) / str(z) / str(x) / f"{y}.png"
                if cached.exists():
                    content = cached.read_bytes()
                else:
                    with RENDER_SLOTS:
                        # Serialize the initial geometry lookup; lru_cache alone permits
                        # several simultaneous misses to read the same large boundary file.
                        with GEOMETRY_LOCK:
                            country_geometry(country)
                        content = render_tile(country, year, z, x, y)
                    cached.parent.mkdir(parents=True, exist_ok=True)
                    # Concurrent duplicate requests can safely replace the same complete tile.
                    import tempfile

                    with tempfile.NamedTemporaryFile(dir=cached.parent, delete=False) as temporary:
                        temporary.write(content)
                        temporary_path = Path(temporary.name)
                    temporary_path.replace(cached)
                self.send_response(200)
                self.send_header("Content-Type", "image/png")
                self.send_header("Cache-Control", "public, max-age=86400")
                self.send_header("Content-Length", str(len(content)))
                self.end_headers()
                self.wfile.write(content)
                return
            if url.path == "/value":
                query = parse_qs(url.query)
                country, year = int(query["country"][0]), int(query["year"][0])
                lat, lon = float(query["lat"][0]), float(query["lon"][0])
                if country not in COUNTRIES or year not in YEARS or not (-90 <= lat <= 90 and -180 <= lon <= 180):
                    raise ValueError("Invalid value query")
                geometry, _ = country_geometry(country)
                value = (
                    sample_grid(year, np.array([lat]), np.array([lon]))[0, 0]
                    if geometry.covers(Point(lon, lat))
                    else np.nan
                )
                content = json.dumps({"value": float(value) if np.isfinite(value) else None}).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(content)))
                self.end_headers()
                self.wfile.write(content)
                return
            super().do_GET()
        except (ValueError, KeyError) as error:
            self.send_error(400, str(error))
        except FileNotFoundError as error:
            self.send_error(503, str(error))
        except (BrokenPipeError, ConnectionResetError):
            pass  # Browser canceled an obsolete tile after a country/year change.
        except (OSError, RuntimeError) as error:
            self.log_error("Source data request failed: %s", error)
            self.send_error(502, "Could not read the fine-resolution source; retry this year.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()
    data = json.loads((OUT_DIR / "countries.json").read_text())
    COUNTRIES = {feature["properties"]["id"]: feature["properties"] for feature in data["features"]}
    print(f"Country explorer: http://localhost:{args.port}/", flush=True)
    ThreadingHTTPServer(("127.0.0.1", args.port), Handler).serve_forever()
