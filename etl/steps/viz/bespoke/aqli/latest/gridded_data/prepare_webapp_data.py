"""Build the data the gridded PM2.5 explorer webapp loads: one global raster per year, 1998-2024.

This is the SatPM V6.GL.03 grid and nothing else. An earlier version of this prototype also carried
AQLI's GADM2 district polygons as a second layer; that was dropped to keep the prototype focused on
the gridded product. `../prepare_pm_map_data.py` still owns the district layer if it is wanted back.

Resolution is the coarse 0.1 deg (~11 km) global grid, for every year. This is not a compromise
forced by download size so much as by the rendering approach: the app draws each year as a single
`L.imageOverlay` PNG, and the fine 0.01 deg global grid is 13,000 x 36,000 px, which no browser
will take as one image. Going finer means tiling (COG or PMTiles), which is a different app.

The grid is masked to land before anything else happens. SatPM emits values over coastal and
enclosed water too (the North Sea, the Great Lakes, shelf seas), and those are 16% of every cell
that carries a value -- enough to visibly haze the oceans and to drag every "over land" statistic.
Rasterising GADM1 at the grid's own resolution removes all of it and cuts 0.0% of real land data.

!! The grid is TOTAL PM2.5 -- it includes mineral dust and sea salt. It is NOT the human-caused
   measure OWID's published "Exposure to air pollution from human sources" chart shows, which has
   those removed. Globally that difference is dominated by the Sahara, the Arabian peninsula, the
   Taklamakan and the Gobi, which are the brightest things on this map and nearly uninhabited.

Run with:
    .venv/bin/python etl/steps/viz/bespoke/aqli/latest/gridded_data/prepare_webapp_data.py

Then serve it:
    .venv/bin/python -m http.server 8000 --directory ai/aqli_gridded/webapp
"""

import hashlib
import json
import warnings
from pathlib import Path

import geopandas as gpd
import h5py
import numpy as np
import pandas as pd
import rioxarray  # noqa: F401  -- registers the .rio accessor
import xarray as xr
from explore_country_grid import (
    CACHE_DIR,
    SHAPEFILE_SNAPSHOT_URI,
    WHO_GUIDELINE,
    area_weighted_mean,
    download_grid,
    open_grid,
)
from matplotlib.colors import LinearSegmentedColormap, to_hex
from rasterio.enums import Resampling
from rasterio.features import rasterize
from rasterio.transform import from_origin
from rasterio.warp import transform_bounds

from etl.paths import BASE_DIR
from etl.snapshot import Snapshot

YEARS = list(range(1998, 2025))

# 5 ug/m3 brackets all the way to 90, with an open-ended top class.
BRACKETS = list(range(5, 95, 5))

# The eight colours sampled pixel-by-pixel out of OWID's own "Exposure to air pollution from human
# sources" legend, plus one darker anchor so the ramp still has somewhere to go above 35.
OWID_RAMP = ["#fdf5ec", "#fae7d1", "#f6d2a8", "#f1b176", "#ee934f", "#e07131", "#c95323", "#813415"]
RAMP_EXTENSION = "#2b0f06"

NODATA_INDEX = 255

# Long edge of the written PNG. The source grid is 3600 px wide (360 deg at 0.1 deg), so 4000 is
# marginally oversampled in longitude and gives Mercator some room to stretch the high latitudes.
MAX_RASTER_PX = 4000

# 2024 is drawn from the fine 0.01 deg source instead of the coarse one. It cannot be drawn at its
# native 36,000 x 13,000 px: that is 468M pixels, and a canvas that size fails outright in the
# browser (measured -- 268M works, 468M does not). Block-averaging the source 4x, then rendering
# 8000 px wide, lands at ~5 km: a little over twice the detail of the coarse grid, for ~124 MB of
# decoded RGBA in the hover canvas rather than ~1.9 GB.
FINE_YEARS = {2024}
FINE_DOWNSAMPLE = 4  # 0.01 deg -> 0.04 deg, done blockwise so the full array is never resident
MAX_RASTER_PX_FINE = 8000

# What to tell the reader, per year: (source resolution as shown, approximate ground size).
RESOLUTION_LABELS = {True: ("0.04°", 5), False: ("0.1°", 11)}

# City lookup. Coordinates come from the GHSL Urban Centres snapshot already tracked in this repo,
# so nothing here is hand-entered.
CITIES_SNAPSHOT_URI = "urbanization/2025-12-10/ghsl_urban_centers.xlsx"
N_CITIES = 1000
# GHSL runs to 2100. The repo's own garden step sets START_OF_PROJECTIONS = 2025, so 2020 is the
# last *observed* epoch -- ranking on Year.max() would rank cities by their projected 2100 size,
# which puts Luanda above Tokyo.
CITIES_YEAR = 2020
# Same plausibility scale the meadow step documents: 0 = low, 1 = moderate, 2 = high, 3 = very high.
# The garden step treats 0 as unreliable for observed years; do the same rather than invent a rule.
LOW_PLAUSIBILITY = 0

# Spot checks: (name, lat, lon, expect_data). Verified against the written PNG, not the array, so
# they cover the whole reproject -> classify -> palettise -> encode chain.
LANDMARKS = [
    ("Delhi", 28.6, 77.2, True),
    ("Beijing", 39.9, 116.4, True),
    ("London", 51.5, -0.13, True),
    ("Sahara (Chad)", 21.0, 17.0, True),
    ("mid-Pacific", 0.0, -150.0, False),
    ("mid-Atlantic", 30.0, -40.0, False),
    # Carried a value in the unmasked grid; must be empty once the land mask is applied.
    ("North Sea", 56.0, 3.0, False),
]

APP_HTML = Path(__file__).parent / "pm25_explorer.html"
OUT_DIR = BASE_DIR / "ai" / "aqli_gridded" / "webapp"


def load_cities() -> list[dict]:
    """The N_CITIES largest urban centres, from the GHSL snapshot, as {name, country, lat, lon, pop}."""
    snap = Snapshot(CITIES_SNAPSHOT_URI)
    if not snap.path.exists():
        snap.pull()
    tb = pd.read_excel(snap.path, sheet_name="UC_STATS")

    tb = tb[tb["Year"] == CITIES_YEAR].dropna(subset=["UCname", "Lat", "Lon"])
    tb = tb[(tb["UCname"] != "N/A") & (tb["POP"] > 0) & (tb["Plausibility"] != LOW_PLAUSIBILITY)]
    top = tb.nlargest(N_CITIES, "POP")

    # Every city must sit inside the grid's own extent, or the lookup would fly to a blank spot.
    outside = ((top["Lat"] < -59.95) | (top["Lat"] > 69.95)).sum()
    assert outside == 0, f"{outside} cities fall outside the grid's 60S-70N coverage."

    return [
        {
            "name": row["UCname"],
            "country": row["UNLocName"],
            "lat": round(float(row["Lat"]), 4),
            "lon": round(float(row["Lon"]), 4),
            "pop": int(row["POP"]),
        }
        for _, row in top.iterrows()
    ]


def build_colors(n: int) -> list[str]:
    """Resample OWID's ramp across n classes rather than pinning its 8 colours to the first 8.

    Pinning them and darkening only the tail leaves eight near-identical browns above 35 ug/m3
    (adjacent Lab dE of ~2-5, below the ~5 needed to read a classed choropleth). Respreading the
    whole ramp lifts the worst adjacent pair to dE ~4.9 and averages ~10.8, at the cost of the
    middle classes no longer being byte-identical to the published chart.
    """
    ramp = LinearSegmentedColormap.from_list("owid_pm25", OWID_RAMP + [RAMP_EXTENSION])
    return [to_hex(ramp(i / (n - 1))) for i in range(n)]


COLORS = build_colors(len(BRACKETS) + 1)
SCALE_ID = "pm25-" + hashlib.sha256(json.dumps([BRACKETS, COLORS]).encode()).hexdigest()[:10]


def hex_to_rgb(color: str) -> tuple[int, int, int]:
    return tuple(int(color[i : i + 2], 16) for i in (1, 3, 5))  # type: ignore[return-value]


def land_mask(da) -> np.ndarray:
    """A boolean land mask on the grid's own cells, rasterised from GADM1 and cached to disk.

    SatPM's grid is not land-only: 16% of the cells carrying a value sit over water (coastal and
    enclosed seas). Left in, they haze every coastline and are silently averaged into any statistic
    described as being "over land".

    `all_touched=True` keeps any cell a polygon so much as clips, which is what makes the mask a
    superset of the land that actually has data -- measured, it drops 0.0% of real land values. With
    it off, small islands and thin coastal strips fall through the 0.1 deg grid and get cut.
    """
    lat, lon = da["lat"].values, da["lon"].values
    cached = CACHE_DIR / f"landmask_{len(lat)}x{len(lon)}.npy"
    if cached.exists():
        return np.load(cached)

    print(f"  building land mask {len(lat)}x{len(lon)} from GADM1 (one 2GB extraction)...")
    resolution = float(abs(lon[1] - lon[0]))
    # rasterize wants a north-up transform; `lat` ascends in these files, so build the transform
    # from the top edge and flip the result back afterwards.
    transform = from_origin(lon[0] - resolution / 2, lat[-1] + resolution / 2, resolution, resolution)

    snap = Snapshot(SHAPEFILE_SNAPSHOT_URI)
    with snap.extracted() as archive:
        gdf = gpd.read_file(archive.path / "gadm1" / "aqli_gadm1_final_june302023.shp", columns=["geometry"])

    mask = rasterize(
        ((geom, 1) for geom in gdf.geometry if geom is not None),
        out_shape=(len(lat), len(lon)),
        transform=transform,
        fill=0,
        all_touched=True,
        dtype="uint8",
    ).astype(bool)[::-1]  # back to ascending-lat order, to match the DataArray

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    np.save(cached, mask)
    print(f"  land mask covers {mask.mean() * 100:.1f}% of cells")
    return mask


def open_downsampled_fine(year: int):
    """Read the fine global grid and block-average it down without ever holding it whole.

    The fine grid is 36,000 x 13,000 float32 = 1.87 GB, and masking it would immediately make a
    second copy. Reading it in row blocks and averaging each block down keeps peak memory in the
    low hundreds of MB, and averaging (rather than subsampling) is what preserves the local maxima
    that make the fine source worth using at all.
    """
    path = download_grid("GL", year, fine=True)
    k = FINE_DOWNSAMPLE
    with h5py.File(path, "r") as f:
        pm = f["PM25"]
        lat, lon = f["lat"][:], f["lon"][:]
        height, width = pm.shape[0] // k, pm.shape[1] // k
        out = np.empty((height, width), dtype=np.float32)
        rows_per_block = 200
        for start in range(0, height, rows_per_block):
            stop = min(start + rows_per_block, height)
            block = pm[start * k : stop * k, : width * k].astype(np.float32)
            # Mask on sign: the fine files' fill is -999.9, not the coarse files' -999.0, and
            # neither declares a _FillValue. See is_valid() in explore_country_grid.
            block[block < 0] = np.nan
            with warnings.catch_warnings():
                # Whole blocks out at sea are all-NaN; nanmean warns and returns NaN, which is what
                # we want here.
                warnings.simplefilter("ignore", RuntimeWarning)
                out[start:stop] = np.nanmean(block.reshape(stop - start, k, width, k), axis=(1, 3))

    da = xr.DataArray(
        out,
        coords={
            "lat": lat[: height * k].reshape(height, k).mean(axis=1),
            "lon": lon[: width * k].reshape(width, k).mean(axis=1),
        },
        dims=("lat", "lon"),
        name="PM25",
    )
    return da.rio.write_crs("EPSG:4326")


def to_web_mercator(da, max_px: int = MAX_RASTER_PX):
    """Reproject to EPSG:3857 and cap the long edge, so Leaflet can place the PNG as a plain overlay.

    Leaflet's ImageOverlay stretches an image into a lat/lon box but renders in Web Mercator, so an
    unprojected lon/lat raster is squashed north-south by a growing amount away from the equator --
    which for a whole-world image is a gross distortion, not a subtle one.
    """
    da = da.rio.set_spatial_dims(x_dim="lon", y_dim="lat").rio.reproject("EPSG:3857", nodata=np.nan)
    height, width = da.shape
    if max(height, width) > max_px:
        scale = max_px / max(height, width)
        # Average, not nearest: point-sampling a smooth continuous field on the way down drops
        # local maxima and makes the result depend on which pixels land on the grid.
        da = da.rio.reproject(
            "EPSG:3857",
            shape=(max(1, int(height * scale)), max(1, int(width * scale))),
            resampling=Resampling.average,
            nodata=np.nan,
        )
    return da


def classify(values: np.ndarray) -> np.ndarray:
    indices = np.digitize(values, BRACKETS, right=False).astype(np.uint8)
    return np.where(np.isnan(values), NODATA_INDEX, indices).astype(np.uint8)


def bracket_label(index: int) -> str:
    if index == 0:
        return f"<{BRACKETS[0]}"
    if index >= len(BRACKETS):
        return f"{BRACKETS[-1]}+"
    return f"{BRACKETS[index - 1]}-{BRACKETS[index]}"


def write_raster_png(da, path: Path) -> dict:
    from PIL import Image

    image = Image.fromarray(classify(da.values), mode="P")
    palette: list[int] = []
    for color in COLORS:
        palette.extend(hex_to_rgb(color))
    palette.extend([0, 0, 0] * (256 - len(COLORS)))
    image.putpalette(palette)

    path.parent.mkdir(parents=True, exist_ok=True)
    image.save(path, optimize=True, transparency=NODATA_INDEX)

    left, bottom, right, top = da.rio.bounds()
    west, south, east, north = transform_bounds("EPSG:3857", "EPSG:4326", left, bottom, right, top)
    return {
        "bounds": [[south, west], [north, east]],
        "width": int(da.shape[1]),
        "height": int(da.shape[0]),
        "bytes": path.stat().st_size,
    }


def check_landmarks(path: Path, info: dict) -> list[str]:
    """Read the written PNG back and confirm known places land where they should.

    Catches a flipped axis, an off-by-one in the bounds, or a broken palette -- none of which show
    up in the summary statistics, because those are computed before the image is ever encoded.
    """
    from PIL import Image

    image = Image.open(path)
    indices = np.array(image)
    (south, west), (north, east) = info["bounds"]
    height, width = indices.shape

    # Mercator y, so that the lat -> row mapping matches how the image was actually projected.
    def merc_y(lat: float) -> float:
        return np.log(np.tan(np.pi / 4 + np.radians(lat) / 2))

    y_top, y_bottom = merc_y(north), merc_y(south)
    results = []
    for name, lat, lon, expect_data in LANDMARKS:
        col = int((lon - west) / (east - west) * width)
        row = int((y_top - merc_y(lat)) / (y_top - y_bottom) * height)
        col, row = min(max(col, 0), width - 1), min(max(row, 0), height - 1)
        index = int(indices[row, col])
        has_data = index != NODATA_INDEX
        ok = "OK " if has_data == expect_data else "!! "
        value = bracket_label(index) if has_data else "no data"
        results.append(f"{ok}{name}: {value}")
    return results


def build_year(year: int) -> dict:
    fine = year in FINE_YEARS
    grid = open_downsampled_fine(year) if fine else open_grid("GL", year, fine=False)
    # Mask before the stats, not just before the render: the "over land" figures are wrong
    # otherwise, and wrong quietly. The mask is rebuilt per grid shape and cached.
    grid = grid.where(land_mask(grid))
    values = grid.values[~np.isnan(grid.values)]

    degrees, km = RESOLUTION_LABELS[fine]
    path = OUT_DIR / "rasters" / SCALE_ID / f"{year}.png"
    info = write_raster_png(to_web_mercator(grid, MAX_RASTER_PX_FINE if fine else MAX_RASTER_PX), path)
    return {
        "year": year,
        "raster": f"rasters/{SCALE_ID}/{year}.png",
        "fine": fine,
        "resolution_degrees": degrees,
        "resolution_km": km,
        "grid_mean": round(area_weighted_mean(grid), 1),
        "grid_max": round(float(values.max()), 1),
        "share_above_who": round(float((values >= WHO_GUIDELINE).mean() * 100), 1),
        "share_above_35": round(float((values >= 35).mean() * 100), 1),
        **info,
    }


def main() -> None:
    (OUT_DIR / "rasters").mkdir(parents=True, exist_ok=True)

    print(f"{len(COLORS)} classes, brackets {BRACKETS[0]}-{BRACKETS[-1]}+ ug/m3")
    print("Fetching coarse global grids (5.2 MB/year)...")
    for year in YEARS:
        download_grid("GL", year, fine=False)

    years = []
    for year in YEARS:
        entry = build_year(year)
        years.append(entry)
        print(
            f"  {entry['year']}  {entry['width']}x{entry['height']}  {entry['bytes'] / 1e3:6.0f} KB"
            f"  mean {entry['grid_mean']:5.1f}  max {entry['grid_max']:5.1f}"
            f"  >WHO {entry['share_above_who']:5.1f}%  {entry['resolution_degrees']}"
        )

    print("\nLandmark round-trip through the written PNG (latest year):")
    latest = years[-1]
    for line in check_landmarks(OUT_DIR / latest["raster"], latest):
        print(f"  {line}")

    cities = load_cities()
    print(
        f"\nCity lookup: {len(cities)} urban centres ({CITIES_YEAR} population), "
        f"{cities[0]['name']} ({cities[0]['pop'] / 1e6:.1f}M) down to "
        f"{cities[-1]['name']} ({cities[-1]['pop'] / 1e6:.1f}M)"
    )

    baseline = years[0]["grid_mean"]
    (OUT_DIR / "manifest.json").write_text(
        json.dumps(
            {
                "years": YEARS,
                "brackets": BRACKETS,
                "colors": COLORS,
                "who_guideline": WHO_GUIDELINE,
                "baseline_year": YEARS[0],
                "baseline_mean": baseline,
                "years_data": {str(entry["year"]): entry for entry in years},
                "cities": cities,
            },
            indent=2,
        )
    )

    from publish_prototype_pages import publish_pages

    publish_pages()

    total_mb = sum(entry["bytes"] for entry in years) / 1e6
    print(f"\nDone. {len(years)} world rasters, {total_mb:.1f} MB -> {OUT_DIR}")
    print(
        f"Global area-weighted mean: {years[0]['grid_mean']} ({YEARS[0]}) -> {years[-1]['grid_mean']} ({YEARS[-1]}) ug/m3"
    )
    print("Serve with:  .venv/bin/python -m http.server 8000 --directory ai/aqli_gridded/webapp")


if __name__ == "__main__":
    main()
