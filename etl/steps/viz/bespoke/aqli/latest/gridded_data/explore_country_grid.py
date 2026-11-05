"""Prototype: explore the SatPM V6.GL.03 gridded PM2.5 surface for a single country.

Not wired into the DAG, and deliberately not a snapshot step yet -- this reads straight from
SatPM's public S3 bucket to see what the raw grid looks like before we decide what (if anything)
to ingest. The sibling prototypes in `..` work off AQLI's GADM0/1/2 *aggregates*; this one is the
layer underneath them, the ~1km satellite-derived surface those aggregates are averaged from.

Why S3 and not the Box links advertised on satpm.org: the bucket is anonymously readable over
plain HTTPS (no credentials, no AWS CLI) and supports range requests, so it is the only access
route that a snapshot step could ever automate. See `README.md` next to this file.

Run with:
    .venv/bin/python etl/steps/viz/bespoke/aqli/latest/gridded_data/explore_country_grid.py [COUNTRY]

Output: a PNG (map + value distribution) and a printed sanity check, both under ai/aqli_gridded/.
"""

import sys

import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import rioxarray  # noqa: F401  -- registers the .rio accessor used for clipping
import xarray as xr
from matplotlib.colors import BoundaryNorm

from etl.paths import BASE_DIR
from etl.snapshot import Snapshot

S3_BASE = "https://satpmdata.s3.amazonaws.com/V6GL03"

YEAR = 2024
DEFAULT_COUNTRY = "China"

# The grids carry no _FillValue attribute, so the fill reads back as a real number and silently
# poisons any mean taken over it (63% of the global 0.1 deg grid is fill -- ocean).
#
# Worse, the fill value is NOT the same across resolutions: the coarse 0.1 deg files use -999.0 and
# the fine 0.01 deg files use -999.9. Testing against either literal silently fails to mask the
# other. PM2.5 is a concentration and cannot be negative, and the only negative values in either
# product ARE the fill (verified across a full coarse file and a full fine file: the smallest real
# values are 0.99 and 2.37), so mask on sign instead of on a literal.
FILL_VALUE = -999.0  # kept for reference only; use is_fill()/>= 0, not equality


def is_valid(values):
    """True where the cell holds a real concentration rather than the product's fill value."""
    return values >= 0


# SatPM ships continental cuts alongside the global file. The global fine-resolution grid is
# ~450 MB/year; the continental ones are a tenth of that, so pick the smallest cut that covers
# the country. GL is the fallback, and is a deliberate last resort.
COUNTRY_REGION = {
    "China": "AS",
    "India": "AS",
    "Indonesia": "AS",
    "Pakistan": "AS",
    "Bangladesh": "AS",
    "Nigeria": "AF",
    "Egypt": "AF",
    "South Africa": "AF",
    "Germany": "EU",
    "Poland": "EU",
    "Italy": "EU",
    "United States": "NA",
    "Mexico": "NA",
    "Canada": "NA",
    "Brazil": "SA",
    "Chile": "SA",
}

# WHO 2021 air quality guideline (5) and its interim targets (10, 15, 25, 35), extended upwards to
# cover the top of the range. Classing the map on these rather than on a continuous ramp keeps the
# legend meaningful, and matches the thresholds SatPM's own country summary CSV reports against.
PM25_LEVELS = [0, 5, 10, 15, 25, 35, 50, 75, 100, 150]
WHO_GUIDELINE = 5

SHAPEFILE_SNAPSHOT_URI = "aqli/2026-09-07/air_quality_life_index_shapefiles.zip"

OUT_DIR = BASE_DIR / "ai" / "aqli_gridded"
CACHE_DIR = OUT_DIR / "cache"


def grid_filename(region: str, year: int, fine: bool) -> str:
    resolution = "" if fine else "0p10."
    return f"V6GL03.CNNPM25.{resolution}{region}.{year}01-{year}12.nc"


def download_grid(region: str, year: int, fine: bool = True):
    """Fetch one annual grid from SatPM's public S3 bucket, caching it under ai/."""
    import urllib.request

    name = grid_filename(region, year, fine)
    path = CACHE_DIR / name
    if path.exists():
        return path

    folder = "FineResolution" if fine else "CoarseResolution"
    url = f"{S3_BASE}/{folder}/{region}/Annual/{name}"
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Downloading {url} ...")
    urllib.request.urlretrieve(url, path)
    return path


def open_grid(region: str, year: int, fine: bool = True) -> xr.DataArray:
    """Open one annual grid as a masked, CRS-tagged DataArray of PM2.5 in ug/m3."""
    da = xr.open_dataset(download_grid(region, year, fine), engine="h5netcdf")["PM25"]
    da = da.where(is_valid(da))
    # rioxarray looks for dims literally named x/y; these files use lon/lat.
    return da.rio.set_spatial_dims(x_dim="lon", y_dim="lat").rio.write_crs("EPSG:4326")


def country_geometry(country: str) -> gpd.GeoDataFrame:
    """Country outline, dissolved from the AQLI GADM1 shapefile (no GADM0 layer ships with it)."""
    snap = Snapshot(SHAPEFILE_SNAPSHOT_URI)
    with snap.extracted() as archive:
        shp_path = archive.path / "gadm1" / "aqli_gadm1_final_june302023.shp"
        gdf = gpd.read_file(shp_path, where=f"name0 = '{country}'").copy()
    if gdf.empty:
        raise ValueError(f"No GADM1 features for country {country!r} -- check the spelling.")
    return gdf


def clip_to_country(da: xr.DataArray, gdf: gpd.GeoDataFrame) -> xr.DataArray:
    """Cut the grid down to the country, bbox first so we never rasterize a continent."""
    minx, miny, maxx, maxy = gdf.total_bounds
    # `lat` is ascending in these files, so slice low->high; a reversed slice returns an empty array.
    da = da.sel(lon=slice(minx, maxx), lat=slice(miny, maxy))
    if da.size == 0:
        raise ValueError("Country bounding box falls outside the grid -- wrong regional cut?")
    # The .rio accessor is rebuilt on every derived object, so the spatial dims have to be set
    # again here -- the ones declared in open_grid did not survive the .sel() above.
    da = da.rio.set_spatial_dims(x_dim="lon", y_dim="lat")
    return da.rio.clip(gdf.geometry, gdf.crs, drop=True)


def area_weighted_mean(da: xr.DataArray) -> float:
    """Unweighted cell means over-count high latitudes: a 0.01 deg cell at 50N is ~2/3 the area
    of one at the equator. Weight by cos(latitude) to get a true geographic mean."""
    weights = np.cos(np.deg2rad(da["lat"]))
    return float(da.weighted(weights).mean())


def sanity_check(da: xr.DataArray, country: str, year: int) -> None:
    """Compare our clipped grid against SatPM's own published country summary.

    These will not match exactly -- SatPM aggregates from the full-resolution grid using their own
    country mask, while we clip with AQLI's GADM1 boundaries -- but a large gap means the clip,
    the fill-value mask, or the regional cut is wrong.
    """
    url = f"{S3_BASE}/RegionSummaries/GlobalPM25-V6GL03-Annual-1998-2024-wThresFrac.csv"
    cached = CACHE_DIR / "GlobalPM25-V6GL03-Annual-1998-2024-wThresFrac.csv"
    if not cached.exists():
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        pd.read_csv(url).to_csv(cached, index=False)
    summary = pd.read_csv(cached)

    row = summary[(summary["Region"] == country) & (summary["Year"] == year)]
    ours = area_weighted_mean(da)

    print(f"\n--- Sanity check: {country} {year} ---")
    print(f"  valid grid cells            {int(da.notnull().sum()):,}")
    print(f"  our geographic mean         {ours:.2f} ug/m3  (area-weighted)")
    print(f"  our unweighted cell mean    {float(da.mean()):.2f} ug/m3")
    if row.empty:
        print(f"  !! {country} {year} not in SatPM's country summary -- cannot cross-check.")
        return
    theirs = float(row["Geographic-Mean PM2.5 [ug/m3]"].iloc[0])
    print(f"  SatPM geographic mean       {theirs:.2f} ug/m3")
    print(f"  SatPM pop-weighted mean     {float(row['Population-Weighted PM2.5 [ug/m3]'].iloc[0]):.2f} ug/m3")
    diff = abs(ours - theirs) / theirs * 100
    print(f"  difference                  {diff:.1f}%  {'OK' if diff < 15 else '<-- INVESTIGATE'}")


def plot(da: xr.DataArray, gdf: gpd.GeoDataFrame, country: str, year: int, out_path) -> None:
    cmap = plt.get_cmap("YlOrBr", len(PM25_LEVELS) - 1)
    norm = BoundaryNorm(PM25_LEVELS, cmap.N)

    fig = plt.figure(figsize=(15, 6.5))
    grid = fig.add_gridspec(1, 2, width_ratios=[2.4, 1], wspace=0.16)
    ax_map, ax_hist = fig.add_subplot(grid[0]), fig.add_subplot(grid[1])

    mesh = da.plot.pcolormesh(ax=ax_map, cmap=cmap, norm=norm, add_colorbar=False)
    # Subnational outlines, kept faint -- they are orientation, not the data.
    gdf.boundary.plot(ax=ax_map, linewidth=0.3, edgecolor="white", alpha=0.6)

    bar = fig.colorbar(mesh, ax=ax_map, boundaries=PM25_LEVELS, ticks=PM25_LEVELS, pad=0.02, shrink=0.85)
    bar.set_label("Annual mean PM2.5 (µg/m³)", fontsize=9)
    bar.ax.tick_params(labelsize=8)
    bar.outline.set_visible(False)

    ax_map.set_title(f"{country}, {year} — satellite-derived PM2.5 at 0.01° (~1 km)", fontsize=13, pad=10)
    ax_map.set_xlabel("")
    ax_map.set_ylabel("")
    # These are plain lon/lat axes, so an "equal" aspect stretches everything east-west: at 36N a
    # degree of longitude covers only ~0.8 of a degree of latitude. Scale by 1/cos(lat) instead --
    # a poor man's equirectangular projection, good enough for a single country at a glance.
    ax_map.set_aspect(1 / np.cos(np.deg2rad(float(da["lat"].mean()))))
    for spine in ax_map.spines.values():
        spine.set_visible(False)
    ax_map.tick_params(labelsize=7, colors="#888888", length=0)

    values = da.values[~np.isnan(da.values)]
    ax_hist.hist(values, bins=np.arange(0, 121, 2), color="#B8860B", edgecolor="none")
    ax_hist.axvline(WHO_GUIDELINE, color="#2F6F4E", linewidth=2)
    ax_hist.annotate(
        f"WHO guideline\n{WHO_GUIDELINE} µg/m³",
        xy=(WHO_GUIDELINE, ax_hist.get_ylim()[1] * 0.92),
        xytext=(12, 0),
        textcoords="offset points",
        fontsize=9,
        color="#2F6F4E",
        va="top",
    )
    above = (values >= WHO_GUIDELINE).mean() * 100
    # Don't let a near-total share round to a flat "100.0%" -- for China 2024 that would hide the
    # ~800 cells (of 9.5 million) that actually sit below the guideline.
    above_label = ">99.9" if above >= 99.95 else f"{above:.1f}"
    ax_hist.set_title(
        f"Distribution of grid cells\n{above_label}% of land area above the WHO guideline",
        fontsize=11,
        pad=10,
    )
    ax_hist.set_xlabel("Annual mean PM2.5 (µg/m³)", fontsize=9)
    ax_hist.set_ylabel("Grid cells (millions)", fontsize=9)
    ax_hist.yaxis.set_major_formatter(lambda y, _: f"{y / 1e6:g}")
    ax_hist.tick_params(labelsize=8, colors="#888888")
    for side in ("top", "right"):
        ax_hist.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax_hist.spines[side].set_color("#CCCCCC")
    ax_hist.grid(axis="y", color="#EEEEEE", linewidth=0.8)
    ax_hist.set_axisbelow(True)

    fig.text(
        0.5,
        0.02,
        "Data: SatPM V6.GL.03 (Atmospheric Composition Analysis Group, WashU), CC BY 4.0. Boundaries: GADM via AQLI.",
        ha="center",
        fontsize=8,
        color="#888888",
    )
    fig.savefig(out_path, dpi=150, bbox_inches="tight", facecolor="white")
    print(f"\nWrote {out_path}")


def run(country: str = DEFAULT_COUNTRY, year: int = YEAR) -> None:
    region = COUNTRY_REGION.get(country)
    if region is None:
        print(f"No regional cut mapped for {country!r}; falling back to the global grid (~450 MB).")
        region = "GL"

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Loading {region} grid for {year} ...")
    da = open_grid(region, year)
    print(f"Clipping to {country} ...")
    gdf = country_geometry(country)
    da = clip_to_country(da, gdf)

    sanity_check(da, country, year)
    slug = country.lower().replace(" ", "_")
    plot(da, gdf, country, year, OUT_DIR / f"{slug}_pm25_{year}.png")


if __name__ == "__main__":
    run(sys.argv[1] if len(sys.argv) > 1 else DEFAULT_COUNTRY)
