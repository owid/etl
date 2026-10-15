"""Prototype: AQLI life-years-gained map at national, GADM1, and GADM2 levels.

Not wired into the DAG. The GADM1/GADM2 subnational release only has raw snapshots so far
(no meadow/garden step yet), so this reads straight from the snapshot ZIPs to preview what a
national + subnational drill-down map could look like, for one country at a time.

Run with:
    .venv/bin/python etl/steps/viz/bespoke/aqli/latest/national_subnational_map.py [COUNTRY]

Output: a PNG with three panels (world, GADM1, GADM2), saved to ai/.
"""

import sys
import unicodedata

import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd

from etl.paths import BASE_DIR
from etl.snapshot import Snapshot

# Potential gain in life expectancy (years) if the WHO PM2.5 guideline (5 ug/m3) were met --
# AQLI's headline metric. The alternative is `llpp_nat`, the gain from meeting the country's own
# national standard instead.
METRIC = "llpp_who"
METRIC_LABEL = "Potential gain in life expectancy (years) if the WHO PM2.5 guideline were met"
YEAR = 2024

NATIONAL_SNAPSHOT_URI = "aqli/2026-03-30/air_quality_life_index.zip"
SUBNATIONAL_SNAPSHOT_VERSION = "2026-09-07"


def _strip_accents(name: str) -> str:
    # The GADM1 shapefile and the national/subnational CSVs spell some country and region names
    # with different diacritics (e.g. "México" vs "Mexico") -- normalize before joining on name.
    return "".join(c for c in unicodedata.normalize("NFKD", name) if not unicodedata.combining(c))


def load_national() -> pd.DataFrame:
    snap = Snapshot(NATIONAL_SNAPSHOT_URI)
    with snap.extracted() as archive:
        tb = archive.read("gadm0_2024_narrow.csv")
    return pd.DataFrame(tb).rename(columns={"name0": "country"})


def load_subnational(level: str) -> pd.DataFrame:
    snap = Snapshot(f"aqli/{SUBNATIONAL_SNAPSHOT_VERSION}/air_quality_life_index_{level}.zip")
    with snap.extracted() as archive:
        tb = archive.read(f"{level}_aqli_2024_narrow.csv")
    return pd.DataFrame(tb)


def load_shapes(level: str, country: str | None = None) -> gpd.GeoDataFrame:
    snap = Snapshot(f"aqli/{SUBNATIONAL_SNAPSHOT_VERSION}/air_quality_life_index_shapefiles.zip")
    with snap.extracted() as archive:
        shp_path = archive.path / level / f"aqli_{level}_final_june302023.shp"
        where = f"name0 = '{country}'" if country else None
        # Copy out of the temp extraction dir -- pyogrio needs the sibling .dbf/.shx/.prj files,
        # which are still there, but we read eagerly so the GeoDataFrame outlives the `with` block.
        return gpd.read_file(shp_path, where=where).copy()


def national_shapes() -> gpd.GeoDataFrame:
    # This release doesn't ship a dedicated GADM0 shapefile -- dissolve GADM1 polygons by country.
    gdf1 = load_shapes("gadm1")
    gdf0 = gdf1.dissolve(by="name0", as_index=False)[["name0", "geometry"]]
    gdf0["name0_key"] = gdf0["name0"].apply(_strip_accents)
    return gdf0


def plot_panel(ax, gdf: gpd.GeoDataFrame, title: str) -> None:
    gdf.plot(
        column=METRIC,
        cmap="YlOrRd",
        linewidth=0.2,
        edgecolor="white",
        legend=True,
        legend_kwds={"label": METRIC_LABEL, "shrink": 0.6},
        missing_kwds={"color": "#e0e0e0", "label": "No data"},
        ax=ax,
    )
    ax.set_title(title)
    ax.set_axis_off()


def main(country: str) -> None:
    tb_national = load_national()
    tb_national_year = tb_national[tb_national["year"] == YEAR].copy()
    tb_national_year["country_key"] = tb_national_year["country"].apply(_strip_accents)
    gdf_national = national_shapes().merge(tb_national_year, left_on="name0_key", right_on="country_key", how="left")

    tb_gadm1 = load_subnational("gadm1")
    tb_gadm1_year = tb_gadm1[(tb_gadm1["year"] == YEAR) & (tb_gadm1["country"] == country)]
    gdf_gadm1 = load_shapes("gadm1", country=country).merge(
        tb_gadm1_year, left_on="name1", right_on="name_1", how="left"
    )

    tb_gadm2 = load_subnational("gadm2")
    tb_gadm2_year = tb_gadm2[(tb_gadm2["year"] == YEAR) & (tb_gadm2["country"] == country)]
    gdf_gadm2 = load_shapes("gadm2", country=country).merge(
        tb_gadm2_year, left_on=["name1", "name2"], right_on=["name_1", "name_2"], how="left"
    )

    fig, axes = plt.subplots(1, 3, figsize=(21, 7))
    plot_panel(axes[0], gdf_national, f"World, {YEAR}")
    plot_panel(axes[1], gdf_gadm1, f"{country} — states/provinces (GADM1), {YEAR}")
    plot_panel(axes[2], gdf_gadm2, f"{country} — counties/districts (GADM2), {YEAR}")
    fig.suptitle(METRIC_LABEL)
    fig.tight_layout()

    out_dir = BASE_DIR / "ai"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"aqli_national_subnational_map_{country.lower().replace(' ', '_')}.png"
    fig.savefig(out_path, dpi=150)
    print(f"Saved {out_path}")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "India")
