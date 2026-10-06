"""Prepare country-scoped GADM1 boundaries and GHSL urban-centre labels for the prototype."""

import json

import geopandas as gpd
import pandas as pd
from explore_country_grid import SHAPEFILE_SNAPSHOT_URI
from prepare_webapp_data import CITIES_SNAPSHOT_URI, CITIES_YEAR, LOW_PLAUSIBILITY, OUT_DIR

from etl.snapshot import Snapshot


def main():
    countries = json.loads((OUT_DIR / "countries.json").read_text())["features"]
    ids = {feature["properties"]["name"]: feature["properties"]["id"] for feature in countries}
    print("Reading administrative boundaries…", flush=True)
    with Snapshot(SHAPEFILE_SNAPSHOT_URI).extracted() as archive:
        regions = gpd.read_file(
            archive.path / "gadm1" / "aqli_gadm1_final_june302023.shp", columns=["name0", "name1", "geometry"]
        )
    assert set(regions.name0) == set(ids), "Country coverage differs from the explorer"
    print("Reading urban centres…", flush=True)
    cities = pd.read_excel(
        Snapshot(CITIES_SNAPSHOT_URI).path,
        sheet_name="UC_STATS",
        usecols=["Year", "UCname", "Lat", "Lon", "POP", "Plausibility"],
    )
    cities = cities[(cities.Year == CITIES_YEAR) & (cities.POP > 0) & (cities.Plausibility != LOW_PLAUSIBILITY)]
    cities = cities.dropna(subset=["UCname", "Lat", "Lon"])
    cities = cities[cities.UCname != "N/A"].copy()
    points = gpd.GeoDataFrame(cities, geometry=gpd.points_from_xy(cities.Lon, cities.Lat), crs="EPSG:4326")
    joined = gpd.sjoin(points, regions[["name0", "geometry"]], predicate="within", how="left")
    assert not joined.index.duplicated().any(), "Urban centre assigned to multiple administrative areas"
    unmatched = joined[joined.name0.isna()]
    print(f"Urban centres: {len(joined)}; outside supplied boundaries: {len(unmatched)}", flush=True)
    output = OUT_DIR / "overlays"
    output.mkdir(exist_ok=True)
    (output / "unmatched_cities.json").write_text(unmatched[["UCname", "Lat", "Lon"]].to_json(orient="records"))
    print("Simplifying display boundaries…", flush=True)
    regions.geometry = regions.geometry.simplify(0.005, preserve_topology=True)
    for name, country_id in ids.items():
        frame = regions[regions.name0 == name]
        selected = joined[joined.name0 == name].sort_values("POP", ascending=False)
        features = [
            {"type": "Feature", "properties": {"name": row.name1}, "geometry": row.geometry.__geo_interface__}
            for row in frame.itertuples()
        ]
        labels = [
            {"name": row.UCname, "lat": float(row.Lat), "lon": float(row.Lon), "population": int(row.POP)}
            for row in selected.itertuples()
        ]
        payload = {
            "boundaries": {"type": "FeatureCollection", "features": features},
            "cities": labels,
            "cities_year": CITIES_YEAR,
        }
        (output / f"{country_id}.json").write_text(json.dumps(payload, allow_nan=False, separators=(",", ":")))
    print(f"Prepared overlays for {len(ids)} countries and territories.", flush=True)


if __name__ == "__main__":
    main()
