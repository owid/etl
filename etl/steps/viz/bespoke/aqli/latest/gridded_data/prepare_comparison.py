"""Build the standalone AQLI administrative-area versus SatPM grid comparison."""

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

from etl.paths import BASE_DIR
from etl.snapshot import Snapshot

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from prepare_pm_map_data import _crosswalk, _normalize_name  # noqa: E402
from prepare_webapp_data import OUT_DIR, YEARS  # noqa: E402

HERE = Path(__file__).parent


def load_series(level):
    with Snapshot(f"aqli/2026-09-07/air_quality_life_index_{level}.zip").extracted() as archive:
        frame = archive.read(f"{level}_aqli_2024_narrow.csv")
    frame = pd.DataFrame(frame)
    assert set(frame.year) == set(YEARS)
    assert frame.pm.dropna().ge(0).all()
    frame["key"] = frame.name_1.map(_normalize_name)
    if level == "gadm2":
        frame["key"] += "||" + frame.name_2.map(_normalize_name)
    counts = frame.groupby(["country", "key", "year"], observed=True).pm.nunique()
    ambiguous = set(counts[counts > 1].index)
    print(f"{level}: {len(ambiguous)} ambiguous region-years; shown as missing", flush=True)
    indexed = pd.MultiIndex.from_frame(frame[["country", "key", "year"]])
    frame.loc[indexed.isin(ambiguous), "pm"] = np.nan
    frame = frame.drop_duplicates(["country", "key", "year"])
    return frame.pivot(index=["country", "key"], columns="year", values="pm").reindex(columns=YEARS), ambiguous


def main():
    source = BASE_DIR / "ai/aqli_pm_map"
    assert (source / "countries").exists(), "Run ../prepare_pm_map_data.py to prepare the boundary mappings first"
    grid = json.loads((OUT_DIR / "countries.json").read_text())
    world = json.loads((source / "world.json").read_text())
    iso_by_shape_name = {f["properties"]["country"]: f["properties"]["iso_alpha3"] for f in world["features"]}
    series = {level: load_series(level) for level in ("gadm1", "gadm2")}
    target = OUT_DIR / "comparison"
    target.mkdir(exist_ok=True)
    index, audit = [], []
    for feature in grid["features"]:
        props = feature["properties"]
        iso = iso_by_shape_name.get(props["name"])
        path = source / "countries" / f"{iso}.json"
        if not path.exists():
            audit.append({"country": props["name"], "reason": "No cached AQLI country geometry"})
            continue
        payload = json.loads(path.read_text())
        for level in ("gadm1", "gadm2"):
            missing = 0
            for region in payload[level]["features"]:
                p = region["properties"]
                k1 = _normalize_name(
                    _crosswalk(p["name1"], payload["country"], "gadm1" if level == "gadm1" else "gadm2_name1")
                )
                key = (
                    k1
                    if level == "gadm1"
                    else k1 + "||" + _normalize_name(_crosswalk(p["name2"], payload["country"], "gadm2_name2"))
                )
                lookup = (payload["country"], key)
                table, ambiguous = series[level]
                if lookup in table.index:
                    values = table.loc[lookup].to_numpy(dtype=float)
                    p["values"] = [None if not np.isfinite(v) else float(v) for v in np.round(values, 1)]
                    p["ambiguous_years"] = [y for y in YEARS if (*lookup, y) in ambiguous]
                    # Independently compare the latest-year value against the existing map export.
                    if YEARS[-1] not in p["ambiguous_years"]:
                        assert p.get("pm") == p["values"][-1], (payload["country"], level, key)
                else:
                    p["values"] = [None] * len(YEARS)
                    missing += 1
                    assert p.get("pm") is None, "A previously matched region lost its data"
                p.pop("pm", None)
            audit.append(
                {
                    "country": props["name"],
                    "level": level,
                    "regions": len(payload[level]["features"]),
                    "unmatched": missing,
                    "ambiguous_regions": [
                        f["properties"].get("name2", f["properties"]["name1"])
                        for f in payload[level]["features"]
                        if f["properties"].get("ambiguous_years")
                    ],
                }
            )
        (target / f"{props['id']}.json").write_text(json.dumps(payload, allow_nan=False, separators=(",", ":")))
        index.append({"id": props["id"], "name": props["name"], "bounds": props["bounds"]})
    assert len(index) > 200, "Unexpected loss of country coverage"
    (target / "index.json").write_text(
        json.dumps(
            {
                "countries": index,
                "years": YEARS,
                "colors": grid["colors"],
                "brackets": grid["brackets"],
                "tile_path": grid["tile_path"],
            }
        )
    )
    (target / "matching_audit.json").write_text(json.dumps(audit, indent=2))
    from publish_prototype_pages import publish_pages

    publish_pages()
    print(f"Comparison: {len(index)} countries, {YEARS[0]}–{YEARS[-1]}; matching audit saved.")


if __name__ == "__main__":
    main()
