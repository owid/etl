"""Dump the GADM1 (state/province) name crosswalk used by prepare_pm_map_data.py to a single JSON
file, as a human-readable record of every shapefile-name -> CSV-name correspondence -- including
the ones plain accent-stripping already resolves, not just the hand-curated NAME_CROSSWALK
exceptions.

Writes: name_mapping_gadm1.json, keyed by country -> {shapefile name1: CSV name_1 or null}.
A null value means no CSV region matched at all (a genuine data gap, not a naming bug) -- see
prepare_pm_map_data.py's NAME_CROSSWALK docstring for how those are told apart from real bugs.

Run with:
    .venv/bin/python etl/steps/viz/bespoke/aqli/latest/build_name_mapping.py
"""

import json
from pathlib import Path

import prepare_pm_map_data as m


def main() -> None:
    tb_national = m.load_national_pm()
    tb1 = m.load_subnational_pm("gadm1")
    gdf1_all, _ = m.load_gadm_shapes()
    gdf1_all["name0_key"] = gdf1_all["name0"].apply(m._normalize_name)

    mapping = {}
    unresolved = []
    for country in sorted(tb_national["country"].unique()):
        key = m._normalize_name(country)
        matches = gdf1_all.loc[gdf1_all["name0_key"] == key, "name0"]
        if matches.empty:
            continue  # no GADM1 shapes at all for this country -- not a name-mapping issue.
        shapefile_name0 = matches.iloc[0]

        csv_names = tb1.loc[tb1["country"] == country, "name_1"].dropna().unique()
        csv_by_key = {m._normalize_name(n): n for n in csv_names}

        shp_names = sorted(gdf1_all.loc[gdf1_all["name0"] == shapefile_name0, "name1"].dropna().unique())
        country_mapping = {}
        for shp_name in shp_names:
            match_key = m._normalize_name(m._crosswalk(shp_name, country, "gadm1"))
            csv_name = csv_by_key.get(match_key)
            country_mapping[shp_name] = csv_name
            if csv_name is None:
                unresolved.append((country, shp_name))

        mapping[country] = country_mapping

    out_path = Path(__file__).parent / "name_mapping_gadm1.json"
    out_path.write_text(json.dumps(mapping, ensure_ascii=False, indent=2) + "\n")

    n_regions = sum(len(v) for v in mapping.values())
    print(f"{out_path}: {len(mapping)} countries, {n_regions} regions, {len(unresolved)} unresolved")
    if unresolved:
        print("Unresolved (no CSV match found -- genuine data gaps):")
        for country, name in unresolved:
            print(f"  {country}: {name!r}")


if __name__ == "__main__":
    main()
