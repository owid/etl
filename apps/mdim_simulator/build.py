"""Build the MDim simulator: a single-file HTML page for sketching an MDim with the Grapher npm package.

The page can start empty, or pre-filled from a setup in the same YAML the page copies out ("Copy MDim setup").
The pre-filled indicators' data and metadata, and the fonts, are inlined; Grapher itself loads from the Tailnet,
and indicators linked in the page later are fetched live from OWID's public API.

    .venv/bin/python apps/mdim_simulator/build.py [OUTPUT_PATH] [--setup SETUP.yml]
    .venv/bin/python apps/mdim_simulator/build.py --check     # can this machine reach Grapher (Tailscale)?

OUTPUT_PATH defaults to ai/mdim_simulator/index.html (gitignored). Each output file keeps its own saved work in
the browser, and a new setup starts fresh rather than reopening what was saved for the previous one.
"""

import argparse
import base64
import hashlib
import json
import re
import sys
from pathlib import Path

import yaml

from etl.config import DEFAULT_GRAPHER_SCHEMA
from etl.http import session as http_session

HERE = Path(__file__).parent
TEMPLATE = HERE / "template.html"
SCRIPT = HERE / "simulator.js"
REPO = HERE.parents[1]
DEFAULT_OUT = REPO / "ai" / "mdim_simulator" / "index.html"
DATASET_SCHEMA = REPO / "schemas" / "dataset-schema.json"

# Shown as the example in an empty indicator field when nothing is pre-filled: "Number of people using the
# Internet" (WDI), in its newest version.
EXAMPLE_INDICATOR = ("worldbank_wdi", "wdi", "wdi", "it_net_user_zs_number")  # namespace, dataset, table, indicator
PACKAGE_BASE = "https://owid-packages.tail6e23.ts.net/ourworldindata/grapher/v0.2.0"
DEFAULT_SELECTION = ["World", "Africa", "Asia", "Europe", "North America", "South America", "Oceania"]
DATASETTE = "https://datasette-public.owid.io/owid.json"
API = "https://api.ourworldindata.org/v1/indicators"

FONTS = [
    ("Lato", 400, "normal", "LatoLatin-Regular"),
    ("Lato", 400, "italic", "LatoLatin-Italic"),
    ("Lato", 500, "normal", "LatoLatin-Medium"),
    ("Lato", 700, "normal", "LatoLatin-Bold"),
    ("Lato", 900, "normal", "LatoLatin-Black"),
    ("Playfair Display", 400, "normal", "PlayfairDisplayLatin-Regular"),
    ("Playfair Display", 600, "normal", "PlayfairDisplayLatin-SemiBold"),
]


def csv_cell(value) -> str:
    text = "" if value is None else str(value)
    return '"' + text.replace('"', '""') + '"' if any(ch in text for ch in ',"\n') else text


def topic_tags() -> tuple[list[str], dict[str, str]]:
    """Valid topic tags, as ETL validates them, and the slug of each one's topic page."""
    schema = json.loads(DATASET_SCHEMA.read_text())
    presentation = schema["properties"]["tables"]["additionalProperties"]["properties"]["variables"]
    presentation = presentation["additionalProperties"]["properties"]["presentation"]
    tags = presentation["properties"]["topic_tags"]["items"]["enum"]
    rows = http_session.get(
        DATASETTE,
        params={"sql": "select name, slug from tags where slug is not null and slug != ''", "_shape": "array"},
    ).json()
    slugs = {r["name"]: r["slug"] for r in rows if r["name"] in tags}
    return tags, slugs


def grapher_schema_version() -> str:
    """The version an MDim pins in `grapher_schema`, e.g. "011", taken from ETL's default schema URL."""
    match = re.search(r"grapher-schema\.(\d+)\.json", DEFAULT_GRAPHER_SCHEMA)
    assert match, f"Can't read a version from {DEFAULT_GRAPHER_SCHEMA}"
    return match.group(1)


def color_schemes() -> list[str]:
    """Map colour schemes the vendored grapher schema accepts."""
    (schema_path,) = (REPO / "schemas").glob(f"grapher-schema.{grapher_schema_version()}.json")
    return json.loads(schema_path.read_text())["$defs"]["colorScheme"]["enum"]


def latest_indicator(namespace: str, dataset: str, table: str, indicator: str) -> tuple[int, str]:
    """ID and catalog path of the newest published version of an indicator, found by name."""
    pattern = re.compile(rf"grapher/{namespace}/([^/]+)/{dataset}/{table}#{indicator}")
    rows = http_session.get(
        DATASETTE,
        params={
            # `_` is a LIKE wildcard, so the query casts a wide net and the regex keeps exact matches.
            "sql": "select id, catalogPath from variables where catalogPath like :pattern",
            "pattern": f"grapher/{namespace}/%/{dataset}/{table}#{indicator}",
            "_shape": "array",
        },
    ).json()
    matches = [(m.group(1), r["id"], r["catalogPath"]) for r in rows if (m := pattern.fullmatch(r["catalogPath"]))]
    assert matches, f"No published indicator {namespace}/{dataset}/{table}#{indicator}"
    _, variable_id, catalog_path = max(matches)
    return variable_id, catalog_path


def indicator_id(catalog_path: str) -> int:
    """ID of a published indicator, from its full catalog path."""
    if not catalog_path.startswith("grapher/") or "#" not in catalog_path:
        sys.exit(
            f"Use full catalog paths in the setup (grapher/<namespace>/<version>/<dataset>/<table>#<indicator>): {catalog_path}"
        )
    rows = http_session.get(
        DATASETTE.replace(".json", "/variables.json"),
        params={"catalogPath__exact": catalog_path, "_shape": "array", "_col": "id"},
    ).json()
    if not rows:
        sys.exit(f"No published indicator has this catalog path: {catalog_path}")
    return rows[0]["id"]


def indicator_data(variable_id: int) -> dict:
    """Metadata and data of an indicator, with the data as the CSV the page feeds to Grapher."""
    meta = http_session.get(f"{API}/{variable_id}.metadata.json").json()
    data = http_session.get(f"{API}/{variable_id}.data.json").json()
    entities = {e["id"]: e for e in meta["dimensions"]["entities"]["values"]}
    rows = [f"entityName,entityCode,entityId,year,ind_{variable_id}"]
    for value, year, entity_id in zip(data["values"], data["years"], data["entities"]):
        e = entities[entity_id]
        rows.append(",".join([csv_cell(e["name"]), csv_cell(e.get("code")), str(entity_id), str(year), str(value)]))
    return {"meta": meta, "csv": "\n".join(rows)}


def description_key_blocks(description_key: str | list) -> list[str]:
    """The page's editable blocks for a description_key, as descriptionKeyBlocks() in simulator.js makes them."""
    if not description_key:
        return []
    if isinstance(description_key, list):
        return [f"- {item}" for item in description_key]
    blocks: list[str] = []
    for line in str(description_key).split("\n"):
        if line.startswith("- ") or not blocks:
            blocks.append(line)
        else:
            blocks[-1] += f"\n{line}"
    return blocks


def state_from_setup(setup: dict) -> tuple[dict, list[int]]:
    """The page's state for a setup in the copied-setup format, and the IDs of the indicators it links."""
    dims = [
        {
            "id": f"d-{d['slug']}",
            "name": d.get("name", ""),
            **({"description": d["description"]} if d.get("description") else {}),
            "choices": [
                {
                    "id": f"c-{d['slug']}-{c['slug']}",
                    "name": c.get("name", ""),
                    **({"description": c["description"]} if c.get("description") else {}),
                    **({"group": c["group"]} if c.get("group") else {}),
                }
                for c in d.get("choices", [])
            ],
        }
        for d in setup.get("dimensions") or []
    ]

    def key_of(choices: dict) -> str:
        return "|".join(f"c-{d['slug']}-{choices[d['slug']]}" for d in setup.get("dimensions") or [])

    links, text_edits, ids = {}, {}, []
    for view in setup.get("views") or []:
        key = key_of(view.get("dimensions") or {})
        y = view["indicators"]["y"][0]
        path = y["catalogPath"] if isinstance(y, dict) else y
        variable_id = indicator_id(path)
        ids.append(variable_id)
        links[key] = {"input": path, "id": variable_id}

        own: dict = {}
        config = view.get("config") or {}
        for field in ("title", "subtitle", "note"):
            if field in config:
                own[field] = config[field]
        color_scale = (config.get("map") or {}).get("colorScale") or {}
        map_settings = {k: color_scale[k] for k in ("baseColorScheme", "binningStrategy") if k in color_scale}
        if map_settings:
            own["map"] = map_settings
        description_key = (view.get("metadata") or {}).get("description_key")
        if description_key is not None:
            own["descriptionKey"] = description_key_blocks(description_key)
        display = y.get("display") or {} if isinstance(y, dict) else {}
        if display.get("roundingMode") == "significantFigures" and "numSignificantFigures" in display:
            own["rounding"] = {"mode": "significantFigures", "digits": display["numSignificantFigures"]}
        elif "numDecimalPlaces" in display:
            own["rounding"] = {"mode": "decimalPlaces", "digits": display["numDecimalPlaces"]}
        if own:
            text_edits[key] = own

    shared = ((setup.get("definitions") or {}).get("common_views") or [{}])[0].get("config") or {}
    default_dimensions = setup.get("default_dimensions") or {}
    state = {
        "open": True,
        "title": (setup.get("title") or {}).get("title", ""),
        "titleVariant": (setup.get("title") or {}).get("title_variant", ""),
        "topicTags": list(setup.get("topic_tags") or []),
        "dims": dims,
        "links": links,
        "selected": {f"d-{slug}": f"c-{slug}-{choice}" for slug, choice in default_dimensions.items()},
        "defaultKey": key_of(default_dimensions) if default_dimensions else None,
        "textEdits": {"views": text_edits},
        "suggest": {},
        "defaultSelection": list(setup.get("default_selection") or DEFAULT_SELECTION),
    }
    if "chartTypes" in shared:
        state["chartTypes"] = shared["chartTypes"]
    if "hasMapTab" in shared:
        state["hasMapTab"] = shared["hasMapTab"]
    if "tab" in shared:
        state["defaultTab"] = shared["tab"]
    state["hideTimeInTitle"] = bool((shared.get("hideAnnotationFieldsInTitle") or {}).get("time", True))
    state["changeCountry"] = shared.get("addCountryMode") == "change-country"
    return state, list(dict.fromkeys(ids))


def check_grapher() -> None:
    """Exit with a plain message if the Grapher package can't be reached, which means Tailscale is off."""
    try:
        ok = http_session.get(f"{PACKAGE_BASE}/package.json", timeout=8).ok
    except Exception:
        ok = False
    if not ok:
        sys.exit("Can't reach Grapher, which the page needs to draw charts. Turn on Tailscale and try again.")
    print("Grapher is reachable.")


def build(out: Path, setup_path: Path | None = None) -> None:
    setup_text = setup_path.read_text() if setup_path else ""
    state, ids = state_from_setup(yaml.safe_load(setup_text)) if setup_path else (None, [])
    indicators = [indicator_data(i) for i in ids]
    example_path = indicators[0]["meta"]["catalogPath"] if indicators else latest_indicator(*EXAMPLE_INDICATOR)[1]
    # One saved sketch per page, and a new setup never reopens the work saved for an earlier one.
    digest = hashlib.sha1(setup_text.encode()).hexdigest()[:8] if setup_text else "blank"
    storage_key = f"owid-mdim-simulator:{out.stem}:{digest}"

    font_faces = []
    for family, weight, style, file in FONTS:
        woff2 = http_session.get(f"https://ourworldindata.org/fonts/{file}.woff2").content
        b64 = base64.b64encode(woff2).decode()
        font_faces.append(
            f'@font-face{{font-family:"{family}";src:url(data:font/woff2;base64,{b64}) format("woff2");'
            f"font-weight:{weight};font-style:{style};font-display:swap}}"
        )

    tags, tag_slugs = topic_tags()

    def as_js(value) -> str:
        # "</" inside inline JSON would end the <script> element early.
        return json.dumps(value).replace("</", "<\\/")

    html = (
        TEMPLATE.read_text()
        .replace("__SCRIPT__", SCRIPT.read_text())
        .replace("__PACKAGE_BASE__", PACKAGE_BASE)
        .replace("__FONT_FACES__", "\n".join(font_faces))
        .replace("__INITIAL_STATE__", as_js(state))
        .replace("__INITIAL_INDICATORS__", as_js(indicators))
        .replace("__EXAMPLE_PATH__", as_js(example_path))
        .replace("__STORAGE_KEY__", as_js(storage_key))
        .replace("__DEFAULT_SELECTION__", as_js(DEFAULT_SELECTION))
        .replace("__GRAPHER_SCHEMA_VERSION__", as_js(grapher_schema_version()))
        .replace("__TOPIC_TAGS__", as_js(tags))
        .replace("__TAG_SLUGS__", as_js(tag_slugs))
        .replace("__COLOR_SCHEMES__", as_js(color_schemes()))
    )

    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(html)
    filled = f"pre-filled with {len(ids)} indicator(s)" if setup_path else "empty"
    print(f"wrote {out} ({out.stat().st_size / 1024:.0f} KB), {filled}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build the MDim simulator page.")
    parser.add_argument("out", nargs="?", type=Path, default=DEFAULT_OUT, help="Where to write the page.")
    parser.add_argument("--setup", type=Path, help="Pre-fill the page from a setup in the copied-setup format.")
    parser.add_argument("--check", action="store_true", help="Only check that Grapher can be reached.")
    args = parser.parse_args()
    if args.check:
        check_grapher()
    else:
        build(args.out, args.setup)
