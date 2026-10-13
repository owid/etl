"""Build the MDim simulator: a single-file HTML page for sketching an MDim with the Grapher npm package.

The starting indicator's data, its metadata and the fonts are inlined. Grapher itself loads from the Tailnet;
any other indicator linked in the page is fetched live from OWID's public API.

    .venv/bin/python apps/mdim_simulator/build.py [OUTPUT_PATH]

OUTPUT_PATH defaults to ai/mdim_simulator/index.html (gitignored).
"""

import base64
import json
import sys
from pathlib import Path

from etl.http import session as http_session

HERE = Path(__file__).parent
TEMPLATE = HERE / "template.html"
REPO = HERE.parents[1]
DEFAULT_OUT = REPO / "ai" / "mdim_simulator" / "index.html"
DATASET_SCHEMA = REPO / "schemas" / "dataset-schema.json"

VARIABLE_ID = 1296069  # grapher/worldbank_wdi/2026-07-27/wdi/wdi#it_net_user_zs_number
PACKAGE_BASE = "https://owid-packages.tail6e23.ts.net/ourworldindata/grapher/v0.2.0"
DEFAULT_SELECTION = ["World", "Africa", "Asia", "Europe", "North America", "South America", "Oceania"]

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
        "https://datasette-public.owid.io/owid.json",
        params={"sql": "select name, slug from tags where slug is not null and slug != ''", "_shape": "array"},
    ).json()
    slugs = {r["name"]: r["slug"] for r in rows if r["name"] in tags}
    return tags, slugs


def build(out: Path) -> None:
    meta = http_session.get(f"https://api.ourworldindata.org/v1/indicators/{VARIABLE_ID}.metadata.json").json()
    data = http_session.get(f"https://api.ourworldindata.org/v1/indicators/{VARIABLE_ID}.data.json").json()

    # Same CSV layout the page builds for indicators it fetches itself.
    entities = {e["id"]: e for e in meta["dimensions"]["entities"]["values"]}
    rows = [f"entityName,entityCode,entityId,year,ind_{VARIABLE_ID}"]
    for value, year, entity_id in zip(data["values"], data["years"], data["entities"]):
        e = entities[entity_id]
        rows.append(",".join([csv_cell(e["name"]), csv_cell(e.get("code")), str(entity_id), str(year), str(value)]))

    font_faces = []
    for family, weight, style, file in FONTS:
        woff2 = http_session.get(f"https://ourworldindata.org/fonts/{file}.woff2").content
        b64 = base64.b64encode(woff2).decode()
        font_faces.append(
            f'@font-face{{font-family:"{family}";src:url(data:font/woff2;base64,{b64}) format("woff2");'
            f"font-weight:{weight};font-style:{style};font-display:swap}}"
        )

    tags, tag_slugs = topic_tags()

    # "</" inside inline JSON would end the <script> element early.
    html = (
        TEMPLATE.read_text()
        .replace("__PACKAGE_BASE__", PACKAGE_BASE)
        .replace("__FONT_FACES__", "\n".join(font_faces))
        .replace("__INITIAL_META__", json.dumps(meta).replace("</", "<\\/"))
        .replace("__DEFAULT_SELECTION__", json.dumps(DEFAULT_SELECTION))
        .replace("__TOPIC_TAGS__", json.dumps(tags))
        .replace("__TAG_SLUGS__", json.dumps(tag_slugs))
        .replace("__INITIAL_CSV__", "\n".join(rows))
    )

    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(html)
    print(f"wrote {out} ({out.stat().st_size / 1024:.0f} KB), {len(rows) - 1} data rows")


if __name__ == "__main__":
    build(Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_OUT)
