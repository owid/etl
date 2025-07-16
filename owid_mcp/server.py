"""
Our World in Data – FastMCP Server (prototype v0.2)
---------------------------------------------------
• Exposes search & fetch endpoints compatible with ChatGPT Deep research
  and any other Model Context Protocol (MCP) client.
• Supports two domain‑specific search tools:
    – search_indicator → ind://{id}
    – search_chart     → chart://{slug}
• Fetch resources return:
    – JSON (data+metadata) for indicators
    – SVG (image/svg+xml) for charts, with alt‑text header

Env vars (optional overrides):
    OWID_DATASETTE_BASE   – base URL for Datasette (default public instance)
    OWID_API_BASE         – base URL for OWID v1 indicator API
    GRAPHER_BASE          – base URL for SVG charts
    PORT                  – port to serve on (default 9000)

Dependencies (pip):
    fastmcp httpx uvicorn
"""

import asyncio
import os
import re
import urllib.parse
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import httpx
from fastmcp import FastMCP
from fastmcp.server.server import Response

# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass
class ChartSearchResult:
    title: str
    resource_uri: str
    snippet: str
    metadata: Dict
    score: float


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DATASETTE_BASE = os.getenv("OWID_DATASETTE_BASE", "https://datasette-public.owid.io/owid.json")
OWID_API_BASE = os.getenv("OWID_API_BASE", "https://api.ourworldindata.org/v1/indicators")
GRAPHER_BASE = os.getenv("GRAPHER_BASE", "https://ourworldindata.org/grapher")
HTTP_TIMEOUT = httpx.Timeout(10.0)

# Common entities list for instructions
COMMON_ENTITIES = """
Abkhazia
Afghanistan
Africa
Akrotiri and Dhekelia
Aland Islands
Albania
Algeria
American Samoa
Andorra
Angola
Anguilla
Antarctica
Antigua and Barbuda
Argentina
Armenia
Aruba
Asia
Australia
Austria
Austria-Hungary
Azerbaijan
Bahamas
Bahrain
Bangladesh
Barbados
Belarus
Belgium
Belize
Benin
Bermuda
Bhutan
Bolivia
Bonaire Sint Eustatius and Saba
Bosnia and Herzegovina
Bosnia and Herz.
Botswana
Bouvet Island
Brazil
British Indian Ocean Territory
British Virgin Islands
Brunei
Bulgaria
Burkina Faso
Burundi
Cambodia
Cameroon
Canada
Cape Verde
Cayman Islands
Central African Republic
Chad
Channel Islands
Chile
China
Christmas Island
Cocos Islands
Colombia
Comoros
Congo
Cook Islands
Costa Rica
Cote d'Ivoire
Croatia
Cuba
Curacao
Cyprus
Czechia
Czechoslovakia
Democratic Republic of Congo
DR Congo
Denmark
Djibouti
Dominica
Dominican Republic
East Germany
East Timor
Ecuador
Egypt
El Salvador
Equatorial Guinea
Eritrea
Ethiopia (former)
Estonia
Eswatini
Ethiopia
Europe
European Union (27)
Faroe Islands
Falkland Islands
Fiji
Finland
France
French Guiana
French Polynesia
French Southern Territories
Gabon
Gambia
Gaza Strip
Georgia
Germany
Ghana
Gibraltar
Greece
Greenland
Grenada
Guadeloupe
Guam
Guatemala
Guernsey
Guinea
Guinea-Bissau
Guyana
Haiti
Heard Island and McDonald Islands
Honduras
Hong Kong
Hungary
Iceland
India
Indonesia
Iran
Iraq
Ireland
Isle of Man
Israel
Italy
Jamaica
Japan
Jersey
Jordan
Kazakhstan
Kenya
Kiribati
Kosovo
Kuwait
Kyrgyzstan
Laos
Latvia
Lebanon
Lesotho
Liberia
Libya
Liechtenstein
Lithuania
Luxembourg
Macao
Madagascar
Malawi
Malaysia
Maldives
Mali
Malta
Marshall Islands
Martinique
Mauritania
Mauritius
Mayotte
Melanesia
Mexico
Micronesia (country)
Moldova
Monaco
Mongolia
Montenegro
Montserrat
Morocco
Mozambique
Myanmar
Nagorno-Karabakh
Namibia
Nauru
Nepal
Netherlands
Netherlands Antilles
New Caledonia
New Zealand
Nicaragua
Niger
Nigeria
Niue
Norfolk Island
North America
North Korea
North Macedonia
Northern Cyprus
Northern Mariana Islands
Norway
Oceania
Oman
Pakistan
Palau
Palestine
Panama
Papua New Guinea
Paraguay
Peru
Philippines
Pitcairn
Poland
Polynesia
Portugal
Puerto Rico
Qatar
Democratic Republic of Vietnam
Republic of Vietnam
Reunion
Romania
Russia
Rwanda
Saint Barthelemy
Saint Helena
Saint Kitts and Nevis
Saint Lucia
Saint Martin (French part)
Saint Pierre and Miquelon
Saint Vincent and the Grenadines
Samoa
San Marino
Sao Tome and Principe
Saudi Arabia
Senegal
Serbia
Serbia and Montenegro
Serbia excluding Kosovo
Seychelles
Sierra Leone
Singapore
Sint Maarten (Dutch part)
Slovakia
Slovenia
Solomon Islands
Somalia
Somaliland
South Africa
South America
South Georgia and the South Sandwich Islands
South Korea
South Ossetia
South Sudan
Spain
Sri Lanka
Sudan
Sudan (former)
Suriname
Svalbard and Jan Mayen
Sweden
Switzerland
Syria
Taiwan
Tajikistan
Tanzania
Thailand
Togo
Tokelau
Tonga
Transnistria
Trinidad and Tobago
Tunisia
Turkey
Turkmenistan
Turks and Caicos Islands
Turks and Caicos
Tuvalu
USSR
Uganda
Ukraine
United Arab Emirates
United Kingdom
Korea (former)
United States
United States Minor Outlying Islands
United States Virgin Islands
Uruguay
Uzbekistan
Vanuatu
Vatican
Venezuela
Vietnam
Wallis and Futuna
West Germany
Western Sahara
World
Yemen
Yemen Arab Republic
Yemen People's Republic
Yugoslavia
Zambia
Zanzibar
Zimbabwe
Grand Duchy of Tuscany
Kingdom of the Two Sicilies
Duchy of Modena and Reggio
Kingdom of Sardinia
Duchy of Parma and Piacenza
Grand Duchy of Baden
Kingdom of Bavaria
Kingdom of Saxony
Kingdom of Wurttemberg
Great Colombia
Federal Republic of Central America
"""

# ---------------------------------------------------------------------------
# FastMCP server instance
# ---------------------------------------------------------------------------
mcp = FastMCP(
    name="OWID MCP Prototype",
    instructions=(
        "Search and fetch indicators & charts from Our World in Data. "
        "Call `search_indicator` or `search_chart` to discover resources. "
        "Fetch indicators via `ind://{id}` for all data or `ind://{id}/{entity}` for specific country/entity data. "
        "Fetch charts via `chart://{slug}`. "
        "Entity names must match exactly as they appear in OWID:\n"
        f"{COMMON_ENTITIES}"
    ),
)

# ---------------------------------------------------------------------------
# Search tools
# ---------------------------------------------------------------------------


@mcp.tool
async def search_indicator(query: str, limit: int = 10) -> List[Dict]:
    """Search for OWID indicators by name or description.

    Returns a list of indicators matching the query, each with:
    - title: indicator name
    - resource_uri: URI to fetch the indicator data (e.g., 'ind://2118')
    - snippet: truncated description
    - score: relevance score

    Use the resource_uri with ReadMcpResourceTool to get the actual data.
    """
    sql = """
    SELECT
        id, name, description
    FROM variables
    WHERE
        name LIKE :q COLLATE NOCASE
        OR description LIKE :q COLLATE NOCASE
    LIMIT :limit
    """
    params = {"q": f"%{query}%", "limit": limit}
    qs = urllib.parse.urlencode({"sql": sql, **params})
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        resp = await client.get(f"{DATASETTE_BASE}?{qs}")
        resp.raise_for_status()
        rows = resp.json()["rows"]

    return [
        {
            "title": row[1],
            "resource_uri": f"ind://{row[0]}",
            "snippet": (row[2] or "")[:160],
            "score": 1.0 - idx / max(1, len(rows)),
        }
        for idx, row in enumerate(rows)
    ]


@mcp.tool
async def search_chart(query: str, limit: int = 10) -> List[ChartSearchResult]:
    """Search for OWID charts by slug, title, or note.

    Returns a list of charts matching the query, each with:
    - title: chart title
    - resource_uri: URI to fetch the chart SVG (e.g., 'chart://population-density')
    - snippet: truncated chart note/description
    - metadata: additional chart metadata
    - score: relevance score

    Use the resource_uri with ReadMcpResourceTool to get the chart image.
    """
    sql = """
    SELECT
        slug, title, note, json_extract(config,'$.dimensions[0].variableId')
    FROM charts
    WHERE
        slug LIKE :q COLLATE NOCASE
        OR title LIKE :q COLLATE NOCASE
        OR note LIKE :q COLLATE NOCASE
    LIMIT :limit
    """
    params = {"q": f"%{query}%", "limit": limit}
    qs = urllib.parse.urlencode({"sql": sql, **params})
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        resp = await client.get(f"{DATASETTE_BASE}?{qs}")
        resp.raise_for_status()
        rows = resp.json()["rows"]

    return [
        ChartSearchResult(
            title=row[1] or row[0],
            resource_uri=f"chart://{row[0]}",
            snippet=(row[2] or "")[:160],
            metadata={"variable_id": row[3]},
            score=1.0 - idx / max(1, len(rows)),
        )
        for idx, row in enumerate(rows)
    ]


# ---------------------------------------------------------------------------
# Fetch resources
# ---------------------------------------------------------------------------


def _build_rows(data_json: Dict[str, Any], entities_meta: Dict[int, Dict[str, str]]) -> List[Dict[str, Any]]:
    """Convert the compact OWID arrays into a list[{entity, year, value}]."""

    values = data_json["values"]
    years = data_json["years"]
    entity_ids = data_json["entities"]

    # Guard against length mismatch
    if not (len(values) == len(years) == len(entity_ids)):
        raise ValueError("Mismatched lengths in OWID data arrays")

    rows: List[Dict[str, Any]] = []
    append = rows.append

    for v, y, eid in zip(values, years, entity_ids):
        meta = entities_meta.get(eid)
        if meta is None:
            # Skip unknown entity id (should not normally happen)
            continue
        append(
            {
                "entity": meta["name"],
                "year": y,
                "value": v,
            }
        )

    return rows


async def _fetch_json(url: str) -> Dict[str, Any]:
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        resp = await client.get(url)
        resp.raise_for_status()
        return resp.json()


@mcp.resource("ind://{indicator_id}")
async def indicator_resource(indicator_id: int, entity: str | None = None) -> Dict[str, Any]:
    """Return indicator **data** and **metadata** in a flat, convenient format.

    Args:
        indicator_id: Numeric OWID indicator id (e.g. ``2118``).
        entity: Optional entity name **or** ISO‑3 code. If provided, the server
                 returns only rows matching that entity (case‑insensitive).

    Response JSON schema::

        {
          "metadata": { <OWID metadata unchanged> },
          "data": [
              {"entity": "United States", "year": 2019, "value": 36.0},
              ...
          ]
        }
    """

    # Fetch OWID raw data + metadata concurrently.
    data_url = f"{OWID_API_BASE}/{indicator_id}.data.json"
    meta_url = f"{OWID_API_BASE}/{indicator_id}.metadata.json"
    data_json, metadata = await asyncio.gather(_fetch_json(data_url), _fetch_json(meta_url))

    # Build mapping from numeric id -> {name, code}
    entities_meta = {
        ent["id"]: {"name": ent["name"], "code": ent["code"]} for ent in metadata["dimensions"]["entities"]["values"]
    }

    rows = _build_rows(data_json, entities_meta)

    # Optional server‑side filter for a single entity
    if entity is not None:
        ent_lower = entity.lower()
        # Filter by entity name or code (check against entities_meta for code matching)
        filtered_rows = []
        for r in rows:
            if r["entity"] and r["entity"].lower() == ent_lower:
                filtered_rows.append(r)
            else:
                # Check if entity matches any code in entities_meta
                for ent_meta in entities_meta.values():
                    if ent_meta["code"] and ent_meta["code"].lower() == ent_lower and ent_meta["name"] == r["entity"]:
                        filtered_rows.append(r)
                        break
        rows = filtered_rows

    return {"metadata": metadata, "data": rows}


# Convenience alias: ``ind://{indicator_id}/{entity}``
@mcp.resource("ind://{indicator_id}/{entity}")
async def indicator_resource_for_entity(indicator_id: int, entity: str) -> Dict[str, Any]:
    """Shorthand path that simply forwards to ``indicator_resource`` with ``entity``."""
    # Fetch OWID raw data + metadata concurrently.
    data_url = f"{OWID_API_BASE}/{indicator_id}.data.json"
    meta_url = f"{OWID_API_BASE}/{indicator_id}.metadata.json"
    data_json, metadata = await asyncio.gather(_fetch_json(data_url), _fetch_json(meta_url))

    # Build mapping from numeric id -> {name, code}
    entities_meta = {
        ent["id"]: {"name": ent["name"], "code": ent["code"]} for ent in metadata["dimensions"]["entities"]["values"]
    }

    rows = _build_rows(data_json, entities_meta)

    # Filter for the specific entity
    if entity is not None:
        ent_lower = entity.lower()
        # Filter by entity name or code (check against entities_meta for code matching)
        filtered_rows = []
        for r in rows:
            if r["entity"] and r["entity"].lower() == ent_lower:
                filtered_rows.append(r)
            else:
                # Check if entity matches any code in entities_meta
                for ent_meta in entities_meta.values():
                    if ent_meta["code"] and ent_meta["code"].lower() == ent_lower and ent_meta["name"] == r["entity"]:
                        filtered_rows.append(r)
                        break
        rows = filtered_rows

    return {"metadata": metadata, "data": rows}


@mcp.resource("chart://{slug}")
async def chart_resource(slug: str, **query) -> Response:
    """Return the SVG image of a chart slug; forwards query parameters (country, time…)."""
    qs = ("?" + urllib.parse.urlencode(query)) if query else ""
    source = f"{GRAPHER_BASE}/{slug}.svg{qs}"
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as c:
        r = await c.get(source)
        r.raise_for_status()
        svg = r.content

    # Attempt to extract <title> for alt‑text
    match = re.search(rb"<title>([^<]{1,200})</title>", svg)
    alt = match.group(1).decode("utf-8") if match else slug.replace("-", " ")

    return Response(
        content=svg,
        media_type="image/svg+xml",
        headers={
            "X-Alt-Text": alt,
            "X-Source-URI": source,
        },
    )


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    mcp.run()
