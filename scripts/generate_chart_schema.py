#!/usr/bin/env python3
"""
Generate a chart.yml JSON schema by fetching the upstream grapher schema
and adapting it for ETL graph step usage.

Differences from the upstream grapher schema:
  - `$schema` is not required (multidim files may omit it)
  - `dimensions` is not required at top level (multidim files use views instead)
  - `additionalProperties` is allowed at top level (for ETL-specific fields like
    `views`, `definitions`, `default_selection`, `topic_tags`, `slug`)
  - `title` also accepts an object with `title` and `title_variant` (for multidim)
  - dimension items accept `catalogPath` instead of requiring `variableId`
  - dimension items accept multidim fields: `slug`, `name`, `choices`
  - `customNumericValues` items also accept strings (for template placeholders)

Usage:
    python scripts/generate_chart_schema.py

This will write schemas/chart-schema.json.
"""

import copy
import json
import urllib.request

from etl.config import DEFAULT_GRAPHER_SCHEMA
from etl.paths import SCHEMAS_DIR

OUTPUT_PATH = SCHEMAS_DIR / "chart-schema.json"


def fetch_upstream_schema(url: str) -> dict:
    with urllib.request.urlopen(url) as resp:
        return json.loads(resp.read())


def adapt_schema(schema: dict) -> dict:
    schema = copy.deepcopy(schema)

    # 1. Remove top-level required (no fields are mandatory in chart.yml)
    schema.pop("required", None)

    # 2. Allow additional top-level properties (views, definitions, etc.)
    schema["additionalProperties"] = True

    # 3. Allow `title` to be either a string or an object (for multidim title_variant)
    if "title" in schema.get("properties", {}):
        schema["properties"]["title"] = {
            "oneOf": [
                {"type": "string"},
                {
                    "type": "object",
                    "properties": {
                        "title": {"type": "string"},
                        "title_variant": {"type": "string"},
                    },
                    "required": ["title"],
                    "additionalProperties": False,
                },
            ],
            "description": "Chart title. Can be a string or an object with title and title_variant for multidimensional charts.",
        }

    # 4. Adapt dimension items: allow either grapher-style or multidim-style dimensions
    dims = schema.get("properties", {}).get("dimensions", {})
    if "items" in dims:
        items = dims["items"]

        # Remove all required fields — neither property nor variableId is mandatory
        # (multidim dimensions use slug/name/choices instead)
        items.pop("required", None)

        # Allow additional properties in dimension items (for multidim fields)
        items["additionalProperties"] = True

        # Add catalogPath to properties
        items.setdefault("properties", {})["catalogPath"] = {
            "type": "string",
            "description": "Catalog path to the indicator variable (ETL-specific, resolved to variableId at runtime)",
        }

    # 5. Allow strings in customNumericValues (for template placeholders like {threshold})
    _allow_string_in_custom_numeric_values(schema)

    return schema


def _allow_string_in_custom_numeric_values(schema: dict) -> None:
    """Patch customNumericValues in $defs/colorScale to also accept string items
    (for template placeholders like {global_per_capita_threshold})."""
    color_scale = schema.get("$defs", {}).get("colorScale", {})
    cnv = color_scale.get("properties", {}).get("customNumericValues", {})
    if cnv.get("items", {}).get("type") == "number":
        cnv["items"] = {"oneOf": [{"type": "number"}, {"type": "string"}]}


def main():
    print(f"Fetching upstream schema from {DEFAULT_GRAPHER_SCHEMA}...")
    schema = fetch_upstream_schema(DEFAULT_GRAPHER_SCHEMA)

    print("Adapting schema for chart.yml files...")
    adapted = adapt_schema(schema)

    print(f"Writing to {OUTPUT_PATH}...")
    with open(OUTPUT_PATH, "w") as f:
        json.dump(adapted, f, indent=2)
        f.write("\n")

    print("Done.")


if __name__ == "__main__":
    main()
