"""
One-off script to add presentation.topic_tags to tagless datapage variables.

Reads tagless-datapages.csv and injects topic_tags into the appropriate .meta.yml
or .meta.override.yml files across the ETL codebase.

Usage:
    .venv/bin/python ai/add_topic_tags.py
"""

import csv
import sys
from collections import defaultdict
from pathlib import Path

from etl.files import ruamel_dump, ruamel_load

ETL_DIR = Path(__file__).resolve().parent.parent
CSV_PATH = ETL_DIR / "tagless-datapages.csv"
STEPS_DIR = ETL_DIR / "etl" / "steps" / "data"


def parse_catalog_path(catalog_path: str) -> dict:
    """Parse grapher/{ns}/{ver}/{ds}/{table}#{variable} into components."""
    # Remove leading 'grapher/' prefix
    path = catalog_path.removeprefix("grapher/")
    # Split table#variable
    table_part, variable = path.rsplit("#", 1)
    parts = table_part.split("/")
    return {
        "namespace": parts[0],
        "version": parts[1],
        "dataset": parts[2],
        "table": parts[3],
        "variable": variable,
    }


def _garden_meta_has_variables(path: Path) -> bool:
    """Check if a garden .meta.yml has a non-empty tables section with variable entries."""
    with open(path) as f:
        data = ruamel_load(f)
    if data and data.get("tables"):
        tables = data["tables"]
        if isinstance(tables, dict) and any(
            isinstance(v, dict) and v.get("variables") for v in tables.values() if v is not None
        ):
            return True
    return False


def resolve_yaml_path(ns: str, ver: str, ds: str) -> Path | None:
    """
    Resolve which YAML file to edit, in priority order:
    1. garden .meta.override.yml (if exists)
    2. garden .meta.yml (if exists AND has a non-empty tables section with variables)
    2b. garden nested .meta.yml: garden/{ns}/{ver}/{ds}/{ds}.meta.yml (same check)
    3. grapher .meta.yml (if exists)
    4. Fall back to garden .meta.yml (create tables section if needed)
    5. Create grapher .meta.yml alongside existing .py file
    """
    garden_override = STEPS_DIR / "garden" / ns / ver / f"{ds}.meta.override.yml"
    garden_meta = STEPS_DIR / "garden" / ns / ver / f"{ds}.meta.yml"
    garden_nested = STEPS_DIR / "garden" / ns / ver / ds / f"{ds}.meta.yml"
    grapher_meta = STEPS_DIR / "grapher" / ns / ver / f"{ds}.meta.yml"
    grapher_py = STEPS_DIR / "grapher" / ns / ver / f"{ds}.py"

    # Priority 1: override file
    if garden_override.exists():
        return garden_override

    # Priority 2: garden meta with non-empty tables
    if garden_meta.exists() and _garden_meta_has_variables(garden_meta):
        return garden_meta

    # Priority 2b: nested garden directory (e.g. excess_mortality/latest/excess_mortality/excess_mortality.meta.yml)
    if garden_nested.exists() and _garden_meta_has_variables(garden_nested):
        return garden_nested

    # Priority 3: grapher meta
    if grapher_meta.exists():
        return grapher_meta

    # Priority 4: fall back to garden meta
    if garden_meta.exists():
        return garden_meta
    if garden_nested.exists():
        return garden_nested

    # Priority 5: create grapher .meta.yml alongside existing .py file
    if grapher_py.exists():
        return grapher_meta

    return None


def main():
    # Parse CSV
    with open(CSV_PATH) as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    skipped_empty = 0
    parse_errors = []

    # Group variables by target YAML file
    # file_path -> [(table, variable, tags)]
    file_updates: dict[Path, list[tuple[str, str, list[str]]]] = defaultdict(list)
    unresolved = []

    for row in rows:
        catalog_path = row["catalogPath"].strip()
        if not catalog_path:
            skipped_empty += 1
            continue

        tags_str = row["grapherTags"].strip()
        if not tags_str:
            parse_errors.append(f"Empty tags for {row['slug']}")
            continue

        try:
            parsed = parse_catalog_path(catalog_path)
        except Exception as e:
            parse_errors.append(f"Failed to parse {catalog_path}: {e}")
            continue

        tags = [t.strip() for t in tags_str.split(";") if t.strip()]

        yaml_path = resolve_yaml_path(parsed["namespace"], parsed["version"], parsed["dataset"])
        if yaml_path is None:
            unresolved.append((catalog_path, parsed))
            continue

        file_updates[yaml_path].append((parsed["table"], parsed["variable"], tags))

    # Apply updates
    files_modified = 0
    variables_updated = 0
    variables_skipped_existing = 0

    for yaml_path, updates in sorted(file_updates.items()):
        if yaml_path.exists():
            with open(yaml_path) as f:
                data = ruamel_load(f)
            if data is None:
                data = {}
        else:
            data = {}

        modified = False

        for table_name, var_name, tags in updates:
            # Ensure tables.<table>.variables.<variable>.presentation.topic_tags
            if "tables" not in data or data["tables"] is None:
                data["tables"] = {}

            tables = data["tables"]
            if table_name not in tables or tables[table_name] is None:
                tables[table_name] = {}

            table = tables[table_name]
            if "variables" not in table or table["variables"] is None:
                table["variables"] = {}

            variables = table["variables"]
            if var_name not in variables or variables[var_name] is None:
                variables[var_name] = {}

            var_meta = variables[var_name]
            if "presentation" not in var_meta or var_meta["presentation"] is None:
                var_meta["presentation"] = {}

            presentation = var_meta["presentation"]

            if "topic_tags" in presentation and presentation["topic_tags"]:
                existing = presentation["topic_tags"]
                print(f"  WARNING: {var_name} already has topic_tags={list(existing)}, skipping")
                variables_skipped_existing += 1
                continue

            presentation["topic_tags"] = tags
            variables_updated += 1
            modified = True

        if modified:
            with open(yaml_path, "w") as f:
                f.write(ruamel_dump(data))
            files_modified += 1
            rel_path = yaml_path.relative_to(ETL_DIR)
            print(f"  Modified: {rel_path} ({sum(1 for _, _, _ in updates)} variables)")

    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Total CSV rows:              {len(rows)}")
    print(f"Skipped (empty catalogPath): {skipped_empty}")
    print(f"Variables updated:           {variables_updated}")
    print(f"Variables skipped (existing): {variables_skipped_existing}")
    print(f"Files modified:              {files_modified}")

    if parse_errors:
        print(f"\nParse errors ({len(parse_errors)}):")
        for e in parse_errors:
            print(f"  - {e}")

    if unresolved:
        print(f"\nUnresolved catalog paths ({len(unresolved)}):")
        for cp, parsed in unresolved:
            print(f"  - {cp}")
            print(f"    Looked for: garden/{parsed['namespace']}/{parsed['version']}/{parsed['dataset']}.meta.yml")

    if parse_errors or unresolved:
        sys.exit(1)


if __name__ == "__main__":
    main()
