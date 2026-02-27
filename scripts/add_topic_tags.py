"""
One-off script to add presentation.topic_tags to tagless datapage variables.

Reads tagless-datapages.csv and injects topic_tags into the appropriate .meta.yml
or .meta.override.yml files across the ETL codebase.

Features:
- Validates tags against database
- Provides suggestions for similar tags
- Dry-run mode to preview changes
- Better error reporting

Usage:
    .venv/bin/python scripts/add_topic_tags.py [--dry-run] [--validate-only]
"""

import csv
import sys
from collections import defaultdict
from difflib import get_close_matches
from pathlib import Path

from etl.config import OWID_ENV
from etl.files import ruamel_dump, ruamel_load

ETL_DIR = Path(__file__).resolve().parent.parent
CSV_PATH = ETL_DIR / "tagless-datapages.csv"
STEPS_DIR = ETL_DIR / "etl" / "steps" / "data"


def slugify_tag(tag: str) -> str:
    """Convert display tag to slug format."""
    return tag.lower().replace(" & ", "-").replace("&", "and").replace(" ", "-").replace(",", "")


def load_valid_tags() -> dict[str, str]:
    """Load valid tag slugs and their display names from database.

    Returns:
        dict mapping slug -> display name (e.g. 'ozone-layer' -> 'Ozone Layer')
    """
    try:
        df = OWID_ENV.read_sql("SELECT DISTINCT slug, name FROM tags WHERE slug IS NOT NULL ORDER BY slug")
        return dict(zip(df["slug"], df["name"]))
    except Exception as e:
        print(f"WARNING: Could not load tags from database: {e}")
        print("Proceeding without tag validation...")
        return {}


def validate_tags(
    tags: list[str], slug_to_name: dict[str, str]
) -> tuple[list[str], list[tuple[str, str, list[str]]]]:
    """
    Validate tags and return (valid_tag_names, invalid_tags_with_suggestions).

    Args:
        tags: List of raw tag strings from CSV
        slug_to_name: Dict mapping slug -> display name

    Returns:
        valid_tags: List of valid tag display names (for YAML)
        invalid_tags: List of (original_tag, slug, slug_suggestions) tuples
    """
    valid_slugs = set(slug_to_name.keys())
    valid_names = set(slug_to_name.values())
    name_to_slug = {name: slug for slug, name in slug_to_name.items()}

    valid = []
    invalid = []

    for tag in tags:
        # Try exact name match first
        if tag in valid_names:
            valid.append(tag)
            continue

        # Try case-insensitive name match
        tag_lower = tag.lower()
        for name in valid_names:
            if name.lower() == tag_lower:
                valid.append(name)
                break
        else:
            # Try slug match
            slug = slugify_tag(tag)
            if slug in valid_slugs:
                # Use the official display name
                valid.append(slug_to_name[slug])
            else:
                # Find close matches by both slug and name
                slug_suggestions = get_close_matches(slug, valid_slugs, n=3, cutoff=0.6)
                name_suggestions = get_close_matches(tag, valid_names, n=3, cutoff=0.7)

                # Combine and deduplicate suggestions
                all_suggestions = []
                seen_slugs = set()

                # Add name matches first (higher priority)
                for name in name_suggestions:
                    s = name_to_slug[name]
                    if s not in seen_slugs:
                        all_suggestions.append(s)
                        seen_slugs.add(s)

                # Add slug matches
                for s in slug_suggestions:
                    if s not in seen_slugs:
                        all_suggestions.append(s)
                        seen_slugs.add(s)

                invalid.append((tag, slug, all_suggestions[:3]))

    return valid, invalid


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
    try:
        with open(path) as f:
            data = ruamel_load(f)
        if data and data.get("tables"):
            tables = data["tables"]
            if isinstance(tables, dict) and any(
                isinstance(v, dict) and v.get("variables") for v in tables.values() if v is not None
            ):
                return True
    except Exception:
        pass
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
    import argparse

    parser = argparse.ArgumentParser(description="Add topic tags to datapage metadata files")
    parser.add_argument("--dry-run", action="store_true", help="Preview changes without writing files")
    parser.add_argument("--validate-only", action="store_true", help="Only validate tags, don't process files")
    args = parser.parse_args()

    # Load valid tags from database
    print("Loading valid tags from database...")
    slug_to_name = load_valid_tags()
    if slug_to_name:
        print(f"Loaded {len(slug_to_name)} valid tags")
    else:
        print("WARNING: No tags loaded from database, proceeding without validation")

    # Parse CSV
    print(f"\nReading {CSV_PATH}...")
    with open(CSV_PATH) as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    skipped_empty = 0
    parse_errors = []
    tag_validation_errors = []

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

        # Parse and validate tags
        raw_tags = [t.strip() for t in tags_str.split(";") if t.strip()]

        if slug_to_name:
            valid_tag_list, invalid_tag_list = validate_tags(raw_tags, slug_to_name)

            if invalid_tag_list:
                error_msg = f"\n  Variable: {row['slug']}"
                error_msg += f"\n  Catalog path: {catalog_path}"
                for orig_tag, slug, suggestions in invalid_tag_list:
                    error_msg += f"\n    ❌ Invalid tag: '{orig_tag}' (slug: {slug})"
                    if suggestions:
                        # Show display names for suggestions
                        suggestion_names = [f"{s} ({slug_to_name[s]})" for s in suggestions]
                        error_msg += f"\n       Suggestions: {', '.join(suggestion_names)}"
                tag_validation_errors.append(error_msg)

            # Use only valid tags (display names)
            tags = valid_tag_list
        else:
            # No validation, just use raw tags as-is
            tags = raw_tags

        if not tags:
            parse_errors.append(f"No valid tags for {row['slug']}")
            continue

        yaml_path = resolve_yaml_path(parsed["namespace"], parsed["version"], parsed["dataset"])
        if yaml_path is None:
            unresolved.append((catalog_path, parsed))
            continue

        file_updates[yaml_path].append((parsed["table"], parsed["variable"], tags))

    # Print validation errors (limit to first 10 in validate-only mode)
    if tag_validation_errors:
        print("\n" + "=" * 70)
        print(f"TAG VALIDATION ERRORS ({len(tag_validation_errors)} variables with invalid tags)")
        print("=" * 70)
        if args.validate_only:
            # Show first 10 errors
            for error in tag_validation_errors[:10]:
                print(error)
            if len(tag_validation_errors) > 10:
                print(f"\n... and {len(tag_validation_errors) - 10} more variables with invalid tags")
        else:
            for error in tag_validation_errors:
                print(error)

    if args.validate_only:
        # Print summary and exit
        print("\n" + "=" * 70)
        print("VALIDATION SUMMARY")
        print("=" * 70)
        print(f"Total CSV rows:                    {len(rows)}")
        print(f"Skipped (empty catalogPath):       {skipped_empty}")
        print(f"Variables with invalid tags:       {len(tag_validation_errors)}")
        print(f"Parse errors:                      {len(parse_errors)}")
        print(f"Unresolved catalog paths:          {len(unresolved)}")
        print("\n✅ Validation complete (--validate-only mode, no files modified)")
        sys.exit(1 if tag_validation_errors or parse_errors or unresolved else 0)

    # Apply updates
    files_modified = 0
    variables_updated = 0
    variables_skipped_existing = 0

    if args.dry_run:
        print("\n" + "=" * 70)
        print("DRY RUN - Preview of changes")
        print("=" * 70)

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

            # Check if existing tags are valid
            existing_tags = presentation.get("topic_tags", [])
            if existing_tags:
                # Validate existing tags
                valid_names = set(slug_to_name.values())
                existing_valid = [t for t in existing_tags if t in valid_names]
                existing_invalid = [t for t in existing_tags if t not in valid_names]

                if existing_invalid:
                    # Has invalid tags - replace with CSV tags
                    if args.dry_run:
                        print(f"  FIX: {var_name}")
                        print(f"       Old (invalid): {existing_tags}")
                        print(f"       New (valid):   {tags}")
                    presentation["topic_tags"] = tags
                    variables_updated += 1
                    modified = True
                elif set(existing_valid) != set(tags):
                    # Has valid tags but different from CSV - update to CSV
                    if args.dry_run:
                        print(f"  UPDATE: {var_name}")
                        print(f"          Old: {existing_tags}")
                        print(f"          New: {tags}")
                    presentation["topic_tags"] = tags
                    variables_updated += 1
                    modified = True
                else:
                    # Already has correct tags
                    if args.dry_run:
                        print(f"  OK: {var_name} already has correct tags: {tags}")
                    variables_skipped_existing += 1
            else:
                # No existing tags - add new ones
                if args.dry_run:
                    print(f"  ADD: {var_name} -> {tags}")
                presentation["topic_tags"] = tags
                variables_updated += 1
                modified = True

        if modified and not args.dry_run:
            with open(yaml_path, "w") as f:
                f.write(ruamel_dump(data))
            files_modified += 1
            rel_path = yaml_path.relative_to(ETL_DIR)
            print(f"  ✅ Modified: {rel_path} ({sum(1 for _, _, _ in updates)} variables)")

    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Total CSV rows:                    {len(rows)}")
    print(f"Skipped (empty catalogPath):       {skipped_empty}")
    print(f"Variables with invalid tags:       {len(tag_validation_errors)}")
    print(f"Variables updated:                 {variables_updated}")
    print(f"Variables skipped (existing tags): {variables_skipped_existing}")

    if not args.dry_run:
        print(f"Files modified:                    {files_modified}")
    else:
        print(f"Files that would be modified:      {len([f for f in file_updates.keys() if any(True for _, _, _ in file_updates[f])])}")

    if parse_errors:
        print(f"\n❌ Parse errors ({len(parse_errors)}):")
        for e in parse_errors:
            print(f"  - {e}")

    if unresolved:
        print(f"\n❌ Unresolved catalog paths ({len(unresolved)}):")
        for cp, parsed in unresolved[:10]:  # Show first 10
            print(f"  - {cp}")
            print(f"    Looked for: garden/{parsed['namespace']}/{parsed['version']}/{parsed['dataset']}.meta.yml")
        if len(unresolved) > 10:
            print(f"  ... and {len(unresolved) - 10} more")

    if args.dry_run:
        print("\n💡 Run without --dry-run to apply changes")

    # Exit with error if there were issues
    has_errors = bool(tag_validation_errors or parse_errors or unresolved)
    if has_errors:
        print("\n⚠️  Completed with errors")
        sys.exit(1)
    else:
        print("\n✅ Success")


if __name__ == "__main__":
    main()
