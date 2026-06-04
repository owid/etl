#!/usr/bin/env python3
"""
Generate TypedDict classes from JSON schemas for better typing in Collection model.

This script reads the multidim, grapher, and dataset schemas and generates
static TypedDict classes that provide autocompletion and type checking.

Usage:
    python scripts/generate_schema_types.py            # regenerate the file
    python scripts/generate_schema_types.py --check    # fail if the file is out of date

This will update etl/collection/model/schema_types.py with the latest types.

NOTE: hand-written types that are not derived from any JSON schema belong in
`etl/collection/model/params.py`, NOT in the generated file (they would be lost on
regeneration). The only exception is `VIEW_CONFIG_EXTRA_FIELDS` below, which injects
legacy fields into ViewConfig that the schemas don't know about.
"""

import argparse
import difflib
import json
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

from etl.config import DEFAULT_GRAPHER_SCHEMA
from etl.files import get_schema_from_url
from etl.paths import SCHEMAS_DIR

OUTPUT_PATH = Path(__file__).parent.parent / "etl" / "collection" / "model" / "schema_types.py"

# Extra fields injected into ViewConfig that are not part of the multidim schema.
# TODO: remove once we are done with explorers
VIEW_CONFIG_EXTRA_FIELDS = [
    "",
    "    # TODO: remove once we are done with explorers",
    "    # Legacy ID-shortcut fields for scatter/Marimekko color/x/size dimensions. The schema",
    "    # stores integers, but ETL `View.expand_paths` also accepts catalog-path strings",
    "    # (short or full) which `replace_catalog_paths_with_ids` resolves to ints at upload.",
    "    colorVariableId: int | str",
    "    xVariableId: int | str",
    "    sizeVariableId: int | str",
]


class TypedDictGenerator:
    """Generate TypedDict classes from JSON schemas."""

    def __init__(self):
        self.generated_classes: set[str] = set()
        self.imports: set[str] = set()
        self.nested_types: dict[str, dict[str, Any]] = {}  # Maps class names to their schemas

    def json_type_to_python_type(
        self, json_schema: dict[str, Any], property_name: str = "", grapher_schema: dict[str, Any] = None
    ) -> str:
        """Convert JSON schema to Python type annotation."""
        # Handle $ref to grapher schema
        if "$ref" in json_schema and grapher_schema:
            ref = json_schema["$ref"]
            if "grapher-schema" in ref and "#/properties/" in ref:
                # Extract the property path from the $ref
                prop_path = ref.split("#/properties/")[-1]
                try:
                    # Navigate to the referenced property in grapher schema
                    parts = prop_path.split("/")
                    current = grapher_schema.get("properties", {})
                    for part in parts:
                        if part == "properties":
                            continue
                        current = current.get(part, {})

                    if current:
                        # If it's an object with properties, create a nested TypedDict
                        if current.get("type") == "object" and "properties" in current:
                            class_name = self._generate_nested_class_name(property_name)
                            if class_name not in self.generated_classes:
                                self.generated_classes.add(class_name)
                                self.nested_types[class_name] = current
                            return class_name
                        elif "type" in current or "anyOf" in current or "oneOf" in current:
                            return self.json_type_to_python_type(current, property_name, grapher_schema)
                except Exception:
                    pass
            return "dict[str, Any]"  # Fallback for other refs

        json_type = json_schema.get("type")

        # Handle enums (they may appear without an explicit "type", e.g. processing_level)
        if "enum" in json_schema:
            self.imports.add("from typing import Literal")
            enum_values = [repr(v) for v in json_schema["enum"]]
            return f"Literal[{', '.join(enum_values)}]"

        # Handle oneOf/anyOf
        for combinator in ("oneOf", "anyOf"):
            if combinator not in json_schema:
                continue
            options = json_schema[combinator]
            # If all options are objects with properties, merge them into a single TypedDict.
            # (e.g. comparisonLines items: a formula-based shape and a vertical-line shape.)
            if options and all(opt.get("type") == "object" and "properties" in opt for opt in options):
                merged_props: dict[str, Any] = {}
                for opt in options:
                    merged_props.update(opt["properties"])
                merged = {"type": "object", "properties": merged_props}
                return self.json_type_to_python_type(merged, property_name, grapher_schema)
            # Otherwise build a union of the (non-null) option types.
            types = []
            has_null = False
            for option in options:
                if option.get("type") == "null":
                    has_null = True
                else:
                    opt_type = self.json_type_to_python_type(option, property_name, grapher_schema)
                    if opt_type not in types:
                        types.append(opt_type)
            if not types or "Any" in types:
                return "Any"
            union = " | ".join(types)
            return f"{union} | None" if has_null else union

        # Handle arrays of types like ["string", "null"]
        if isinstance(json_type, list):
            if len(json_type) == 2 and "null" in json_type:
                non_null_type = [t for t in json_type if t != "null"][0]
                return f"{self.json_type_to_python_type({'type': non_null_type}, property_name, grapher_schema)} | None"
            else:
                types = [self.json_type_to_python_type({"type": t}, property_name, grapher_schema) for t in json_type]
                return " | ".join(types)

        if json_type == "string":
            return "str"
        elif json_type == "number":
            return "float"
        elif json_type == "integer":
            return "int"
        elif json_type == "boolean":
            return "bool"
        elif json_type == "array":
            if "items" in json_schema:
                item_type = self.json_type_to_python_type(json_schema["items"], property_name, grapher_schema)
                return f"list[{item_type}]"
            return "list[Any]"
        elif json_type == "object":
            # Check if this object has properties that we can create a TypedDict for
            if "properties" in json_schema and json_schema["properties"]:
                # Create a nested TypedDict class name
                class_name = self._generate_nested_class_name(property_name)
                if class_name not in self.generated_classes:
                    self.generated_classes.add(class_name)
                    # Store the properties for later generation
                    self.nested_types[class_name] = json_schema
                return class_name
            return "dict[str, Any]"
        elif json_type == "null":
            return "None"
        else:
            return "Any"

    def _generate_nested_class_name(self, property_name: str) -> str:
        """Generate a class name for a nested object property."""
        # Convert property name to PascalCase
        if not property_name:
            return "NestedObject"

        # Handle camelCase and snake_case
        import re

        # Split on capital letters and underscores
        parts = re.findall(r"[A-Z][a-z]*|[a-z]+|[0-9]+", property_name)
        if not parts:
            parts = [property_name]

        # Convert to PascalCase and add suffix
        class_name = "".join(word.capitalize() for word in parts)
        return f"{class_name}Config" if not class_name.endswith("Config") else class_name

    def _field_lines(
        self,
        prop_name: str,
        prop_schema: dict[str, Any],
        grapher_schema: dict[str, Any] = None,
        quoted: bool = False,
    ) -> list[str]:
        """Generate the (commented) lines for a single field."""
        lines = []
        python_type = self.json_type_to_python_type(prop_schema, prop_name, grapher_schema)

        # Add comment if description exists
        if "description" in prop_schema:
            for desc_line in prop_schema["description"].split("\n"):
                if desc_line.strip():
                    lines.append(f"    # {desc_line.strip()}")

        if quoted:
            lines.append(f'        "{prop_name}": {python_type},')
        else:
            lines.append(f"    {prop_name}: {python_type}")
        return lines

    def generate_typeddict(
        self,
        class_name: str,
        properties: dict[str, Any],
        required: list[str] = None,
        description: str = "",
        grapher_schema: dict[str, Any] = None,
        extra_field_lines: list[str] = None,
    ) -> str:
        """Generate a TypedDict definition.

        Field names that are not valid Python identifiers (e.g. "$schema") cannot be declared
        with class syntax. For those we generate a `_<ClassName>Base` class holding the regular
        fields (so static type checkers see them), subclass it, and add the special fields at
        runtime via `__annotations__.update(...)`.
        """
        if required is None:
            required = []

        safe_props = {k: v for k, v in properties.items() if k.isidentifier() and not k.startswith("$")}
        special_props = {k: v for k, v in properties.items() if k not in safe_props}

        body_class_name = f"_{class_name}Base" if special_props else class_name

        lines = []

        # Class definition - use total=False since config/metadata fields are typically optional
        lines.append(f"class {body_class_name}(TypedDict, total=False):")

        # Docstring
        if special_props:
            special_names = ", ".join(sorted(special_props))
            lines.append(f'    """Base {class_name} without the special-character fields ({special_names})."""')
        elif description:
            lines.append(f'    """{description}"""')
        else:
            lines.append('    """Generated from JSON schema."""')

        if not safe_props and not extra_field_lines:
            lines.append("    pass")
        else:
            lines.append("")
            for prop_name in sorted(safe_props):
                lines.extend(self._field_lines(prop_name, safe_props[prop_name], grapher_schema))
            if extra_field_lines:
                lines.extend(extra_field_lines)

        if special_props:
            lines.append("")
            lines.append("")
            lines.append(f"class {class_name}({body_class_name}, total=False):")
            lines.append(f'    """{description or "Generated from JSON schema."}"""')
            lines.append("")
            lines.append("    pass")
            lines.append("")
            lines.append("")
            lines.append("# Add special-character fields using __annotations__ to avoid syntax issues")
            lines.append(f"{class_name}.__annotations__.update(")
            lines.append("    {")
            for prop_name in sorted(special_props):
                lines.extend(self._field_lines(prop_name, special_props[prop_name], grapher_schema, quoted=True))
            lines.append("    }")
            lines.append(")")

        return "\n".join(lines)

    def extract_view_config_properties(self, multidim_schema: dict[str, Any]) -> dict[str, Any]:
        """Extract view config properties from multidim schema."""
        try:
            return (
                multidim_schema.get("properties", {})
                .get("views", {})
                .get("items", {})
                .get("properties", {})
                .get("config", {})
                .get("properties", {})
            )
        except (KeyError, AttributeError):
            return {}

    def extract_metadata_properties(self, dataset_schema: dict[str, Any]) -> dict[str, Any]:
        """Extract metadata properties from dataset schema."""
        try:
            return (
                dataset_schema.get("properties", {})
                .get("tables", {})
                .get("additionalProperties", {})
                .get("properties", {})
                .get("variables", {})
                .get("additionalProperties", {})
                .get("properties", {})
            )
        except (KeyError, AttributeError):
            return {}

    def generate_file_content(self) -> str:
        """Generate the complete file content."""
        # Load schemas
        multidim_schema_path = SCHEMAS_DIR / "multidim-schema.json"
        with open(multidim_schema_path) as f:
            multidim_schema = json.load(f)

        dataset_schema_path = SCHEMAS_DIR / "dataset-schema.json"
        with open(dataset_schema_path) as f:
            dataset_schema = json.load(f)

        grapher_schema = get_schema_from_url(DEFAULT_GRAPHER_SCHEMA)

        # Extract properties
        view_config_props = self.extract_view_config_properties(multidim_schema)
        metadata_props = self.extract_metadata_properties(dataset_schema)

        # First pass: collect all nested types by processing properties individually
        for prop_name, prop_schema in view_config_props.items():
            self.json_type_to_python_type(prop_schema, prop_name, grapher_schema)
        for prop_name, prop_schema in metadata_props.items():
            self.json_type_to_python_type(prop_schema, prop_name, grapher_schema)

        # Generate nested TypedDict classes from types discovered in the first pass.
        # Generating these can discover deeper nested types (e.g. DumbbellConfig -> TrendColorMapConfig);
        # those are not expanded into classes but get `dict[str, Any]` fallback aliases instead (see below).
        first_pass_nested = dict(self.nested_types)
        nested_classes = []
        for class_name, schema in first_pass_nested.items():
            nested_class = self.generate_typeddict(
                class_name,
                schema.get("properties", {}),
                required=schema.get("required", []),
                description=f"Nested configuration for {class_name}.",
                grapher_schema=grapher_schema,
            )
            nested_classes.append(nested_class)

        # Generate ViewConfig TypedDict
        if view_config_props:
            view_config_class = self.generate_typeddict(
                "ViewConfig",
                view_config_props,
                required=[],  # All config fields are optional in views
                description="View configuration options based on multidim schema.",
                grapher_schema=grapher_schema,
                extra_field_lines=VIEW_CONFIG_EXTRA_FIELDS,
            )
        else:
            view_config_class = "\n".join(
                [
                    "class ViewConfig(TypedDict, total=False):",
                    '    """View configuration options (fallback)."""',
                    "    pass",
                ]
            )

        # Generate ViewMetadata TypedDict
        if metadata_props:
            # Get required fields for metadata
            required_fields = (
                dataset_schema.get("properties", {})
                .get("tables", {})
                .get("additionalProperties", {})
                .get("properties", {})
                .get("variables", {})
                .get("additionalProperties", {})
                .get("required", [])
            )

            metadata_class = self.generate_typeddict(
                "ViewMetadata",
                metadata_props,
                required=required_fields,
                description="View metadata options based on dataset schema.",
            )
        else:
            metadata_class = "\n".join(
                [
                    "class ViewMetadata(TypedDict, total=False):",
                    '    """View metadata options (fallback)."""',
                    "    pass",
                ]
            )

        # Nested types discovered while generating the classes above are referenced by name
        # but never expanded into classes; alias them to dict[str, Any] so the names resolve.
        fallback_names = sorted(set(self.nested_types) - set(first_pass_nested))

        # Assemble file content
        lines = [
            '"""',
            "Generated TypedDict schemas for Collection model.",
            "",
            "This file is auto-generated from JSON schemas. Do not edit manually.",
            "Run `python scripts/generate_schema_types.py` to regenerate.",
            "",
            "Provides strongly-typed interfaces for:",
            "- View configuration (based on multidim-schema.json, resolving $refs against",
            f"  {DEFAULT_GRAPHER_SCHEMA})",
            "- View metadata (based on dataset-schema.json)",
            '"""',
            "",
            "from typing import Any, Literal, TypedDict",
        ]

        # Add conditional imports (excluding Literal since we already have it)
        if self.imports:
            additional_imports = [imp for imp in self.imports if "Literal" not in imp]
            lines.extend(sorted(additional_imports))

        if fallback_names:
            lines.extend(
                [
                    "",
                    "",
                    "# =============================================================================",
                    "# Fallback Types for Undefined Nested Objects",
                    "# =============================================================================",
                    "",
                    "# Fallback for nested types that reference other nested types not yet generated",
                ]
            )
            lines.extend(f"{name} = dict[str, Any]" for name in fallback_names)

        lines.extend(
            [
                "",
                "",
                "# =============================================================================",
                "# Nested Configuration Types",
                "# =============================================================================",
                "",
            ]
        )

        for nested_class in nested_classes:
            lines.append(nested_class)
            lines.extend(["", ""])

        lines.extend(
            [
                "# =============================================================================",
                "# View Configuration Types",
                "# =============================================================================",
                "",
                view_config_class,
                "",
                "",
                "# =============================================================================",
                "# View Metadata Types",
                "# =============================================================================",
                "",
                metadata_class,
                "",
                "",
                "# =============================================================================",
                "# Type Aliases for Method Parameters",
                "# =============================================================================",
                "",
                "# These provide type hints for methods that accept configuration/metadata",
                "ViewConfigParam = ViewConfig | dict[str, Any]",
                "ViewMetadataParam = ViewMetadata | dict[str, Any]",
                "",
            ]
        )

        return "\n".join(lines)


def format_content(content: str) -> str:
    """Run ruff format on the generated content so it matches repo style."""
    with tempfile.NamedTemporaryFile("w", suffix=".py", delete=False) as f:
        f.write(content)
        tmp_path = Path(f.name)
    try:
        subprocess.run(
            [sys.executable, "-m", "ruff", "check", "--select", "I", "--fix", "--quiet", str(tmp_path)],
            check=True,
        )
        subprocess.run(
            [sys.executable, "-m", "ruff", "format", "--quiet", str(tmp_path)],
            check=True,
        )
        return tmp_path.read_text()
    finally:
        tmp_path.unlink()


def main():
    """Generate the schema types file."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="Don't write; exit with code 1 if the generated content differs from the file on disk.",
    )
    args = parser.parse_args()

    generator = TypedDictGenerator()
    content = format_content(generator.generate_file_content())

    if args.check:
        existing = OUTPUT_PATH.read_text() if OUTPUT_PATH.exists() else ""
        if existing != content:
            diff = difflib.unified_diff(
                existing.splitlines(keepends=True),
                content.splitlines(keepends=True),
                fromfile=str(OUTPUT_PATH),
                tofile="generated",
            )
            sys.stdout.writelines(diff)
            print(f"\n{OUTPUT_PATH} is out of date. Run `python scripts/generate_schema_types.py` to update it.")
            sys.exit(1)
        print(f"{OUTPUT_PATH} is up to date.")
        return

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(content)

    print(f"Generated schema types in {OUTPUT_PATH}")
    print("Import in your code with:")
    print("from etl.collection.model.schema_types import ViewConfig, ViewMetadata")


if __name__ == "__main__":
    main()
