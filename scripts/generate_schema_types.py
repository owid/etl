#!/usr/bin/env python3
"""
Generate TypedDict classes from JSON schemas for better typing in Collection model.

This script reads the multidim, grapher, and dataset schemas and generates
static TypedDict classes that provide autocompletion and type checking.

Usage:
    python scripts/generate_schema_types.py

This will update etl/collection/model/schema_types.py with the latest types.
"""

import json
from pathlib import Path
from typing import Any, Dict, List, Set

from etl.config import DEFAULT_GRAPHER_SCHEMA
from etl.files import get_schema_from_url
from etl.paths import SCHEMAS_DIR


class TypedDictGenerator:
    """Generate TypedDict classes from JSON schemas."""

    def __init__(self):
        self.generated_classes: Set[str] = set()
        self.imports: Set[str] = set()
        self.nested_types: Dict[str, Dict[str, Any]] = {}  # Maps class names to their schemas

    def json_type_to_python_type(
        self, json_schema: Dict[str, Any], property_name: str = "", grapher_schema: Dict[str, Any] = None
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
                        elif "type" in current:
                            return self.json_type_to_python_type(current, property_name, grapher_schema)
                except:
                    pass
            return "dict[str, Any]"  # Fallback for other refs

        json_type = json_schema.get("type")

        # Handle oneOf/anyOf
        if "oneOf" in json_schema:
            # Look for common patterns
            one_of = json_schema["oneOf"]
            if len(one_of) == 2:
                # Check if it's optional (null)
                types = []
                has_null = False
                for option in one_of:
                    if option.get("type") == "null":
                        has_null = True
                    else:
                        opt_type = self.json_type_to_python_type(option, property_name, grapher_schema)
                        if opt_type not in types:
                            types.append(opt_type)

                if has_null and len(types) == 1:
                    return f"{types[0]} | None"
                elif types:
                    return " | ".join(types)
            return "Any"  # Complex oneOf

        if "anyOf" in json_schema:
            return "Any"  # Simplified

        # Handle arrays of types like ["string", "null"]
        if isinstance(json_type, list):
            if len(json_type) == 2 and "null" in json_type:
                non_null_type = [t for t in json_type if t != "null"][0]
                return f"{self.json_type_to_python_type({'type': non_null_type}, property_name, grapher_schema)} | None"
            else:
                types = [self.json_type_to_python_type({"type": t}, property_name, grapher_schema) for t in json_type]
                return " | ".join(types)

        if json_type == "string":
            # Check for enums
            if "enum" in json_schema:
                self.imports.add("from typing import Literal")
                enum_values = [f'"{v}"' for v in json_schema["enum"]]
                return f"Literal[{', '.join(enum_values)}]"
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

    def sanitize_field_name(self, field_name: str) -> str:
        """Convert field name to valid Python identifier."""
        # Handle problematic cases but keep original names for TypedDict
        # TypedDict can handle most field names as string literals
        return field_name

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

    def generate_typeddict(
        self,
        class_name: str,
        properties: Dict[str, Any],
        required: List[str] = None,
        description: str = "",
        grapher_schema: Dict[str, Any] = None,
    ) -> str:
        """Generate a TypedDict definition."""
        if required is None:
            required = []

        lines = []

        # Class definition - use total=False since config/metadata fields are typically optional
        lines.append(f"class {class_name}(TypedDict, total=False):")

        # Docstring
        if description:
            lines.append(f'    """{description}"""')
        else:
            lines.append(f'    """Generated from JSON schema."""')

        if not properties:
            lines.append("    pass")
            return "\n".join(lines)

        lines.append("")

        # Check if we have any problematic field names
        has_problematic_fields = any(
            not field_name.isidentifier() or field_name.startswith("$") for field_name in properties.keys()
        )

        if has_problematic_fields:
            # Use __annotations__ approach for problematic field names
            lines.append("    # Fields defined via __annotations__ to handle special characters")
            lines.append("    pass")
            lines.append("")
            lines.append(f"{class_name}.__annotations__ = {{")

            for prop_name in sorted(properties.keys()):
                prop_schema = properties[prop_name]
                python_type = self.json_type_to_python_type(prop_schema, prop_name, grapher_schema)

                # Add comment if description exists
                if "description" in prop_schema:
                    description = prop_schema["description"]
                    # Handle multi-line descriptions
                    desc_lines = description.split("\n")
                    for desc_line in desc_lines:
                        if desc_line.strip():
                            lines.append(f"    # {desc_line.strip()}")

                lines.append(f'    "{prop_name}": {python_type},')

            lines.append("}")
        else:
            # All fields as optional (since total=False) - use normal syntax
            all_props = list(properties.keys())
            for prop_name in sorted(all_props):
                prop_schema = properties[prop_name]
                python_type = self.json_type_to_python_type(prop_schema, prop_name, grapher_schema)
                field_name = self.sanitize_field_name(prop_name)

                # Add comment if description exists
                if "description" in prop_schema:
                    description = prop_schema["description"]
                    # Handle multi-line descriptions
                    desc_lines = description.split("\n")
                    for desc_line in desc_lines:
                        if desc_line.strip():
                            lines.append(f"    # {desc_line.strip()}")

                lines.append(f"    {field_name}: {python_type}")

        return "\n".join(lines)

    def extract_view_config_properties(self, multidim_schema: Dict[str, Any]) -> Dict[str, Any]:
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

    def extract_metadata_properties(self, dataset_schema: Dict[str, Any]) -> Dict[str, Any]:
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

        try:
            grapher_schema = get_schema_from_url(DEFAULT_GRAPHER_SCHEMA)
        except Exception:
            grapher_schema = {}  # Fallback if can't fetch

        # Extract properties
        view_config_props = self.extract_view_config_properties(multidim_schema)
        metadata_props = self.extract_metadata_properties(dataset_schema)

        # First pass: collect all nested types by processing properties individually
        for prop_name, prop_schema in view_config_props.items():
            self.json_type_to_python_type(prop_schema, prop_name, grapher_schema)
        for prop_name, prop_schema in metadata_props.items():
            self.json_type_to_python_type(prop_schema, prop_name, grapher_schema)

        # Generate content
        lines = [
            '"""',
            "AUTOMATICALLY GENERATED SCRIPT",
            "================================",
            "",
            "Generated TypedDict schemas for Collection model.",
            "",
            "This file is auto-generated from JSON schemas. Do not edit manually.",
            "Run `python scripts/generate_schema_types.py` to regenerate.",
            "",
            "Provides strongly-typed interfaces for:",
            "- View configuration (based on multidim-schema.json)",
            "- View metadata (based on dataset-schema.json)",
            '"""',
            "",
            "from typing import Any, Literal, TypedDict",
        ]

        # Add conditional imports (excluding Literal since we already have it)
        if self.imports:
            additional_imports = [imp for imp in self.imports if "Literal" not in imp]
            lines.extend(sorted(additional_imports))

        lines.extend(
            [
                "",
                "",
                "# =============================================================================",
                "# Fallback Types for Undefined Nested Objects",
                "# =============================================================================",
                "",
                "# Fallback for nested types that reference other nested types not yet generated",
                "GlobeConfig = dict[str, Any]",
                "FaqsConfig = dict[str, Any]",
                "GrapherConfig = dict[str, Any]",
                "",
                "",
                "# =============================================================================",
                "# Nested Configuration Types",
                "# =============================================================================",
                "",
            ]
        )

        # Generate nested TypedDict classes first
        nested_classes = []
        nested_types_copy = dict(self.nested_types)  # Make a copy to avoid modification during iteration
        for class_name, schema in nested_types_copy.items():
            nested_class = self.generate_typeddict(
                class_name,
                schema.get("properties", {}),
                required=schema.get("required", []),
                description=f"Nested configuration for {class_name}.",
                grapher_schema=grapher_schema,
            )
            nested_classes.append(nested_class)

        if nested_classes:
            lines.extend(nested_classes)
            lines.extend(["", ""])

        lines.extend(
            [
                "# =============================================================================",
                "# View Configuration Types",
                "# =============================================================================",
                "",
            ]
        )

        # Generate ViewConfig TypedDict
        if view_config_props:
            view_config_class = self.generate_typeddict(
                "ViewConfig",
                view_config_props,
                required=[],  # All config fields are optional in views
                description="View configuration options based on multidim schema.",
                grapher_schema=grapher_schema,
            )
            lines.append(view_config_class)
        else:
            lines.extend(
                [
                    "class ViewConfig(TypedDict, total=False):",
                    '    """View configuration options (fallback)."""',
                    "    pass",
                ]
            )

        lines.extend(
            [
                "",
                "",
                "# =============================================================================",
                "# View Metadata Types",
                "# =============================================================================",
                "",
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
            lines.append(metadata_class)
        else:
            lines.extend(
                [
                    "class ViewMetadata(TypedDict, total=False):",
                    '    """View metadata options (fallback)."""',
                    "    pass",
                ]
            )

        lines.extend(
            [
                "",
                "",
                "# =============================================================================",
                "# Type Aliases for Method Parameters",
                "# =============================================================================",
                "",
                "# These provide type hints for methods that accept configuration/metadata",
                "ViewConfigParam = ViewConfig | dict[str, Any]",
                "ViewMetadataParam = ViewMetadata | dict[str, Any]",
            ]
        )

        return "\n".join(lines)


def main():
    """Generate the schema types file."""
    generator = TypedDictGenerator()
    content = generator.generate_file_content()

    # Write to the schema types file
    output_path = Path(__file__).parent.parent / "etl" / "collection" / "model" / "schema_types.py"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w") as f:
        f.write(content)

    print(f"Generated schema types in {output_path}")
    print("Import in your code with:")
    print("from etl.collection.model.schema_types import ViewConfig, ViewMetadata")


if __name__ == "__main__":
    main()
