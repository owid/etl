"""
Shared utilities for converting OpenAPI specifications to markdown documentation.

This module provides reusable functions for converting OpenAPI YAML/JSON specs
into beautiful, interactive-looking markdown files compatible with Zensical/Material for MkDocs.
"""

import json
import re
import textwrap
from typing import Any, Dict, List


def format_type(schema: Dict[str, Any]) -> str:
    """Format schema type information."""
    if not schema:
        return "any"

    schema_type = schema.get("type", "any")

    # Handle arrays
    if schema_type == "array":
        items = schema.get("items", {})
        item_type = format_type(items)
        return f"array[{item_type}]"

    # Handle enums
    if "enum" in schema:
        enum_values = ", ".join(f"`{v}`" for v in schema["enum"])
        return f"{schema_type} ({enum_values})"

    # Handle format
    if "format" in schema:
        return f"{schema_type} ({schema['format']})"

    return schema_type


def format_constraints(schema: Dict[str, Any]) -> str:
    """Format schema constraints (min, max, default)."""
    constraints = []

    if "default" in schema:
        default_val = schema["default"]
        # Handle empty string default
        if default_val == "":
            constraints.append('default: `""`')
        else:
            constraints.append(f"default: `{default_val}`")
    if "minimum" in schema:
        constraints.append(f"min: {schema['minimum']}")
    if "maximum" in schema:
        constraints.append(f"max: {schema['maximum']}")
    if "minLength" in schema:
        constraints.append(f"minLength: {schema['minLength']}")
    if "maxLength" in schema:
        constraints.append(f"maxLength: {schema['maxLength']}")

    return " | ".join(constraints) if constraints else ""


def render_parameter_table(parameters: List[Dict[str, Any]]) -> str:
    """Render parameters as a markdown table."""
    if not parameters:
        return ""

    lines = ["| Parameter | Type | Required | Description |", "|-----------|------|----------|-------------|"]

    for param in parameters:
        name = param["name"]
        schema = param.get("schema", {})
        param_type = format_type(schema)
        required = "✓" if param.get("required", False) else ""
        description = param.get("description", "").replace("\n", " ")

        # Add constraints to description
        constraints = format_constraints(schema)
        if constraints:
            description = f"{description}<br/><small>{constraints}</small>"

        # Add example if available
        if "example" in param:
            example = param["example"]
            description += f"<br/><small>Example: `{example}`</small>"

        lines.append(f"| `{name}` | {param_type} | {required} | {description} |")

    return "\n".join(lines)


def render_json_example(example: Any, indent: int = 2) -> str:
    """Render JSON example with proper formatting."""
    return json.dumps(example, indent=indent, ensure_ascii=False)


def build_request_url(base_url: str, path: str, params: Dict[str, Any]) -> str:
    """Build a complete request URL from base URL, path, and parameters.

    Handles both path parameters (e.g., {slug}) and query parameters.
    """
    from urllib.parse import urlencode

    # Separate path parameters from query parameters
    path_params = {}
    query_params = {}

    # Find all path parameter placeholders in the path
    path_param_names = re.findall(r"\{(\w+)\}", path)

    for key, value in params.items():
        if key in path_param_names:
            path_params[key] = value
        else:
            query_params[key] = value

    # Substitute path parameters
    url_path = path
    for param_name, param_value in path_params.items():
        url_path = url_path.replace(f"{{{param_name}}}", str(param_value))

    url = f"{base_url}{url_path}"
    if query_params:
        url += "?" + urlencode(query_params)
    return url


def generate_code_samples(
    base_url: str, path: str, params: Dict[str, Any], content_type: str = "application/json"
) -> Dict[str, str]:
    """Generate code samples in multiple languages."""
    request_url = build_request_url(base_url, path, params)

    # Separate path and query parameters
    path_param_names = re.findall(r"\{(\w+)\}", path)
    path_params = {k: v for k, v in params.items() if k in path_param_names}
    query_params = {k: v for k, v in params.items() if k not in path_param_names}

    # Substitute path parameters in the path
    code_path = path
    for param_name, param_value in path_params.items():
        code_path = code_path.replace(f"{{{param_name}}}", str(param_value))

    samples = {}

    # Determine response handler based on content type
    is_text = content_type.startswith("text/")

    # HTTP/cURL
    samples["curl"] = f'curl "{request_url}"'

    # Python
    params_dict_str = json.dumps(query_params, indent=4) if query_params else "{}"
    if is_text:
        samples["python"] = f"""import requests

params = {params_dict_str}
response = requests.get("{base_url}{code_path}", params=params)
data = response.text"""
    else:
        samples["python"] = f"""import requests

params = {params_dict_str}
response = requests.get("{base_url}{code_path}", params=params)
data = response.json()"""

    # JavaScript/TypeScript
    params_obj = ", ".join(f'{k}: "{v}"' for k, v in query_params.items()) if query_params else ""
    response_handler = "text()" if is_text else "json()"
    if params_obj:
        samples["javascript"] = f"""const params = new URLSearchParams({{ {params_obj} }});
const response = await fetch(`{base_url}{code_path}?${{params}}`);
const data = await response.{response_handler};"""
    else:
        samples["javascript"] = f"""const response = await fetch("{base_url}{code_path}");
const data = await response.{response_handler};"""

    # Rust
    rust_response = "text()" if is_text else "json::<serde_json::Value>()"
    if query_params:
        rust_params = "\n".join(f'        .query(&[("{k}", "{v}")])' for k, v in query_params.items())
        samples["rust"] = f"""let response = reqwest::get("{base_url}{code_path}")
{rust_params}
    .await?
    .{rust_response}
    .await?;"""
    else:
        samples["rust"] = f"""let response = reqwest::get("{base_url}{code_path}")
    .await?
    .{rust_response}
    .await?;"""

    return samples


def render_schema_properties(schema: Dict[str, Any], components: Dict[str, Any]) -> str:
    """Render schema properties as a table."""
    properties = schema.get("properties", {})
    required_fields = schema.get("required", [])

    if not properties:
        return ""

    lines = ["| Property | Type | Required | Description |", "|----------|------|----------|-------------|"]

    for prop_name, prop_schema in properties.items():
        original_prop_schema = prop_schema

        # Check if this is a reference before resolving
        ref_name = None
        if "$ref" in prop_schema:
            ref_path = prop_schema["$ref"].split("/")
            if ref_path[0] == "#" and ref_path[1] == "components":
                ref_name = ref_path[-1]
                prop_schema = components["schemas"][ref_name]

        prop_type = format_type(prop_schema)

        # If it's an array with $ref items, create a link
        if original_prop_schema.get("type") == "array" and "items" in original_prop_schema:
            items = original_prop_schema["items"]
            if "$ref" in items:
                ref_path = items["$ref"].split("/")
                if ref_path[0] == "#" and ref_path[1] == "components":
                    item_ref_name = ref_path[-1]
                    # Create link to the schema
                    prop_type = f"array[[{item_ref_name}](#{item_ref_name.lower()})]"
        elif ref_name:
            # Direct reference (not array)
            prop_type = f"[{ref_name}](#{ref_name.lower()})"

        required = "✓" if prop_name in required_fields else ""
        description = prop_schema.get("description", "").replace("\n", " ")

        # Add example if available
        if "example" in prop_schema:
            example = prop_schema["example"]
            if isinstance(example, list):
                example_str = ", ".join(f"`{e}`" for e in example[:3])
                if len(example) > 3:
                    example_str += ", ..."
            else:
                example_str = f"`{example}`"
            description += f"<br/><small>Example: {example_str}</small>"

        lines.append(f"| `{prop_name}` | {prop_type} | {required} | {description} |")

    return "\n".join(lines)


def extract_params_from_example(example_data: Dict[str, Any]) -> Dict[str, Any]:
    """Extract request parameters from example data.

    First checks for x-request-params field, then falls back to inferring from response value.
    """
    # Check if x-request-params is specified
    if "x-request-params" in example_data:
        return example_data["x-request-params"]

    # Fall back to inferring from response value
    params = {}
    example_value = example_data.get("value", {})
    if "query" in example_value:
        params["q"] = example_value["query"]
    # Add other common params if they can be inferred
    return params


def render_example_content(
    example_data: Dict[str, Any],
    base_url: str,
    path: str,
    content_type: str,
    content_schema: Dict[str, Any],
    indent: str = "    ",
) -> List[str]:
    """Render the content for a single example (request URL, code samples, response).

    Args:
        example_data: The example data from OpenAPI spec
        base_url: Base URL for code samples
        path: API endpoint path
        content_type: Response content type
        content_schema: Schema for the content
        indent: Base indentation for the content

    Returns:
        List of lines for the example content
    """
    lines = []
    nested_indent = indent + "    "  # For nested code blocks in tabs

    # Add request URL
    example_params = extract_params_from_example(example_data)
    if base_url and example_params:
        request_url = build_request_url(base_url, path, example_params)
        lines.append(f"{indent}**Request:** `GET {request_url}`")
        lines.append("")

    # Add code samples
    if base_url:
        lines.append(f"{indent}**Code samples:**")
        lines.append("")
        code_samples = generate_code_samples(base_url, path, example_params, content_type)

        lines.append(f'{indent}=== "cURL"')
        lines.append("")
        lines.append(f"{nested_indent}```bash")
        lines.append(f"{nested_indent}{code_samples['curl']}")
        lines.append(f"{nested_indent}```")
        lines.append("")

        lines.append(f'{indent}=== "Python"')
        lines.append("")
        lines.append(f"{nested_indent}```python")
        for line in code_samples["python"].split("\n"):
            lines.append(f"{nested_indent}{line}")
        lines.append(f"{nested_indent}```")
        lines.append("")

        lines.append(f'{indent}=== "JavaScript"')
        lines.append("")
        lines.append(f"{nested_indent}```javascript")
        for line in code_samples["javascript"].split("\n"):
            lines.append(f"{nested_indent}{line}")
        lines.append(f"{nested_indent}```")
        lines.append("")

        lines.append(f'{indent}=== "Rust"')
        lines.append("")
        lines.append(f"{nested_indent}```rust")
        for line in code_samples["rust"].split("\n"):
            lines.append(f"{nested_indent}{line}")
        lines.append(f"{nested_indent}```")
        lines.append("")

    # Add schema link if available
    if "schema" in content_schema:
        schema = content_schema["schema"]
        if "$ref" in schema:
            ref_name = schema["$ref"].split("/")[-1]
            lines.append(f"{indent}**Response schema:** [`{ref_name}`](#{ref_name.lower()})")
            lines.append("")

    # Render response based on content type
    response_value = example_data.get("value")
    if content_type == "text/csv":
        lines.append(f"{indent}```csv")
        if isinstance(response_value, str):
            for line in response_value.split("\n"):
                if line:  # Skip empty lines
                    lines.append(f"{indent}{line}")
        lines.append(f"{indent}```")
    elif content_type == "text/markdown":
        lines.append(f"{indent}```markdown")
        if isinstance(response_value, str):
            for line in response_value.split("\n"):
                lines.append(f"{indent}{line}")
        lines.append(f"{indent}```")
    else:
        lines.append(f"{indent}```json")
        for line in render_json_example(response_value).split("\n"):
            lines.append(f"{indent}{line}")
        lines.append(f"{indent}```")
    lines.append("")

    return lines


def render_endpoint(
    path: str, method: str, operation: Dict[str, Any], components: Dict[str, Any], base_url: str = ""
) -> str:
    """Render a single API endpoint."""
    lines = []

    # Endpoint header with theme-aware colors
    method_upper = method.upper()

    # Use colors that work in both light and dark themes
    # These are carefully chosen to have good contrast in both modes
    method_colors = {
        "get": "#5e81ac",  # muted blue - works in both themes
        "post": "#10b981",  # green - works in both themes
        "put": "#f59e0b",  # amber - works in both themes
        "delete": "#ef4444",  # red - works in both themes
        "patch": "#a78bfa",  # lighter purple - works in both themes
    }
    color = method_colors.get(method, "#6b7280")
    method_badge = f'<span style="color: {color}; font-weight: bold;">{method_upper}</span>'

    lines.append(f"## {method_badge} `{path}`")
    lines.append("")

    # Summary
    if "summary" in operation:
        lines.append(f"**{operation['summary']}**")
        lines.append("")

    # Description in an info admonition
    if "description" in operation:
        lines.append(textwrap.dedent(operation["description"]).strip())
        lines.append("")

    # Parameters in a collapsible block
    if "parameters" in operation:
        lines.append("### Parameters")
        lines.append("")
        lines.append(render_parameter_table(operation["parameters"]))
        lines.append("")

    # Request Body
    if "requestBody" in operation:
        lines.append("#### Request Body")
        lines.append("")
        request_body = operation["requestBody"]
        if "description" in request_body:
            lines.append(request_body["description"])
            lines.append("")

        content = request_body.get("content", {})
        for content_type, content_schema in content.items():
            lines.append(f"**Content-Type:** `{content_type}`")
            lines.append("")

            if "schema" in content_schema:
                schema = content_schema["schema"]
                # Resolve $ref
                if "$ref" in schema:
                    ref_name = schema["$ref"].split("/")[-1]
                    lines.append(f"See [{ref_name}](#{ref_name.lower()}) schema.")
                lines.append("")

    # Responses with collapsible blocks
    if "responses" in operation:
        lines.append("### Responses")
        lines.append("")

        first_response = True
        for status_code, response in operation["responses"].items():
            status_emoji = "✅" if status_code.startswith("2") else "❌"
            description = response.get("description", "")

            # Use ???+ for first response (open by default), ??? for others
            collapsible = "???+" if first_response else "???"
            first_response = False

            # Use collapsible block for each response
            response_type = "success" if status_code.startswith("2") else "failure"
            lines.append(f'{collapsible} {response_type} "{status_emoji} {status_code} - {description}"')
            lines.append("")

            # Extract schema references to show response type
            schema_refs = []
            if "content" in response:
                content = response["content"]
                for content_type, content_schema in content.items():
                    if "schema" in content_schema:
                        schema = content_schema["schema"]
                        # Handle oneOf (multiple possible schemas)
                        if "oneOf" in schema:
                            for one_of_schema in schema["oneOf"]:
                                if "$ref" in one_of_schema:
                                    ref_name = one_of_schema["$ref"].split("/")[-1]
                                    schema_refs.append(ref_name)
                        # Handle direct $ref
                        elif "$ref" in schema:
                            ref_name = schema["$ref"].split("/")[-1]
                            schema_refs.append(ref_name)

            # Response content
            if "content" in response:
                content = response["content"]
                for content_type, content_schema in content.items():
                    lines.append(f"    **Content-Type:** `{content_type}`")
                    lines.append("")

                    # Show examples
                    if "examples" in content_schema:
                        examples = content_schema["examples"]
                        use_tabs = len(examples) > 1

                        for example_name, example_data in examples.items():
                            if use_tabs:
                                # Multiple examples: wrap in tabs with deeper indentation
                                tab_title = example_data.get("summary", example_name)
                                lines.append(f'    === "{tab_title}"')
                                lines.append("")
                                indent = "        "
                            else:
                                # Single example: just show the title
                                lines.append(f"    **Example: {example_data.get('summary', example_name)}**")
                                lines.append("")
                                indent = "    "

                            lines.extend(
                                render_example_content(
                                    example_data, base_url, path, content_type, content_schema, indent
                                )
                            )

                    # Show example (singular)
                    elif "example" in content_schema:
                        lines.append("    **Example:**")
                        lines.append("")
                        lines.append("    ```json")
                        for line in render_json_example(content_schema["example"]).split("\n"):
                            lines.append(f"    {line}")
                        lines.append("    ```")
                        lines.append("")

                    # If no examples but we have a schema reference, show it
                    elif "schema" in content_schema:
                        schema = content_schema["schema"]
                        if "$ref" in schema:
                            ref_name = schema["$ref"].split("/")[-1]
                            lines.append(f"    **Schema:** [`{ref_name}`](#{ref_name.lower()})")
                            lines.append("")

    lines.append("---")
    lines.append("")

    return "\n".join(lines)


def render_schema(schema_name: str, schema: Dict[str, Any], components: Dict[str, Any]) -> str:
    """Render a schema definition."""
    lines = []

    description = schema.get("description", "")

    # Add heading with anchor for linking (use code style for schema name)
    lines.append(f"### `{schema_name}` {{#{schema_name.lower()}}}")
    lines.append("")

    if description:
        lines.append(description)
        lines.append("")

    # Use collapsible block for the properties
    lines.append('??? info "Properties"')
    lines.append("")

    # Properties table (indented for collapsible block)
    properties_table = render_schema_properties(schema, components)
    if properties_table:
        for line in properties_table.split("\n"):
            lines.append(f"    {line}")
        lines.append("")

    return "\n".join(lines)


def generate_markdown(spec: Dict[str, Any], interactive_docs_url: str | None = None) -> str:
    """Generate complete markdown documentation from OpenAPI spec.

    Args:
        spec: OpenAPI specification dictionary
        interactive_docs_url: Optional URL to interactive API documentation (e.g., Swagger UI)
    """
    lines = []

    # Frontmatter for Zensical/MkDocs
    lines.append("---")
    lines.append("icon: material/api")
    lines.append("---")
    lines.append("")

    # Header
    info = spec.get("info", {})
    lines.append(f"# {info.get('title', 'API Documentation')}")
    lines.append("")

    if "description" in info:
        # Handle multi-line descriptions properly
        description = info["description"].strip()
        for line in description.split("\n"):
            lines.append(line)
        lines.append("")

    # Add interactive docs callout if URL provided
    if interactive_docs_url:
        lines.append('!!! tip "Try it out"')
        lines.append("")
        lines.append(f"    You can try out the API interactively at [{interactive_docs_url}]({interactive_docs_url})")
        lines.append("")

    # Key information in a single admonition
    lines.append('!!! info "API Information"')
    lines.append("")

    # Version
    if "version" in info:
        lines.append(f"    **Version:** `{info['version']}`")
        lines.append("")

    # Base URL
    if "servers" in spec:
        for server in spec["servers"]:
            url = server["url"]
            server_desc = server.get("description", "")
            if server_desc:
                lines.append(f"    **Base URL:** `{url}` — {server_desc}")
            else:
                lines.append(f"    **Base URL:** `{url}`")
            lines.append("")

    # License
    if "license" in info:
        license_info = info["license"]
        license_name = license_info.get("name", "")
        license_url = license_info.get("url", "")
        if license_url:
            lines.append(f"    **License:** [{license_name}]({license_url})")
        else:
            lines.append(f"    **License:** {license_name}")
        lines.append("")

    lines.append("---")
    lines.append("")

    # Get base URL for code samples
    base_url = ""
    if "servers" in spec and spec["servers"]:
        base_url = spec["servers"][0]["url"]

    # Endpoints (without header - direct to endpoint documentation)
    paths = spec.get("paths", {})
    components = spec.get("components", {})

    for path, path_item in paths.items():
        for method in ["get", "post", "put", "delete", "patch"]:
            if method in path_item:
                lines.append(render_endpoint(path, method, path_item[method], components, base_url))

    # Schemas
    if "schemas" in components:
        lines.append("## Schemas")
        lines.append("")

        for schema_name, schema in components["schemas"].items():
            lines.append(render_schema(schema_name, schema, components))

    return "\n".join(lines)
