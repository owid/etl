#!/usr/bin/env python
"""
Generate Zensical-friendly documentation from analyticsTypes.ts

This script parses the TypeScript analytics type definitions and generates
comprehensive markdown documentation for the Zensical documentation system.

All documentation content is extracted directly from the TypeScript file
to ensure documentation stays in sync with the source code.

The TypeScript file is fetched from the owid-grapher repository on GitHub.
"""

import re
from pathlib import Path
from typing import Dict, List

import click

from etl.git_api_helpers import GithubApiRepo


def load_typescript_from_github(
    org: str = "owid",
    repo: str = "owid-grapher",
    file_path: str = "packages/@ourworldindata/types/src/analyticsTypes/analyticsTypes.ts",
    branch: str = "master",
) -> str:
    """Load TypeScript file from GitHub repository.

    Args:
        org: GitHub organization name
        repo: Repository name
        file_path: Path to the TypeScript file in the repository
        branch: Branch to fetch from

    Returns:
        TypeScript file content as string
    """
    github_repo = GithubApiRepo(org=org, repo_name=repo)
    content = github_repo.fetch_file_content(file_path, branch)
    return content


def extract_file_header_comments(content: str) -> str:
    """Extract the header comment block from the TypeScript file."""
    lines = content.split("\n")
    header_lines = []
    in_header = False

    for line in lines:
        stripped = line.strip()

        # Start of header comments
        if stripped.startswith("//"):
            in_header = True
            # Remove the '//' and extra whitespace
            comment_text = line.lstrip("/ ").rstrip()
            if comment_text:
                header_lines.append(comment_text)
        # Stop at first non-comment line
        elif in_header and not stripped.startswith("//"):
            break

    return "\n".join(header_lines)


def parse_enum_values(content: str) -> Dict[str, str]:
    """Extract EventCategory enum values and their string representations."""
    enum_pattern = r"export enum EventCategory \{([^}]+)\}"
    match = re.search(enum_pattern, content, re.DOTALL)

    if not match:
        return {}

    enum_content = match.group(1)
    events = {}

    for line in enum_content.strip().split("\n"):
        line = line.strip()
        if not line or line.startswith("//"):
            continue

        # Parse "EventName = "event.name","
        pattern = r'(\w+)\s*=\s*"([^"]+)"'
        match = re.match(pattern, line.rstrip(","))
        if match:
            events[match.group(1)] = match.group(2)

    return events


def parse_interface(content: str, interface_name: str) -> tuple:
    """Parse an interface and extract its properties with comments.

    Returns:
        Tuple of (properties list, line number where interface is defined)
    """
    # Find the interface definition
    pattern = rf"export interface {interface_name} \{{([^}]+)\}}"
    match = re.search(pattern, content, re.DOTALL)

    if not match:
        return [], -1

    # Find line number by counting newlines before the match
    line_num = content[: match.start()].count("\n") + 1

    interface_content = match.group(1)
    properties = []

    lines = interface_content.strip().split("\n")
    current_comment = None

    for line in lines:
        line = line.strip()

        # Extract JSDoc comment
        if line.startswith("/**"):
            current_comment = line.replace("/**", "").replace("*/", "").strip()
        elif line.startswith("*") and not line.startswith("*/"):
            comment_text = line.lstrip("*").strip()
            if current_comment and comment_text:
                current_comment = comment_text

        # Parse property definition
        # Matches: propertyName: type or propertyName?: type
        prop_pattern = r"(\w+)(\??):\s*(.+?)(?:\s*//.*)?$"
        match = re.match(prop_pattern, line)

        if match:
            prop_name = match.group(1)
            optional = match.group(2) == "?"
            prop_type = match.group(3).strip()

            properties.append(
                {"name": prop_name, "type": prop_type, "optional": optional, "description": current_comment or ""}
            )
            current_comment = None

    return properties, line_num


def parse_section_comments(content: str) -> Dict[int, str]:
    """Parse section comments (e.g., '// Site Events') from the TypeScript file.

    Returns:
        Dictionary mapping line numbers to section titles
    """
    sections = {}
    lines = content.split("\n")

    for i, line in enumerate(lines, 1):
        stripped = line.strip()
        # Match comments like "// Site Events", "// Grapher Events", etc.
        match = re.match(r"^//\s+([A-Z][A-Za-z\s]+Events?)\s*$", stripped)
        if match:
            section_title = match.group(1).strip()
            sections[i] = section_title

    return sections


def get_section_for_interface(interface_line: int, sections: Dict[int, str]) -> str:
    """Find which section an interface belongs to based on line numbers."""
    # Find the most recent section comment before this interface
    section_lines = sorted([line for line in sections.keys() if line < interface_line])

    if section_lines:
        return sections[section_lines[-1]]

    return "Other Events"


def parse_type_definitions(content: str) -> Dict[str, List[str]]:
    """Parse type definitions for action types."""
    types = {}

    # Pattern to match: export type TypeName = ... until we hit a newline followed by non-indented content
    # This handles both single-line and multi-line type definitions
    type_pattern = r"export type (\w+) =\s*([^;]+?)(?=\n\nexport|\n\n//|\Z)"

    for match in re.finditer(type_pattern, content, re.DOTALL):
        type_name = match.group(1)
        type_values_str = match.group(2)

        # Extract string literals from the union type
        values = re.findall(r'"([^"]+)"', type_values_str)
        if values:
            types[type_name] = values

    return types


def parse_inline_comments(content: str) -> Dict[str, str]:
    """Parse inline comments for type values."""
    comments = {}

    # Pattern: | "value" // comment
    pattern = r'\|\s*"([^"]+)"\s*//\s*(.+?)(?:\n|$)'

    for match in re.finditer(pattern, content):
        value = match.group(1)
        comment = match.group(2).strip()
        comments[value] = comment

    return comments


def generate_event_section(event_name: str, params_interface: str, properties: List[Dict[str, str]]) -> str:
    """Generate markdown section for a single event."""
    md = f"### `{event_name}`\n\n"

    # Parameters table
    if properties:
        md += "| Parameter | Type | Required | Description |\n"
        md += "|-----------|------|----------|-------------|\n"

        for prop in properties:
            required = "✓" if not prop["optional"] else ""
            prop_type = prop["type"].replace("|", "\\|")  # Escape pipe in type unions
            description = prop["description"] or "—"
            md += f'| `{prop["name"]}` | `{prop_type}` | {required} | {description} |\n'

        md += "\n"

    # TypeScript interface reference as a note
    md += f'!!! note "TypeScript Interface: `{params_interface}`"\n\n'

    return md


def generate_introduction(header_comments: str) -> str:
    """Generate the documentation introduction from TypeScript file header."""
    # Extract guidelines from header comments
    guidelines_match = re.search(
        r"IMPORTANT GUIDELINES FOR ANALYTICS BACKEND:(.*?)(?=\n\n|\Z)", header_comments, re.DOTALL
    )

    # Frontmatter with tags and icon
    doc = "---\n"
    doc += "tags:\n"
    doc += "  - 👷 Staff\n"
    doc += "  - Reference\n"
    doc += "icon: material/google-analytics\n"
    doc += "---\n\n"

    doc += "# GA Events\n\n"
    doc += "This reference documents all custom Google Analytics (GA4) events tracked on Our World in Data's website and interactive visualizations.\n\n"

    doc += '!!! warning "Auto-generated documentation"\n'
    doc += "    This documentation is automatically generated from the TypeScript type definitions in [`analyticsTypes.ts`](https://github.com/owid/owid-grapher/blob/master/packages/%40ourworldindata/types/src/analyticsTypes/analyticsTypes.ts). We are working on improving the source of truth (see [:fontawesome-brands-github: #owid-grapher/5730](https://github.com/owid/owid-grapher/issues/5730)).\n\n"

    if guidelines_match:
        guidelines_text = guidelines_match.group(1).strip()
        # Parse the numbered guidelines
        guidelines = re.findall(r"\d+\.\s+([^:]+):\s*\n((?:\s+-.*\n?)+)", guidelines_text)

        if guidelines:
            doc += "## Important Guidelines\n\n"
            for title, points in guidelines:
                doc += f"### {title.strip()}\n\n"
                # Clean up the bullet points
                bullet_points = re.findall(r"-\s+(.+)", points)
                for point in bullet_points:
                    doc += f"- {point.strip()}\n"
                doc += "\n"

    return doc


def generate_type_sections(type_definitions: Dict[str, List[str]], inline_comments: Dict[str, str]) -> str:
    """Generate sections for type definitions found in the file."""
    doc = "\n## Event Action Types\n\n"
    doc += "The following type definitions are used across multiple events:\n\n"

    for type_name, values in sorted(type_definitions.items()):
        doc += f"### `{type_name}`\n\n"

        for value in values:
            comment = inline_comments.get(value, "")
            if comment:
                doc += f"- `{value}`: {comment}\n"
            else:
                doc += f"- `{value}`\n"

        doc += "\n"

    return doc


@click.command()
@click.option(
    "--output",
    "-o",
    "output_file",
    type=click.Path(path_type=Path),
    default=Path(__file__).parent.parent.parent / "guides" / "analytics-events.md",
    help="Output markdown file path",
)
@click.option(
    "--local-file",
    type=click.Path(exists=True, path_type=Path),
    default=None,
    help="Optional: Use local TypeScript file instead of fetching from GitHub (for testing)",
)
def main(output_file: Path, local_file: Path | None):
    """Generate Zensical-friendly documentation from analyticsTypes.ts"""

    if local_file:
        print(f"📖 Reading from local file: {local_file}...")
        content = local_file.read_text()
    else:
        print("📖 Fetching analyticsTypes.ts from GitHub (owid/owid-grapher)...")
        content = load_typescript_from_github()

    print("🔍 Parsing TypeScript file...")
    header_comments = extract_file_header_comments(content)
    events = parse_enum_values(content)
    sections = parse_section_comments(content)
    type_definitions = parse_type_definitions(content)
    inline_comments = parse_inline_comments(content)

    print("📝 Generating documentation...")
    doc = generate_introduction(header_comments)

    # Group events by section based on their interface location in the file
    events_by_section: Dict[str, List[tuple]] = {}

    for event_key, event_name in sorted(events.items()):
        params_interface = f"{event_key}Params"
        properties, line_num = parse_interface(content, params_interface)

        if line_num > 0:
            section = get_section_for_interface(line_num, sections)

            if section not in events_by_section:
                events_by_section[section] = []

            events_by_section[section].append((event_name, params_interface, properties))

    # Generate sections in the order they appear in the TypeScript file
    section_order = sorted(sections.items())

    for _, section_title in section_order:
        if section_title in events_by_section:
            doc += f"## {section_title}\n\n"
            for event_data in events_by_section[section_title]:
                doc += generate_event_section(*event_data)

    # Add type definitions if any were found
    if type_definitions:
        doc += generate_type_sections(type_definitions, inline_comments)

    # Create output directory if needed
    output_file.parent.mkdir(parents=True, exist_ok=True)

    print(f"💾 Writing documentation to {output_file}...")
    output_file.write_text(doc)

    print("✅ Documentation generated successfully!")
    print(f"   - {len(events)} events documented")
    print(f"   - {len(events_by_section)} sections")
    print(f"   - Output: {output_file}")


if __name__ == "__main__":
    main()
