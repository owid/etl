#!/usr/bin/env python3
"""
NOTE: THIS SCRIPT IS CALLED FROM `make docs.post`

Convert Jupyter notebooks to HTML wrapped in full Zensical theme.

This script finds all .ipynb files in the docs directory and converts them
to HTML using nbconvert, then wraps them in the complete Zensical template
with navigation, TOC, and search functionality.
"""

import re
from pathlib import Path
from typing import cast

import click
import nbformat
from bs4 import BeautifulSoup, Tag
from nbconvert import HTMLExporter


@click.command()
@click.option(
    "--docs-dir",
    type=click.Path(exists=True, path_type=Path),
    default=Path("docs"),
    help="Path to the docs directory (source)",
)
@click.option(
    "--output-dir",
    type=click.Path(path_type=Path),
    default=Path("site"),
    help="Path to the output directory (site)",
)
@click.option(
    "--verbose",
    is_flag=True,
    help="Print detailed information about conversions",
)
def convert_notebooks(docs_dir: Path, output_dir: Path, verbose: bool):
    """Convert all Jupyter notebooks from docs directory to HTML in site directory."""
    # Check if output directory exists
    if not output_dir.exists():
        click.echo(f"Error: Output directory '{output_dir}' does not exist.", err=True)
        click.echo("Please run this after building docs (e.g., after 'make docs.build')", err=True)
        return 1

    # Find all notebook files
    notebooks = list(docs_dir.rglob("*.ipynb"))

    if not notebooks:
        click.echo("No Jupyter notebooks found in docs directory")
        return

    # Use basic template to get just the content without full HTML wrapper
    html_exporter = HTMLExporter(template_name="basic")

    # Load a Zensical template page
    template_path = output_dir / "index.html"
    if not template_path.exists():
        click.echo(f"Error: Template page not found at {template_path}", err=True)
        return 1

    with open(template_path, "r", encoding="utf-8") as f:
        zensical_template = f.read()

    converted_count = 0
    skipped_count = 0

    for notebook_path in notebooks:
        try:
            # Skip checkpoint files
            if ".ipynb_checkpoints" in str(notebook_path):
                if verbose:
                    click.echo(f"Skipping checkpoint: {notebook_path}")
                skipped_count += 1
                continue

            # Read the notebook
            with open(notebook_path, "r", encoding="utf-8") as f:
                nb = nbformat.read(f, as_version=4)

            # Convert to HTML
            (body, _resources) = html_exporter.from_notebook_node(nb)

            # Calculate relative path from docs_dir
            relative_path = notebook_path.relative_to(docs_dir)

            # Create corresponding path in output_dir
            html_path = output_dir / relative_path.with_suffix(".html")

            # Create parent directories if they don't exist
            html_path.parent.mkdir(parents=True, exist_ok=True)

            # Get notebook title from metadata or filename
            notebook_title = nb.metadata.get("title", relative_path.stem.replace("_", " ").title())

            # Calculate depth for relative paths to assets
            depth = len(relative_path.parts) - 1
            relative_root = "../" * depth if depth > 0 else "./"

            # Extract headings for TOC and add IDs to headings in HTML
            body_with_ids, headings = extract_headings_from_html(body)

            # Wrap the notebook HTML in full Zensical structure
            wrapped_html = wrap_in_full_zensical_template(
                zensical_template,
                body_with_ids,
                title=notebook_title,
                site_name="OWID's Technical Documentation",
                relative_root=relative_root,
                headings=headings,
            )

            # Write HTML file
            with open(html_path, "w", encoding="utf-8") as f:
                f.write(wrapped_html)

            converted_count += 1
            if verbose:
                click.echo(f"Converted: {notebook_path} -> {html_path}")
            else:
                click.echo(f"Converted: {relative_path}")

        except Exception as e:
            click.echo(f"Error converting {notebook_path}: {e}", err=True)
            continue

    # Summary
    click.echo(f"\n✓ Converted {converted_count} notebook(s)")
    if skipped_count > 0:
        click.echo(f"  Skipped {skipped_count} checkpoint file(s)")


def extract_headings_from_html(html: str) -> tuple[str, list[dict]]:
    """Extract headings from HTML for TOC generation and add IDs to headings.

    Returns:
        tuple: (modified_html_with_ids, list_of_headings)
    """
    soup = BeautifulSoup(html, "html.parser")
    headings = []

    for heading_elem in soup.find_all(["h1", "h2", "h3"]):
        # Cast to Tag type for proper type checking
        heading = cast(Tag, heading_elem)

        # Get heading text
        text = heading.get_text(strip=True)
        # Remove anchor link symbols
        text = text.replace("¶", "").strip()

        # Create ID from text (preserving case for better readability)
        heading_id = re.sub(r"[^\w\s-]", "", text)
        heading_id = re.sub(r"[-\s]+", "-", heading_id)
        heading_id = heading_id.strip("-")

        # Add ID to the heading element
        heading.attrs["id"] = heading_id

        # Get heading level (heading.name is guaranteed to be h1/h2/h3 from find_all)
        heading_name = heading.name
        assert heading_name is not None, "Heading name should not be None"
        level = int(heading_name[1])

        headings.append({"text": text, "id": heading_id, "level": level})

    return str(soup), headings


def wrap_in_full_zensical_template(
    template: str, notebook_html: str, title: str, site_name: str, relative_root: str, headings: list[dict]
) -> str:
    """Wrap notebook HTML in full Zensical template with navigation and TOC."""

    # Replace title
    template = re.sub(r"<title>([^<]*)</title>", f"<title>{title} - {site_name}</title>", template, count=1)

    # Update header title to show notebook name
    # Use a function to avoid regex escape issues with notebook HTML
    def replace_header(match):
        return match.group(1) + title + match.group(3)

    template = re.sub(
        r'(<div class="md-header__topic" data-md-component="header-topic">.*?<span class="md-ellipsis">)(.*?)(</span>)',
        replace_header,
        template,
        flags=re.DOTALL,
    )

    # Find and replace the main content
    # Look for the article tag and replace its content
    content_pattern = r'(<article class="md-content__inner md-typeset">)(.*?)(</article>)'

    notebook_css = """
<style>
/* Notebook-specific styling */
.cell { margin: 1.5rem 0; }
.text_cell_render { line-height: 1.6; }
.text_cell_render h1, .text_cell_render h2, .text_cell_render h3 { margin-top: 1.5em; margin-bottom: 0.5em; }
.text_cell_render p { margin: 0.5em 0; }
.code_cell {
  background-color: transparent;
}
.input_area {
  background-color: var(--md-code-bg-color, #f5f5f5);
  border: 1px solid #dee2e6;
  border-radius: 0.5rem;
  padding: 1rem;
  margin: 0.5rem 0;
  overflow-x: auto;
  word-wrap: break-word;
  overflow-wrap: break-word;
}
.input_prompt, .prompt { display: none; }
.highlight pre { margin: 0; padding: 0; background-color: transparent !important; white-space: pre-wrap; word-wrap: break-word; overflow-wrap: break-word; }
.output_area { padding: 0.5rem 0; }
.output_subarea { max-width: 100%; overflow-x: auto; }
.output_png img { max-width: 100%; height: auto; }
.anchor-link { display: none !important; }
.notebook-content pre, .notebook-content code { white-space: pre-wrap; word-wrap: break-word; overflow-wrap: break-word; }

/* DataFrame styling - inspired by pandas default style */
.dataframe {
  border-collapse: collapse;
  border: none;
  font-size: 12px;
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
  margin: 1em 0;
  background-color: white;
}

.dataframe thead {
  background-color: #f8f9fa;
  border-bottom: 2px solid #dee2e6;
}

.dataframe thead th {
  text-align: right;
  font-weight: 600;
  padding: 0.5em 0.75em;
  border: none;
  border-bottom: 2px solid #dee2e6;
  color: #495057;
}

.dataframe tbody tr {
  border-bottom: 1px solid #dee2e6;
}

.dataframe tbody tr:hover {
  background-color: #f8f9fa;
}

.dataframe tbody tr:last-child {
  border-bottom: 2px solid #dee2e6;
}

.dataframe tbody td {
  padding: 0.5em 0.75em;
  text-align: right;
  border: none;
  color: #212529;
}

.dataframe tbody th {
  font-weight: 600;
  text-align: right;
  padding: 0.5em 0.75em;
  border: none;
  background-color: #f8f9fa;
  color: #495057;
}

/* Index column styling */
.dataframe tbody tr th:first-child {
  text-align: right;
  font-weight: 500;
}

/* Handle left-aligned text columns */
.dataframe td[style*="text-align: left"],
.dataframe th[style*="text-align: left"] {
  text-align: left !important;
}

/* Scrollable table container */
.dataframe-container {
  overflow-x: auto;
  margin: 1em 0;
}

/* General table styling fallback */
.rendered_html table:not(.dataframe) {
  border-collapse: collapse;
  margin: 1em 0;
  font-size: 0.9em;
}

.rendered_html table:not(.dataframe) th,
.rendered_html table:not(.dataframe) td {
  border: 1px solid #ddd;
  padding: 0.5em;
}

.rendered_html table:not(.dataframe) th {
  background-color: var(--md-code-bg-color, #f5f5f5);
  font-weight: 600;
}

/* Dark mode support for input area */
[data-md-color-scheme="slate"] .input_area {
  border-color: #404040;
}

/* Dark mode support for dataframes */
[data-md-color-scheme="slate"] .dataframe {
  background-color: #1e1e1e;
}

[data-md-color-scheme="slate"] .dataframe thead {
  background-color: #2d2d2d;
  border-bottom-color: #404040;
}

[data-md-color-scheme="slate"] .dataframe thead th {
  border-bottom-color: #404040;
  color: #e0e0e0;
}

[data-md-color-scheme="slate"] .dataframe tbody tr {
  border-bottom-color: #404040;
}

[data-md-color-scheme="slate"] .dataframe tbody tr:hover {
  background-color: #2d2d2d;
}

[data-md-color-scheme="slate"] .dataframe tbody tr:last-child {
  border-bottom-color: #404040;
}

[data-md-color-scheme="slate"] .dataframe tbody td {
  color: #e0e0e0;
}

[data-md-color-scheme="slate"] .dataframe tbody th {
  background-color: #2d2d2d;
  color: #e0e0e0;
}

/* Copy button styling for notebook code cells */
.input_area .md-clipboard {
  z-index: 1;
  width: 1.5rem;
  height: 1.5rem;
  color: var(--md-default-fg-color--light);
}

.input_area .md-clipboard svg {
  width: 100%;
  height: 100%;
  fill: currentColor;
}

[data-md-color-scheme="slate"] .input_area .md-clipboard {
  color: var(--md-default-fg-color--lighter);
}
</style>
"""

    # Check if notebook already has an h1 title
    has_h1 = bool(re.search(r"<h1[^>]*>", notebook_html))

    # Only add title if notebook doesn't have one
    if has_h1:
        new_content = f"""
<div class="notebook-content">
{notebook_html}
</div>
"""
    else:
        new_content = f"""
<h1>{title}</h1>
<div class="notebook-content">
{notebook_html}
</div>
"""

    # Use a function to avoid regex escape issues with notebook HTML
    def replace_content(match):
        return match.group(1) + new_content + match.group(3)

    template = re.sub(content_pattern, replace_content, template, flags=re.DOTALL)

    # Add notebook CSS before </head>
    template = template.replace("</head>", notebook_css + "</head>")

    # Generate TOC and replace the secondary sidebar
    if headings:
        toc_html = generate_toc_html(headings)
        # Find and replace the TOC sidebar
        toc_pattern = r'(<div class="md-sidebar md-sidebar--secondary".*?>.*?<div class="md-sidebar__inner">)(.*?)(</div>\s*</div>\s*</div>)'

        def replace_toc(match):
            return match.group(1) + toc_html + match.group(3)

        template = re.sub(toc_pattern, replace_toc, template, flags=re.DOTALL)

    # Fix relative paths for assets
    if relative_root != "./":
        template = template.replace('href="assets/', f'href="{relative_root}assets/')
        template = template.replace('src="assets/', f'src="{relative_root}assets/')
        template = template.replace('href="css/', f'href="{relative_root}css/')
        template = template.replace('href="javascripts/', f'href="{relative_root}javascripts/')
        template = template.replace('src="javascripts/', f'src="{relative_root}javascripts/')

        # Fix __config base path for JavaScript bundle
        # Remove trailing slash from relative_root for JSON
        base_path = relative_root.rstrip("/")
        template = template.replace('"base":"."', f'"base":"{base_path}"')
        # Also fix search worker path in config
        template = template.replace('"search":"assets/javascripts/', f'"search":"{relative_root}assets/javascripts/')

    # Fix navigation links to be absolute from root
    # Convert relative navigation links like href="guides/" to href="../../guides/"
    if relative_root != "./":
        # Use regex to fix navigation links without parsing entire HTML
        # This preserves the original HTML structure and avoids BeautifulSoup's reformatting
        def fix_nav_link(match):
            href = match.group(1)
            # Skip external links, anchors, and asset paths
            if (
                href.startswith("http")
                or href.startswith("#")
                or href.startswith(relative_root)
                or href.startswith("assets/")
                or href.startswith("css/")
                or href.startswith("javascripts/")
                or href.startswith("../")
            ):
                return match.group(0)
            # Add relative_root prefix to navigation links
            return f'href="{relative_root}{href}"'

        # Fix all href attributes
        template = re.sub(r'href="([^"]+)"', fix_nav_link, template)

    return template


def generate_toc_html(headings: list[dict]) -> str:
    """Generate TOC HTML from headings list with proper nesting."""
    if not headings:
        return "<p>No table of contents</p>"

    toc_html = '<nav class="md-nav md-nav--secondary" aria-label="Table of contents">'
    toc_html += '<label class="md-nav__title" for="__toc">Table of contents</label>'
    toc_html += '<ul class="md-nav__list" data-md-component="toc" data-md-scrollfix>'

    # Track current nesting state
    current_level = 1
    open_navs = []

    for i, heading in enumerate(headings):
        level = heading["level"]

        # Close nested navs if we're going back to a higher level (lower number)
        while current_level > level:
            toc_html += "</ul></nav>"
            current_level -= 1
            if open_navs:
                open_navs.pop()

        # For h2 (level 2)
        if level == 2:
            toc_html += '<li class="md-nav__item">'
            toc_html += f'<a href="#{heading["id"]}" class="md-nav__link">'
            toc_html += f'<span class="md-ellipsis">{heading["text"]}</span>'
            toc_html += "</a>"

            # Check if next heading is h3 (needs nesting)
            if i + 1 < len(headings) and headings[i + 1]["level"] == 3:
                toc_html += f'<nav class="md-nav" aria-label="{heading["text"]}">'
                toc_html += '<ul class="md-nav__list">'
                current_level = 3
                open_navs.append(heading["text"])
            else:
                toc_html += "</li>"

        # For h3 (level 3)
        elif level == 3:
            toc_html += '<li class="md-nav__item">'
            toc_html += f'<a href="#{heading["id"]}" class="md-nav__link">'
            toc_html += f'<span class="md-ellipsis">{heading["text"]}</span>'
            toc_html += "</a>"
            toc_html += "</li>"

        # For h1 (level 1) - treat like h2
        else:
            toc_html += '<li class="md-nav__item">'
            toc_html += f'<a href="#{heading["id"]}" class="md-nav__link">'
            toc_html += f'<span class="md-ellipsis">{heading["text"]}</span>'
            toc_html += "</a>"
            toc_html += "</li>"

    # Close any remaining open nested navs
    while current_level > 2:
        toc_html += "</ul></nav></li>"
        current_level -= 1

    toc_html += "</ul></nav>"
    return toc_html


if __name__ == "__main__":
    convert_notebooks()
