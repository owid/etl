#!/usr/bin/env python3
"""
Convert Jupyter notebooks to HTML with Zensical theme styling.

This script finds all .ipynb files in the docs directory and converts them
to HTML using a custom template that matches the Zensical theme.
"""

from pathlib import Path

import click
import nbformat
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

    # Use classic template for cleaner output
    html_exporter = HTMLExporter(template_name='classic')

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

            # Write HTML file
            with open(html_path, "w", encoding="utf-8") as f:
                f.write(body)

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


if __name__ == "__main__":
    convert_notebooks()
