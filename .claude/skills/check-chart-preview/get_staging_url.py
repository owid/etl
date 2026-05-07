#!/usr/bin/env python
"""Print the staging URL for a chart, mdim, or step file.

Usage:
    .venv/bin/python .claude/skills/check-chart-preview/get_staging_url.py <slug-or-path>

Accepts:
    - Chart slug:           population-density, life-expectancy
    - Mdim slug:            energy/latest/energy_prices#energy_prices
    - Export multidim path: etl/steps/export/multidim/energy/latest/energy_prices.config.yml
    - Chart file path:      etl/steps/graph/covid/latest/covid-cases.chart.yml

Examples:
    .venv/bin/python .claude/skills/check-chart-preview/get_staging_url.py life-expectancy
    .venv/bin/python .claude/skills/check-chart-preview/get_staging_url.py energy/latest/energy_prices#energy_prices
    .venv/bin/python .claude/skills/check-chart-preview/get_staging_url.py etl/steps/export/multidim/energy/latest/energy_prices.config.yml
"""

import re
import subprocess
import sys
from pathlib import Path
from urllib.parse import quote


def get_git_branch() -> str:
    result = subprocess.run(["git", "branch", "--show-current"], capture_output=True, text=True, check=True)
    return result.stdout.strip()


def get_container_name(branch: str) -> str:
    """Port of etl.config.get_container_name."""
    normalized = re.sub(r"[/._]", "-", branch)
    normalized = normalized.replace("staging-site-", "")
    container = f"staging-site-{normalized[:28]}"
    return container.rstrip("-")


def parse_export_multidim(file_path: str) -> str:
    """Parse export/multidim path → staging URL path."""
    m = re.search(r"export/multidim/(.+?)\.(?:config\.yml|py)$", file_path)
    if not m:
        raise ValueError(f"Cannot parse export multidim path: {file_path}")
    step_path = m.group(1)
    short_name = step_path.split("/")[-1]
    catalog_path = f"{step_path}#{short_name}"
    return f"/admin/grapher/{quote(catalog_path, safe='/')}"


def parse_chart_yml(file_path: str) -> str:
    """Parse .chart.yml → staging URL path."""
    content = Path(file_path).read_text()
    match = re.search(r"^slug:\s*(.+)$", content, re.MULTILINE)
    if not match:
        slug = Path(file_path).stem.replace(".chart", "")
        return f"/grapher/{slug}"

    slug = match.group(1).strip()

    if "#" not in slug:
        return f"/grapher/{slug}"

    # Mdim: need to insert version from file path
    m = re.search(r"graph/(.+?)\.chart\.yml$", file_path)
    if m:
        parts = m.group(1).split("/")
        if len(parts) >= 2:
            version = parts[1]
            slash_idx = slug.index("/")
            namespace = slug[:slash_idx]
            rest = slug[slash_idx + 1 :]
            catalog_path = f"{namespace}/{version}/{rest}"
            return f"/admin/grapher/{quote(catalog_path, safe='/')}"

    return f"/admin/grapher/{quote(slug, safe='/')}"


def parse_slug(slug: str) -> str:
    """Parse a plain slug or mdim slug → staging URL path."""
    if "#" in slug:
        # Mdim slug like energy/latest/energy_prices#energy_prices
        return f"/admin/grapher/{quote(slug, safe='/')}"
    else:
        # Simple chart slug like population-density
        return f"/grapher/{slug}"


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: get_staging_url.py <slug-or-path>", file=sys.stderr)
        sys.exit(1)

    arg = sys.argv[1]
    container = get_container_name(get_git_branch())

    if "export/multidim/" in arg:
        url_path = parse_export_multidim(arg)
    elif arg.endswith(".chart.yml"):
        url_path = parse_chart_yml(arg)
    else:
        # Treat as a slug (simple or mdim)
        url_path = parse_slug(arg)

    print(f"http://{container}{url_path}")


if __name__ == "__main__":
    main()
