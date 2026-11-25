#!/usr/bin/env python
"""Print the staging URL for a chart, mdim, or viz chart step.

Usage:
    .venv/bin/python .claude/skills/check-chart-preview/get_staging_url.py <slug-or-path>

Accepts:
    - Chart slug:           population-density, life-expectancy
    - Mdim slug:            energy/latest/energy_prices#energy_prices
    - Viz chart path:       etl/steps/viz/chart/energy/latest/energy_prices.config.yml

Examples:
    .venv/bin/python .claude/skills/check-chart-preview/get_staging_url.py life-expectancy
    .venv/bin/python .claude/skills/check-chart-preview/get_staging_url.py energy/latest/energy_prices#energy_prices
    .venv/bin/python .claude/skills/check-chart-preview/get_staging_url.py etl/steps/viz/chart/energy/latest/energy_prices.config.yml
"""

import re
import subprocess
import sys
from urllib.parse import quote

from etl.config import STAGING, get_container_name


def get_git_branch() -> str:
    result = subprocess.run(["git", "branch", "--show-current"], capture_output=True, text=True, check=True)
    return result.stdout.strip()


def parse_viz_chart(file_path: str) -> str:
    """Parse viz/chart step path → staging URL path."""
    m = re.search(r"viz/chart/(.+?)\.(?:config\.yml|py)$", file_path)
    if not m:
        raise ValueError(f"Cannot parse viz chart path: {file_path}")
    step_path = m.group(1)
    short_name = step_path.split("/")[-1]
    catalog_path = f"{step_path}#{short_name}"
    return f"/admin/grapher/{quote(catalog_path, safe='/')}"


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
    container = get_container_name(STAGING or get_git_branch())

    if "viz/chart/" in arg:
        url_path = parse_viz_chart(arg)
    else:
        # Treat as a slug (simple or mdim)
        url_path = parse_slug(arg)

    print(f"http://{container}{url_path}")


if __name__ == "__main__":
    main()
