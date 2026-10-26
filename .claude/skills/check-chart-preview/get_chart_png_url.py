#!/usr/bin/env python
"""Print the staging PNG URL for a chart (works for drafts too).

For published charts, `/grapher/<slug>.png` is enough — but it 404s for drafts.
Drafts need the `/grapher/by-uuid/<chart_configs.id>.png` route, which reads
the config from R2's `byUUID/` directory (which has *every* chart, published or
not, because the admin chart-save path writes to it).

This script resolves a slug (or chart_id) to the chart_configs.id UUID by
querying the staging MySQL, then prints the by-uuid PNG URL. The returned URL
works whether the chart is published or a draft.

Usage:
    .venv/bin/python .claude/skills/check-chart-preview/get_chart_png_url.py <slug-or-id>
    .venv/bin/python .claude/skills/check-chart-preview/get_chart_png_url.py <slug-or-id> --tab=chart
    .venv/bin/python .claude/skills/check-chart-preview/get_chart_png_url.py <slug-or-id> --tab=map --time=earliest..latest

Examples:
    .venv/bin/python .claude/skills/check-chart-preview/get_chart_png_url.py hens-by-housing-system
    .venv/bin/python .claude/skills/check-chart-preview/get_chart_png_url.py 9139
"""

import re
import subprocess
import sys
from urllib.parse import urlencode


def get_git_branch() -> str:
    result = subprocess.run(["git", "branch", "--show-current"], capture_output=True, text=True, check=True)
    return result.stdout.strip()


def get_container_name(branch: str) -> str:
    """Port of etl.config.get_container_name."""
    normalized = re.sub(r"[/._]", "-", branch)
    normalized = normalized.replace("staging-site-", "")
    container = f"staging-site-{normalized[:28]}"
    return container.rstrip("-")


def resolve_uuid(slug_or_id: str) -> str:
    """Resolve a chart slug or numeric id to the chart_configs.id UUID on staging."""
    from etl.config import OWID_ENV

    if slug_or_id.isdigit():
        sql = "SELECT cc.id FROM charts c JOIN chart_configs cc ON cc.id = c.configId WHERE c.id = %(key)s"
    else:
        sql = "SELECT cc.id FROM charts c JOIN chart_configs cc ON cc.id = c.configId WHERE cc.slug = %(key)s"

    df = OWID_ENV.read_sql(sql, params={"key": slug_or_id})
    if df.empty:
        raise SystemExit(f"No chart found for '{slug_or_id}' on the staging DB.")
    return df.iloc[0, 0]


def main() -> None:
    if len(sys.argv) < 2:
        print(__doc__, file=sys.stderr)
        sys.exit(1)

    slug_or_id = sys.argv[1]

    # Parse optional --key=value query-param flags (e.g. --tab=chart, --time=2020).
    extra_params: dict[str, str] = {"nocache": "1"}
    for arg in sys.argv[2:]:
        if arg.startswith("--") and "=" in arg:
            k, v = arg[2:].split("=", 1)
            extra_params[k] = v

    container = get_container_name(get_git_branch())
    uuid = resolve_uuid(slug_or_id)
    qs = urlencode(extra_params)
    print(f"http://{container}/grapher/by-uuid/{uuid}.png?{qs}")


if __name__ == "__main__":
    main()
