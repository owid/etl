#
#  owid.catalog.api.popularity
#
#  Popularity data fetching from datasette.
#
from __future__ import annotations

from typing import Literal

import requests

DATASETTE_BASE_URL = "https://datasette-public.owid.io/owid.json"


def fetch_popularity(
    slugs: list[str],
    type: Literal["indicator", "dataset"],
    timeout: int = 10,
) -> dict[str, float]:
    """Fetch popularity scores from datasette for given slugs.

    Args:
        slugs: List of slugs to fetch popularity for.
        type: Type of popularity to fetch ("indicator" or "dataset").
        timeout: HTTP request timeout in seconds.

    Returns:
        Dict mapping slug to popularity score (0.0 to 1.0).
        Missing slugs will not be in the dict.
    """
    if not slugs:
        return {}

    # Build SQL query with IN clause
    # Escape single quotes in slugs
    escaped_slugs = [s.replace("'", "''") for s in slugs]
    slugs_str = ", ".join(f"'{s}'" for s in escaped_slugs)

    sql = f"""
    SELECT slug, popularity
    FROM analytics_popularity
    WHERE type = '{type}' AND slug IN ({slugs_str})
    """

    try:
        resp = requests.get(
            DATASETTE_BASE_URL,
            params={"sql": sql, "_shape": "array"},
            timeout=timeout,
        )
        resp.raise_for_status()
        data = resp.json()

        return {row["slug"]: float(row["popularity"]) for row in data}
    except Exception:
        # If fetch fails, return empty dict (popularity stays at default 0.0)
        return {}
