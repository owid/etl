"""Interim: point every view of a multidim at a file to download, as a key point.

Explorers can carry a `downloadDataLink`, shown as a download button. A multidim that
replaces one has no equivalent yet: ETL already builds a complete-dataset package for it
(`download_package.py`), but grapher does not link to it. Until it does, a multidim that has
to keep offering the explorer's download adds the link as the last bullet of each view's
"What you should know about this data".

Remove this module, and the calls to it, once grapher shows the download package.

Grapher shows a view's metadata merged over the metadata of its first `y` indicator, and a
`description_key` there replaces the indicator's instead of adding to it. So the bullet
cannot be added on its own: each view gets the key points it shows today, read from the
grapher DB (what the page reads), with the bullet appended.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from owid.catalog.core.meta import description_key_to_string

from etl.config import OWID_ENV, OWIDEnv

if TYPE_CHECKING:
    from etl.viz.chart.model.core import Chart


def add_download_link(chart: Chart, url: str, text: str, owid_env: OWIDEnv | None = None) -> None:
    """Append a download bullet to the key points of every view of `chart`, in place.

    Call it after `paths.create_chart(...)` (so indicator paths are complete) and before
    `chart.save()`.

    Args:
        chart: The multidim.
        url: The file or page to download from.
        text: The bullet, in markdown, with `{url}` where the link goes, e.g.
            "Download the complete dataset [here]({url})."
        owid_env: The grapher DB to read the indicators' key points from. Defaults to OWID_ENV.
    """
    if "{url}" not in text:
        raise ValueError("`text` must contain `{url}`, where the link goes.")
    bullet = text.format(url=url)

    paths = {view.indicators.y[0].catalogPath for view in chart.views if view.indicators.y}
    indicator_keys = _description_keys_from_db(paths, owid_env or OWID_ENV)

    for view in chart.views:
        metadata = dict(view.metadata or {})
        if "description_key" in metadata:
            current = metadata["description_key"]
        elif view.indicators.y:
            current = indicator_keys.get(view.indicators.y[0].catalogPath)
        else:
            current = None
        metadata["description_key"] = append_bullet(current, bullet)
        view.metadata = metadata


def append_bullet(description_key: str | list[str] | None, bullet: str) -> str | list[str]:
    """Add `bullet` after the existing key points, keeping them as they are.

    A list gets one more item. A markdown string that is already a bulleted list gets one more
    line. A single paragraph of prose (how a one-item list renders) becomes the first of two
    bullets. A value that already contains the bullet is returned unchanged.
    """
    if not description_key:
        return [bullet]
    if isinstance(description_key, list):
        return description_key if bullet in description_key else [*description_key, bullet]
    if bullet in description_key:
        return description_key
    first_line = next(line for line in description_key.splitlines() if line.strip())
    if first_line.lstrip().startswith(("- ", "* ")):
        return f"{description_key.rstrip()}\n- {bullet}"
    return description_key_to_string([description_key, bullet]) or bullet


def _description_keys_from_db(catalog_paths: set[str], owid_env: OWIDEnv) -> dict[str, str | None]:
    """Each indicator's `descriptionKey` in the grapher DB, as markdown."""
    if not catalog_paths:
        return {}
    df = owid_env.read_sql(
        "SELECT catalogPath, descriptionKey FROM variables WHERE catalogPath IN %(paths)s",
        params={"paths": tuple(catalog_paths)},
    )
    missing = catalog_paths - set(df["catalogPath"])
    if missing:
        raise ValueError(f"Indicators not in the grapher DB, run their grapher step first: {sorted(missing)}")

    out: dict[str, str | None] = {}
    for path, raw in zip(df["catalogPath"], df["descriptionKey"]):
        # The JSON column comes back as text; a NULL comes back as NaN from pandas.
        value = json.loads(raw) if isinstance(raw, str) else raw if isinstance(raw, list) else None
        # Rows written before description_key became a markdown string hold lists.
        out[path] = description_key_to_string(value) if isinstance(value, list) else value
    return out
