"""Interim: show an explorer's own text (the text below it, its download link) on the multidim that replaces it.

Explorers can show text below the chart (a WordPress block, `wpBlockId`) and a download button
(`downloadDataLink`). A multidim has neither yet. Until grapher has a place for multidim-level
text, a multidim that replaces an explorer appends that text, written as markdown in
`<short_name>.explorer.md` next to its step, to every view's "What you should know about this
data", after the key points the view already shows and separated from them by a horizontal rule.

Remove this module, the `.explorer.md` files and the calls to it, once grapher can show
multidim-level text and the multidim's download package.

Grapher shows a view's metadata merged over the metadata of its first `y` indicator, and a
`description_key` there replaces the indicator's instead of adding to it. So the text cannot be
added on its own: each view gets the key points it shows today, read from the grapher DB (what
the page reads), with the text appended. The text is markdown as `SimpleMarkdownText` renders it:
paragraphs, bold, links, lists and images work; raw HTML (e.g. `<details>`) and tables do not.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING

from owid.catalog.core.meta import description_key_to_string

from etl.config import OWID_ENV, OWIDEnv

if TYPE_CHECKING:
    from etl.helpers import PathFinder
    from etl.viz.chart.model.core import Chart

SEPARATOR = "\n\n---\n\n"


def explorer_text_path(paths: PathFinder) -> Path:
    """The step's `<short_name>.explorer.md`."""
    return paths.directory / f"{paths.short_name}.explorer.md"


def add_explorer_text(chart: Chart, paths: PathFinder, owid_env: OWIDEnv | None = None) -> None:
    """Append the step's `<short_name>.explorer.md` to the key points of every view of `chart`, in place.

    Call it after `paths.create_chart(...)` (so indicator paths are complete) and before
    `chart.save()`.

    Args:
        chart: The multidim.
        paths: The step's PathFinder; the text is read from `<short_name>.explorer.md` next to the step.
        owid_env: The grapher DB to read the indicators' key points from. Defaults to OWID_ENV.
    """
    path = explorer_text_path(paths)
    if not path.exists():
        raise FileNotFoundError(f"No explorer text for this step: create {path}.")
    append_text_to_views(chart, path.read_text(), owid_env=owid_env)


def append_text_to_views(chart: Chart, text: str, owid_env: OWIDEnv | None = None) -> None:
    """Append markdown `text` to the key points of every view of `chart`, in place."""
    text = text.strip()
    if not text:
        raise ValueError("The explorer text is empty.")

    catalog_paths = {view.indicators.y[0].catalogPath for view in chart.views if view.indicators.y}
    indicator_keys = _description_keys_from_db(catalog_paths, owid_env or OWID_ENV)

    for view in chart.views:
        metadata = dict(view.metadata or {})
        if "description_key" in metadata:
            current = metadata["description_key"]
        elif view.indicators.y:
            current = indicator_keys.get(view.indicators.y[0].catalogPath)
        else:
            current = None
        metadata["description_key"] = append_text(current, text)
        view.metadata = metadata


def append_text(description_key: str | list[str] | None, text: str) -> str:
    """Add `text` after the existing key points, separated by a horizontal rule.

    A list of key points (the legacy form) is rendered as bullets first. Without key points the
    text stands alone. A value that already ends with the text is returned unchanged.
    """
    current = description_key_to_string(description_key) if isinstance(description_key, list) else description_key
    current = (current or "").strip()
    if not current:
        return text
    if current.endswith(text):
        return current
    return f"{current}{SEPARATOR}{text}"


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
