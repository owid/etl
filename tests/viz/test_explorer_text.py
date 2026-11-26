from types import SimpleNamespace

import pandas as pd
import pytest

from etl.viz.chart.explorer_text import add_explorer_text, append_text, append_text_to_views

TEXT = "**About this data explorer**\n\nSome text with [a link](https://example.org)."


class FakeEnv:
    """Stands in for the grapher DB: `variables` rows as (catalogPath, descriptionKey JSON)."""

    def __init__(self, rows: dict[str, str | None]):
        self.rows = rows

    def read_sql(self, query, params):
        paths = [p for p in params["paths"] if p in self.rows]
        return pd.DataFrame({"catalogPath": paths, "descriptionKey": [self.rows[p] for p in paths]})


def view(path: str | None, metadata: dict | None = None):
    y = [SimpleNamespace(catalogPath=path)] if path else None
    return SimpleNamespace(indicators=SimpleNamespace(y=y), metadata=metadata)


def test_append_after_markdown_key_points():
    assert append_text("- One.\n- Two.", TEXT) == f"- One.\n- Two.\n\n---\n\n{TEXT}"


def test_append_after_legacy_list():
    assert append_text(["One.", "Two."], TEXT) == f"- One.\n- Two.\n\n---\n\n{TEXT}"


def test_no_key_points():
    assert append_text(None, TEXT) == TEXT
    assert append_text("", TEXT) == TEXT
    assert append_text([], TEXT) == TEXT


def test_already_there():
    once = append_text("- One.", TEXT)
    assert append_text(once, TEXT) == once


def test_views_keep_indicator_and_view_key_points():
    env = FakeEnv({"grapher/a/latest/a/a#x": '"- From the indicator."', "grapher/a/latest/a/a#y": None})
    views = [
        view("grapher/a/latest/a/a#x"),
        view("grapher/a/latest/a/a#y"),
        # A view's own key points win over the indicator's, as on the page.
        view("grapher/a/latest/a/a#x", {"description_key": ["Set in the view."], "description_short": "Kept."}),
        view(None),
    ]

    append_text_to_views(SimpleNamespace(views=views), f"\n{TEXT}\n\n", owid_env=env)

    assert views[0].metadata == {"description_key": f"- From the indicator.\n\n---\n\n{TEXT}"}
    assert views[1].metadata == {"description_key": TEXT}
    assert views[2].metadata == {"description_key": f"Set in the view.\n\n---\n\n{TEXT}", "description_short": "Kept."}
    assert views[3].metadata == {"description_key": TEXT}


def test_fails_on_indicator_missing_from_db():
    chart = SimpleNamespace(views=[view("grapher/a/latest/a/a#missing")])
    with pytest.raises(ValueError, match="not in the grapher DB"):
        append_text_to_views(chart, TEXT, owid_env=FakeEnv({}))


def test_fails_on_empty_text():
    with pytest.raises(ValueError, match="empty"):
        append_text_to_views(SimpleNamespace(views=[]), "  \n", owid_env=FakeEnv({}))


def test_reads_the_step_file(tmp_path):
    paths = SimpleNamespace(directory=tmp_path, short_name="thing")
    chart = SimpleNamespace(views=[view(None)])

    with pytest.raises(FileNotFoundError, match="thing.explorer.md"):
        add_explorer_text(chart, paths, owid_env=FakeEnv({}))

    (tmp_path / "thing.explorer.md").write_text(TEXT + "\n")
    add_explorer_text(chart, paths, owid_env=FakeEnv({}))
    assert chart.views[0].metadata == {"description_key": TEXT}
