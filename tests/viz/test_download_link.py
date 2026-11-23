from types import SimpleNamespace

import pandas as pd
import pytest

from etl.viz.chart.download_link import add_download_link, append_bullet

BULLET = "Download the complete dataset [here](https://example.org)."


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


def test_append_to_bulleted_markdown():
    assert append_bullet("- One.\n- Two.", BULLET) == f"- One.\n- Two.\n- {BULLET}"


def test_append_to_list():
    assert append_bullet(["One.", "Two."], BULLET) == ["One.", "Two.", BULLET]


def test_prose_becomes_first_bullet():
    assert append_bullet("A single paragraph.", BULLET) == f"- A single paragraph.\n- {BULLET}"


def test_no_key_points():
    assert append_bullet(None, BULLET) == [BULLET]
    assert append_bullet("", BULLET) == [BULLET]


def test_already_there():
    assert append_bullet(f"- One.\n- {BULLET}", BULLET) == f"- One.\n- {BULLET}"
    assert append_bullet([BULLET], BULLET) == [BULLET]


def test_add_download_link_keeps_indicator_and_view_key_points():
    env = FakeEnv({"grapher/a/latest/a/a#x": '"- From the indicator."', "grapher/a/latest/a/a#y": None})
    views = [
        view("grapher/a/latest/a/a#x"),
        view("grapher/a/latest/a/a#y"),
        # A view's own key points win over the indicator's, as on the page.
        view("grapher/a/latest/a/a#x", {"description_key": ["Set in the view."], "description_short": "Kept."}),
    ]
    chart = SimpleNamespace(views=views)

    add_download_link(chart, "https://example.org", "Download the complete dataset [here]({url}).", owid_env=env)

    assert views[0].metadata == {"description_key": f"- From the indicator.\n- {BULLET}"}
    assert views[1].metadata == {"description_key": [BULLET]}
    assert views[2].metadata == {"description_key": ["Set in the view.", BULLET], "description_short": "Kept."}


def test_add_download_link_fails_on_indicator_missing_from_db():
    chart = SimpleNamespace(views=[view("grapher/a/latest/a/a#missing")])
    with pytest.raises(ValueError, match="not in the grapher DB"):
        add_download_link(chart, "https://example.org", "[Download]({url})", owid_env=FakeEnv({}))


def test_text_needs_url_placeholder():
    with pytest.raises(ValueError, match="must contain"):
        add_download_link(SimpleNamespace(views=[]), "https://example.org", "Download it.", owid_env=FakeEnv({}))
