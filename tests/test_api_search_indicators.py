import io

import pandas as pd

from api_search import indicators


def test_load_indicators_reads_the_published_copy(monkeypatch):
    df = pd.DataFrame(
        {
            "variableId": [1, 2],
            "name": ["Life expectancy", "Population"],
            "description": ["", "Total population"],
            "catalogPath": ["grapher/demography/2024-07-15/le/le#life_expectancy", "indicator/2"],
            "n_charts": [3, 0],
            "popularity": [0.5, 0.0],
        }
    )
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)

    class Response:
        content = buffer.getvalue()

        def raise_for_status(self):
            pass

    monkeypatch.setattr(indicators.config, "SEARCH_INDICATORS_URL", "https://example.org/search_indicators.parquet")
    monkeypatch.setattr(indicators.requests, "get", lambda url, timeout: Response())
    monkeypatch.setattr(indicators, "indicators_from_db", lambda: _fail_db_read())

    loaded = indicators.load_indicators()

    assert [i.variableId for i in loaded] == [1, 2]
    assert loaded[0].catalogPath == "grapher/demography/2024-07-15/le/le#life_expectancy"
    assert loaded[1].popularity == 0.0


def _fail_db_read():
    raise AssertionError("read the database although SEARCH_INDICATORS_URL is set")
