import io

import pandas as pd
from click.testing import CliRunner

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
    monkeypatch.setattr(indicators.http_session, "get", lambda url, timeout: Response())
    monkeypatch.setattr(indicators, "indicators_from_db", _fail)

    pd.testing.assert_frame_equal(indicators.load_indicators(), df)


def test_publish_refuses_outside_production(monkeypatch):
    monkeypatch.setattr(indicators.config, "DATA_API_ENV", "staging-site-x")
    monkeypatch.setattr(indicators, "indicators_from_db", _fail)
    monkeypatch.setattr(indicators.s3_utils, "upload", _fail)

    result = CliRunner().invoke(indicators.main)

    assert result.exit_code != 0
    assert "staging-site-x" in result.output


def _fail(*args, **kwargs):
    raise AssertionError("this test must not read the database or upload")
