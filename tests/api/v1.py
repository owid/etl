from unittest.mock import patch

import yaml
from fastapi.testclient import TestClient

from api.main import app
from etl.grapher import model as gm

client = TestClient(app)


def test_health():
    response = client.get("/api/v1/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


@patch("etl.grapher_model.Variable.from_catalog_path")
def test_update_indicator(mock_from_catalog_path):
    mock_from_catalog_path.return_value = gm.Variable(
        datasetId=1,
        description="",
        timespan="",
        unit="",
        coverage="",
        catalogPath="garden/dummy/2020-01-01/dummy/dummy#dummy",
        shortName="dummy",
        display={},
        dimensions=None,
        sourceId=None,
    )
    mock_from_catalog_path.id = 1
    response = client.put(
        "/api/v1/indicators",
        json={
            "catalogPath": "garden/dummy/2020-01-01/dummy/dummy#dummy",
            "indicator": {"name": "xxx"},
            "dataApiUrl": "https://api-staging.owid.io/mojmir/v1/indicators/",
            "triggerETL": False,
            "dryRun": True,
        },
    )
    assert response.status_code == 200

    assert yaml.safe_load(response.json()["yaml"]) == {"tables": {"dummy": {"variables": {"dummy": {"title": "xxx"}}}}}
