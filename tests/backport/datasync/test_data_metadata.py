import datetime as dt
import json
from unittest import mock

import pandas as pd
import pytest
from sqlalchemy.orm import Session

from apps.backport.datasync.data_metadata import (
    _convert_strings_to_numeric,
    checksum_metadata,
    variable_data,
    variable_data_df_from_s3,
    variable_metadata,
)
from etl.db import get_engine
from etl.grapher_model import _infer_variable_type


def _call_variable_metadata(variable_id: int, variable_df: pd.DataFrame, variable_meta: dict) -> dict:
    engine = get_engine()

    origins_df = pd.DataFrame(
        {
            "descriptionSnapshot": ["Origin A", "Origin B"],
        }
    )
    faqs = [
        {
            "gdocId": "1",
            "fragmentId": "test",
        }
    ]
    topic_tags = ["Population"]

    with Session(engine) as session:
        with mock.patch("apps.backport.datasync.data_metadata._load_variable", return_value=variable_meta):
            with mock.patch("apps.backport.datasync.data_metadata._load_origins_df", return_value=origins_df):
                with mock.patch("apps.backport.datasync.data_metadata._load_faqs", return_value=faqs):
                    with mock.patch("apps.backport.datasync.data_metadata._load_topic_tags", return_value=topic_tags):
                        return variable_metadata(session, variable_id, variable_df)


def _variable_meta():
    return {
        "id": 525715,
        "name": "Population density",
        "unit": "people per km²",
        "description": "Population density by country...",
        "createdAt": pd.Timestamp("2022-09-20 12:16:46"),  # type: ignore
        "updatedAt": pd.Timestamp("2023-02-10 11:46:31"),  # type: ignore
        "code": None,
        "coverage": "",
        "timespan": "-10000-2100",
        "datasetId": 5774,
        "sourceId": 27065,
        "shortUnit": None,
        "display": '{"name": "Population density", "unit": "people per km²", "shortUnit": null, "includeInTable": true, "numDecimalPlaces": 1}',
        "type": "mixed",
        "sort": None,
        "columnOrder": 0,
        "originalMetadata": None,
        "grapherConfigAdmin": None,
        "shortName": "population_density",
        "catalogPath": "grapher/owid/latest/key_indicators/population_density#population_density",
        "dimensions": None,
        "datasetName": "Key Indicators",
        "nonRedistributable": 0,
        "schemaVersion": 2,
        "processingLevel": "minor",
        "grapherConfigETL": '{"title": "Population density"}',
        "license": '{"name": "License"}',
        "descriptionKey": '["Population density"]',
        "titlePublic": "Population density title",
        "titleVariant": "Population density variant",
        "attributionShort": "Gapminder",
        "attribution": None,
        "descriptionProcessing": None,
        "sourceName": "Gapminder (v6); UN (2022); HYDE (v3.2); Food and Agriculture Organization of the United Nations",
        "sourceDescription": '{"link": "https://www.gapminder.org/data/documentation/gd003/", "retrievedDate": "October 8, 2021", "additionalInfo": "Our World in Data builds...", "dataPublishedBy": "Gapminder (v6); United Nations - Population Division (2022); HYDE (v3.2); World Bank", "dataPublisherSource": null}',
        "dataChecksum": "123",
        "metadataChecksum": "456",
    }


def test_variable_metadata():
    variable_df = pd.DataFrame(
        {
            "value": ["0.008", "0.038", "0.022", "0.031", "NA"],
            "year": [-10000, -10000, -10000, -10000, -10000],
            "entityId": [273, 275, 276, 277, 294],
            "entityName": ["Africa", "Asia", "Europe", "Oceania", "North America"],
            "entityCode": [None, None, None, None, None],
        }
    )
    variable_meta = _variable_meta()

    meta = _call_variable_metadata(525715, variable_df, variable_meta)

    assert meta == {
        "catalogPath": "grapher/owid/latest/key_indicators/population_density#population_density",
        "dataChecksum": "123",
        "metadataChecksum": "456",
        "columnOrder": 0,
        "coverage": "",
        "createdAt": "2022-09-20T12:16:46.000Z",
        "datasetId": 5774,
        "datasetName": "Key Indicators",
        "description": "Population density by country...",
        "dimensions": {
            "entities": {
                "values": [
                    {"code": None, "id": 273, "name": "Africa"},
                    {"code": None, "id": 275, "name": "Asia"},
                    {"code": None, "id": 276, "name": "Europe"},
                    {"code": None, "id": 277, "name": "Oceania"},
                    {"code": None, "id": 294, "name": "North America"},
                ]
            },
            "years": {"values": [{"id": -10000}]},
        },
        "display": {
            "includeInTable": True,
            "name": "Population density",
            "numDecimalPlaces": 1,
            "shortUnit": None,
            "unit": "people per km²",
        },
        "id": 525715,
        "descriptionKey": ["Population density"],
        "name": "Population density",
        "nonRedistributable": False,
        "origins": [{"descriptionSnapshot": "Origin A"}, {"descriptionSnapshot": "Origin B"}],
        "presentation": {
            "faqs": [{"fragmentId": "test", "gdocId": "1"}],
            "grapherConfigETL": {"title": "Population density"},
            "attributionShort": "Gapminder",
            "titlePublic": "Population density title",
            "titleVariant": "Population density variant",
            "topicTagsLinks": ["Population"],
        },
        "license": {"name": "License"},
        "processingLevel": "minor",
        "schemaVersion": 2,
        "shortName": "population_density",
        "source": {
            "additionalInfo": "Our World in Data builds...",
            "dataPublishedBy": "Gapminder (v6); United Nations - Population "
            "Division (2022); HYDE (v3.2); World Bank",
            "dataPublisherSource": "",
            "id": 27065,
            "link": "https://www.gapminder.org/data/documentation/gd003/",
            "name": "Gapminder (v6); UN (2022); HYDE (v3.2); Food and "
            "Agriculture Organization of the United Nations",
            "retrievedDate": "October 8, 2021",
        },
        "timespan": "-10000-2100",
        "type": "mixed",
        "unit": "people per km²",
        "updatedAt": "2023-02-10T11:46:31.000Z",
    }


def test_variable_data():
    data_df = pd.DataFrame(
        {
            "value": ["-2", "1", "2.1", "UK", "9.8e+09"],
            "year": [-10000, -10000, -10000, -10000, -10000],
            "entityId": [273, 275, 276, 277, 294],
            "entityName": ["Africa", "Asia", "Europe", "Oceania", "North America"],
            "entityCode": [None, None, None, None, None],
        }
    )

    assert variable_data(data_df) == {
        "entities": [273, 275, 276, 277, 294],
        "values": [-2, 1, 2.1, "UK", 9800000000],
        "years": [-10000, -10000, -10000, -10000, -10000],
    }


def test_variable_metadata_ordinal():
    variable_df = pd.DataFrame(
        {
            "value": ["Middle", "High", "Low", "Low", "Middle"],
            "year": [2020, 2020, 2020, 2020, 2020],
            "entityId": [273, 275, 276, 277, 294],
            "entityName": ["Africa", "Asia", "Europe", "Oceania", "North America"],
            "entityCode": [None, None, None, None, None],
        }
    )
    variable_meta = _variable_meta()
    variable_meta["type"] = "ordinal"
    variable_meta["sort"] = json.dumps(["Low", "Middle", "High"])

    meta = _call_variable_metadata(525715, variable_df, variable_meta)
    assert meta["type"] == "ordinal"
    assert meta["dimensions"]["values"] == {
        "values": [{"id": 0, "name": "Low"}, {"id": 1, "name": "Middle"}, {"id": 2, "name": "High"}]
    }


def test_variable_data_df_from_s3():
    engine = mock.Mock()
    entities = pd.DataFrame(
        {
            "entityId": [1],
            "entityName": ["UK"],
            "entityCode": ["GBR"],
        }
    )
    s3_data = pd.DataFrame({"entities": [1, 1], "values": ["a", 2], "years": [2000, 2001]})

    with mock.patch("apps.backport.datasync.data_metadata._fetch_entities", return_value=entities):
        with mock.patch("pandas.read_json", return_value=s3_data):
            df = variable_data_df_from_s3(engine, [123])

    assert df.to_dict(orient="records") == [
        {"entityId": 1, "value": "a", "year": 2000, "variableId": 123, "entityName": "UK", "entityCode": "GBR"},
        {"entityId": 1, "value": "2", "year": 2001, "variableId": 123, "entityName": "UK", "entityCode": "GBR"},
    ]


def test_infer_variable_type():
    assert _infer_variable_type(pd.Series(["1", "2"])) == "int"
    assert _infer_variable_type(pd.Series(["1", "2.1"])) == "float"
    assert _infer_variable_type(pd.Series(["1", "2.0"])) == "float"
    assert _infer_variable_type(pd.Series(["1", "2.0", "a"])) == "mixed"
    assert _infer_variable_type(pd.Series(["1", "a"])) == "mixed"
    assert _infer_variable_type(pd.Series(["1.1", "a"])) == "mixed"
    assert _infer_variable_type(pd.Series(["a", "NA"])) == "string"
    assert _infer_variable_type(pd.Series([], dtype=object)) == "mixed"


def test_convert_strings_to_numeric():
    r = _convert_strings_to_numeric(["-2", "1", "2.1", "UK", "9.8e+09", "nan"])
    assert r == [-2, 1, 2.1, "UK", 9800000000, "nan"]
    assert [type(x) for x in r] == [int, int, float, str, int, str]

    with pytest.raises(AssertionError):
        r = _convert_strings_to_numeric([None, "UK"])  # type: ignore


def test_checksum_metadata():
    meta = _variable_meta()
    assert checksum_metadata(meta) == "76dc6be6b7509058c7b3d1a8e75704ec"

    # change id, checksums or updatedAt shouldn't change it
    meta = _variable_meta()
    meta["id"] = 999
    meta["dataChecksum"] = 999
    meta["updatedAt"] = dt.datetime.now()
    assert checksum_metadata(meta) == "76dc6be6b7509058c7b3d1a8e75704ec"
