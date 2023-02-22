from unittest import mock

import pandas as pd

from backport.datasync.data_metadata import (
    _convert_strings_to_numeric,
    _infer_variable_type,
    variable_metadata,
)
from etl.db import get_engine


def test_variable_metadata():
    engine = get_engine()
    variable_df = pd.DataFrame(
        {
            "value": [0.008, 0.038, 0.022, 0.031, 0.056],
            "year": [-10000, -10000, -10000, -10000, -10000],
            "entityId": [273, 275, 276, 277, 294],
            "entityName": ["Africa", "Asia", "Europe", "Oceania", "North America"],
            "entityCode": [None, None, None, None, None],
        }
    )
    variable_meta = (
        pd.Series(
            {
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
                "columnOrder": 0,
                "originalMetadata": None,
                "grapherConfig": None,
                "shortName": "population_density",
                "catalogPath": "grapher/owid/latest/key_indicators/population_density",
                "dimensions": None,
                "dataPath": None,
                "metadataPath": None,
                "datasetName": "Key Indicators",
                "nonRedistributable": 0,
                "sourceName": "Gapminder (v6); UN (2022); HYDE (v3.2); Food and Agriculture Organization of the United Nations",
                "sourceDescription": '{"link": "https://www.gapminder.org/data/documentation/gd003/", "retrievedDate": "October 8, 2021", "additionalInfo": "Our World in Data builds...", "dataPublishedBy": "Gapminder (v6); United Nations - Population Division (2022); HYDE (v3.2); World Bank", "dataPublisherSource": null}',
            }
        )
        .to_frame()
        .T
    )
    with mock.patch("pandas.read_sql", return_value=variable_meta):
        meta = variable_metadata(engine, 525715, variable_df)

    assert meta == {
        "id": 525715,
        "name": "Population density",
        "unit": "people per km²",
        "description": "Population density by country...",
        "createdAt": "2022-09-20T12:16:46.000Z",
        "updatedAt": "2023-02-10T11:46:31.000Z",
        "coverage": "",
        "timespan": "-10000-2100",
        "datasetId": 5774,
        "columnOrder": 0,
        "shortName": "population_density",
        "catalogPath": "grapher/owid/latest/key_indicators/population_density",
        "datasetName": "Key Indicators",
        "type": "float",
        "nonRedistributable": False,
        "display": {
            "name": "Population density",
            "unit": "people per km²",
            "shortUnit": None,
            "includeInTable": True,
            "numDecimalPlaces": 1,
        },
        "source": {
            "id": 27065,
            "name": "Gapminder (v6); UN (2022); HYDE (v3.2); Food and Agriculture Organization of the United Nations",
            "dataPublishedBy": "Gapminder (v6); United Nations - Population Division (2022); HYDE (v3.2); World Bank",
            "dataPublisherSource": "",
            "link": "https://www.gapminder.org/data/documentation/gd003/",
            "retrievedDate": "October 8, 2021",
            "additionalInfo": "Our World in Data builds...",
        },
        "dimensions": {
            "years": {"values": [{"id": -10000}]},
            "entities": {
                "values": [
                    {"id": 273, "name": "Africa", "code": None},
                    {"id": 275, "name": "Asia", "code": None},
                    {"id": 276, "name": "Europe", "code": None},
                    {"id": 277, "name": "Oceania", "code": None},
                    {"id": 294, "name": "North America", "code": None},
                ]
            },
        },
    }


def test_infer_variable_type():
    assert _infer_variable_type(pd.Series([1, 2])) == "int"
    assert _infer_variable_type(pd.Series([1, 2.5])) == "float"
    assert _infer_variable_type(pd.Series([1, 2.5, "c"])) == "mixed"
    assert _infer_variable_type(pd.Series(["a", "b", "c"])) == "string"

    assert _infer_variable_type(pd.Series(["1", "2"])) == "int"
    assert _infer_variable_type(pd.Series(["1", 2])) == "int"
    assert _infer_variable_type(pd.Series(["1", 2, "3.5"])) == "float"
    assert _infer_variable_type(pd.Series(["1", 2, "3.5", "a"])) == "mixed"


def test_convert_strings_to_numeric():
    assert _convert_strings_to_numeric(pd.Series(["1", "2.1", "UK"])).to_list() == ["1", "2.1", "UK"]
    assert _convert_strings_to_numeric(pd.Series(["1", "2"])).to_list() == [1, 2]
    assert _convert_strings_to_numeric(pd.Series(["1", "2.5"])).to_list() == [1, 2.5]
