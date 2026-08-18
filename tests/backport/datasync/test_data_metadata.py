from unittest import mock

import pandas as pd
import pytest

from apps.backport.datasync.data_metadata import (
    _convert_strings_to_numeric,
    variable_data,
)
from etl.grapher.io import variable_data_df_from_s3
from etl.grapher.model import _infer_variable_type


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


def test_variable_data_df_from_s3():
    engine = mock.Mock()
    entities = pd.DataFrame(
        {
            "entityId": [1],
            "entityName": ["UK"],
            "entityCode": ["GBR"],
        }
    )
    fetched_data = pd.DataFrame({"entityId": [1, 1], "value": ["a", 2], "year": [2000, 2001], "variableId": [123, 123]})

    with mock.patch("etl.grapher.io._fetch_entities", return_value=entities):
        with mock.patch("etl.grapher.io._fetch_data_df_from_s3", return_value=fetched_data):
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
        r = _convert_strings_to_numeric([None, "UK"])  # ty: ignore
