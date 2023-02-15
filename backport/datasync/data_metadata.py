import json
from typing import Any, Dict
from urllib.error import HTTPError

import numpy as np
import pandas as pd
from sqlalchemy.engine import Engine


def variable_data_df_from_mysql(engine: Engine, variable_id: int) -> pd.DataFrame:
    q = """
    SELECT
        value,
        year,
        entities.id AS entityId,
        entities.name AS entityName,
        entities.code AS entityCode
    FROM data_values
    LEFT JOIN entities ON data_values.entityId = entities.id
    WHERE data_values.variableId = %(variable_id)s
    ORDER BY
        year ASC
    """
    df = pd.read_sql(q, engine, params={"variable_id": variable_id})

    # convert from string to numerical type if possible
    df["value"] = _convert_to_numeric(df["value"])

    return df


def variable_data_df_from_s3(engine: Engine, data_path: str) -> pd.DataFrame:
    empty_df = pd.DataFrame(columns=["entityId", "entityName", "entityCode", "year", "value"])
    try:
        df = pd.read_json(data_path).rename(
            columns={
                "entities": "entityId",
                "values": "value",
                "years": "year",
            }
        )
    # no data on S3 in dataPath
    except HTTPError:
        return empty_df

    if df.empty:
        return empty_df

    # add entities from DB
    q = """
    SELECT
        id AS entityId,
        name AS entityName,
        code AS entityCode
    FROM entities
    WHERE id in %(entity_ids)s
    """
    entities = pd.read_sql(q, engine, params={"entity_ids": df["entityId"].tolist()})
    df = df.merge(entities, on="entityId")

    return df


def variable_data(data_df: pd.DataFrame) -> Dict[str, Any]:
    data_df = data_df.rename(
        columns={
            "value": "values",
            "entityId": "entities",
            "year": "years",
        }
    )
    return data_df[["values", "years", "entities"]].to_dict(orient="list")


def variable_metadata(engine: Engine, variable_id: int, variable_data: pd.DataFrame) -> Dict[str, Any]:
    """Fetch metadata for a single variable from database.
    This function is similar to Variables.getVariableData in owid-grapher repository
    """
    sql = """
    SELECT
        variables.*,
        datasets.name AS datasetName,
        datasets.nonRedistributable AS nonRedistributable,
        sources.name AS sourceName,
        sources.description AS sourceDescription
    FROM variables
    JOIN datasets ON variables.datasetId = datasets.id
    JOIN sources ON variables.sourceId = sources.id
    WHERE variables.id = %(variable_id)s
    """
    df = pd.read_sql(sql, engine, params={"variable_id": variable_id})
    assert not df.empty, f"variableId `{variable_id}` not found"

    row = df.iloc[0].to_dict()

    variable = row
    sourceId = row.pop("sourceId")
    sourceName = row.pop("sourceName")
    sourceDescription = row.pop("sourceDescription")
    nonRedistributable = row.pop("nonRedistributable")
    displayJson = row.pop("display")

    display = json.loads(displayJson)
    partialSource = json.loads(sourceDescription)
    variableMetadata = dict(
        **_omit_nullable_values(variable),
        type="mixed",  # precise type will be updated further down
        nonRedistributable=bool(nonRedistributable),
        display=display,
        source=dict(
            id=sourceId,
            name=sourceName,
            dataPublishedBy=partialSource.get("dataPublishedBy") or "",
            dataPublisherSource=partialSource.get("dataPublisherSource") or "",
            link=partialSource.get("link") or "",
            retrievedDate=partialSource.get("retrievedDate") or "",
            additionalInfo=partialSource.get("additionalInfo") or "",
        ),
    )

    entityArray = (
        variable_data[["entityId", "entityName", "entityCode"]]
        .drop_duplicates(["entityId"])
        .rename(columns={"entityId": "id", "entityName": "name", "entityCode": "code"})
        .set_index("id", drop=False)
        .to_dict(orient="records")
    )

    yearArray = (
        variable_data[["year"]]
        .drop_duplicates(["year"])
        .rename(columns={"year": "id"})
        .set_index("id", drop=False)
        .to_dict(orient="records")
    )

    variableData = variable_data[["year", "entityId", "value"]].rename(
        columns={"year": "years", "entityId": "entities", "value": "values"}
    )

    variableMetadata["type"] = _infer_variable_type(variableData["values"])

    variableMetadata["dimensions"] = {
        "years": {"values": yearArray},
        "entities": {"values": entityArray},
    }

    # convert timestamp to string
    time_format = "%Y-%m-%dT%H:%M:%S.000Z"
    for col in ("createdAt", "updatedAt"):
        variableMetadata[col] = variableMetadata[col].strftime(time_format)  # type: ignore

    return variableMetadata


def _infer_variable_type(values: pd.Series) -> str:
    try:
        values = pd.to_numeric(values)
        inferred_type = pd.api.types.infer_dtype(values)
        if inferred_type == "floating":
            return "float"
        elif inferred_type == "integer":
            return "int"
        else:
            raise NotImplementedError()
    except ValueError:
        if values.map(lambda s: isinstance(s, str)).all():
            return "string"
        else:
            return "mixed"


def _convert_to_numeric(values: pd.Series) -> pd.Series:
    values = values.astype(str).replace("nan", np.nan)
    # raises ValueError if any value is not numeric or float
    try:
        return values.map(int)
    except ValueError:
        pass
    # perhaps they're all floats
    try:
        return values.map(float)
    except ValueError:
        pass
    # otherwise return them as strings
    return values


def _omit_nullable_values(d: dict) -> dict:
    return {k: v for k, v in d.items() if v is not None and not pd.isna(v)}
