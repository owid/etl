import concurrent.futures
import json
from typing import Any, Dict, List, Union
from urllib.error import HTTPError

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
    return pd.read_sql(q, engine, params={"variable_id": variable_id})


def _fetch_data_df_from_s3(data_path: str):
    try:
        variable_id = int(data_path.split("/")[-1].replace(".json", ""))
        return (
            pd.read_json(data_path)
            .rename(
                columns={
                    "entities": "entityId",
                    "values": "value",
                    "years": "year",
                }
            )
            .assign(variableId=variable_id)
        )
    # no data on S3 in dataPath
    except HTTPError:
        return pd.DataFrame(columns=["variableId", "entityId", "year", "value"])


def variable_data_df_from_s3(
    engine: Engine, data_paths: List[str] = [], variable_ids: List[int] = [], workers: int = 1
) -> pd.DataFrame:
    """Fetch data from S3 and add entity code and name from DB. You can use either data_paths or variable_ids."""
    if not data_paths:
        q = """
        SELECT
            dataPath
        FROM variables as v
        WHERE id in %(variable_ids)s
        """
        data_paths = pd.read_sql(
            q,
            engine,
            params={"variable_ids": variable_ids},
        )["dataPath"].tolist()

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        results = list(executor.map(lambda data_path: _fetch_data_df_from_s3(data_path), data_paths))

    df = pd.concat(results)

    # we work with strings and convert to specific types later
    df["value"] = df["value"].astype(str)

    return add_entity_code_and_name(engine, df)


def add_entity_code_and_name(engine: Engine, df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        df["entityName"] = []
        df["entityCode"] = []
        return df

    # add entities from DB
    q = """
    SELECT
        id AS entityId,
        name AS entityName,
        code AS entityCode
    FROM entities
    WHERE id in %(entity_ids)s
    """
    entities = pd.read_sql(q, engine, params={"entity_ids": list(df["entityId"].unique())})
    return df.merge(entities, on="entityId")


def variable_data(data_df: pd.DataFrame) -> Dict[str, Any]:
    data_df = data_df.rename(
        columns={
            "value": "values",
            "entityId": "entities",
            "year": "years",
        }
    )
    data = data_df[["values", "years", "entities"]].to_dict(orient="list")
    data["values"] = _convert_strings_to_numeric(data["values"])
    return data  # type: ignore


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
    # data_values does not contain null values
    assert values.notnull().all(), "values must not contain nulls"
    assert values.map(lambda x: isinstance(x, str)).all(), "only works for strings"
    if values.empty:
        return "mixed"
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
        if values.map(_is_float).any():
            return "mixed"
        else:
            return "string"


def _is_float(x):
    try:
        float(x)
    except ValueError:
        return False
    else:
        return True


def _convert_strings_to_numeric(lst: List[str]) -> List[Union[int, float, str]]:
    result = []
    for item in lst:
        assert isinstance(item, str)
        try:
            num = float(item)
            if num.is_integer():
                num = int(num)
        except ValueError:
            num = item
        result.append(num)
    return result


def _omit_nullable_values(d: dict) -> dict:
    return {k: v for k, v in d.items() if v is not None and not pd.isna(v)}
