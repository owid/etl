import concurrent.futures
import json
import re
from http.client import RemoteDisconnected
from typing import Any, Dict, List, Union, cast
from urllib.error import HTTPError, URLError

import pandas as pd
from sqlalchemy.engine import Engine
from tenacity import Retrying
from tenacity.retry import retry_if_exception_type
from tenacity.stop import stop_after_attempt
from tenacity.wait import wait_fixed


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


def _extract_variable_id_from_data_path(data_path: str) -> int:
    match = re.search(r"/indicators/(\d+)", data_path)
    if match:
        return int(match.group(1))
    else:
        match = re.search(r"/data/(\d+)", data_path)
        assert match, f"Could not find variableId in dataPath `{data_path}`"
        return int(match.group(1))


def _fetch_data_df_from_s3(data_path: str):
    try:
        variable_id = _extract_variable_id_from_data_path(data_path)

        # Cloudflare limits us to 600 requests per minute, retry in case we hit the limit
        # NOTE: increase wait time or attempts if we hit the limit too often
        for attempt in Retrying(
            wait=wait_fixed(2),
            stop=stop_after_attempt(3),
            retry=retry_if_exception_type((URLError, RemoteDisconnected)),
        ):
            with attempt:
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

    if isinstance(results, list) and all(isinstance(df, pd.DataFrame) for df in results):
        df = pd.concat(cast(List[pd.DataFrame], results))
    else:
        raise TypeError(f"results must be a list of pd.DataFrame, got {type(results)}")

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
    return pd.DataFrame(df).merge(entities, on="entityId")


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


def _load_variable(engine: Engine, variable_id: int) -> Dict[str, Any]:
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
    return df.iloc[0].to_dict()


def _load_origins_df(engine: Engine, variable_id: int) -> pd.DataFrame:
    sql = """
    SELECT
        origins.*
    FROM origins
    JOIN origins_variables ON origins.id = origins_variables.originId
    WHERE origins_variables.variableId = %(variable_id)s
    """
    return pd.read_sql(sql, engine, params={"variable_id": variable_id})


def variable_metadata(engine: Engine, variable_id: int, variable_data: pd.DataFrame) -> Dict[str, Any]:
    """Fetch metadata for a single variable from database.
    This function is similar to Variables.getVariableData in owid-grapher repository
    """
    row = _load_variable(engine, variable_id)

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

    # add origins
    origins_df = _load_origins_df(engine, variable_id)
    variableMetadata["origins"] = [_omit_nullable_values(d) for d in origins_df.to_dict(orient="records")]  # type: ignore

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
