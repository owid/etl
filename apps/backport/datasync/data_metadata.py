import concurrent.futures
import json
from http.client import RemoteDisconnected
from typing import Any, Dict, List, Union, cast
from urllib.error import HTTPError, URLError

import numpy as np
import pandas as pd
from sqlalchemy.engine import Engine
from sqlmodel import Session
from tenacity import Retrying
from tenacity.retry import retry_if_exception_type
from tenacity.stop import stop_after_attempt
from tenacity.wait import wait_fixed

from etl import config


def _fetch_data_df_from_s3(variable_id: int):
    try:
        # Cloudflare limits us to 600 requests per minute, retry in case we hit the limit
        # NOTE: increase wait time or attempts if we hit the limit too often
        for attempt in Retrying(
            wait=wait_fixed(2),
            stop=stop_after_attempt(3),
            retry=retry_if_exception_type((URLError, RemoteDisconnected)),
        ):
            with attempt:
                return (
                    pd.read_json(config.variable_data_url(variable_id))
                    .rename(
                        columns={
                            "entities": "entityId",
                            "values": "value",
                            "years": "year",
                        }
                    )
                    .assign(variableId=variable_id)
                )
    # no data on S3
    except HTTPError:
        return pd.DataFrame(columns=["variableId", "entityId", "year", "value"])


def variable_data_df_from_s3(engine: Engine, variable_ids: List[int] = [], workers: int = 1) -> pd.DataFrame:
    """Fetch data from S3 and add entity code and name from DB."""
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        results = list(executor.map(_fetch_data_df_from_s3, variable_ids))

    if isinstance(results, list) and all(isinstance(df, pd.DataFrame) for df in results):
        df = pd.concat(cast(List[pd.DataFrame], results))
    else:
        raise TypeError(f"results must be a list of pd.DataFrame, got {type(results)}")

    # we work with strings and convert to specific types later
    df["value"] = df["value"].astype(str)

    with Session(engine) as session:
        return add_entity_code_and_name(session, df)


def _fetch_entities(session: Session, entity_ids: List[int]) -> pd.DataFrame:
    # Query entities from the database
    q = """
    SELECT
        id AS entityId,
        name AS entityName,
        code AS entityCode
    FROM entities
    WHERE id in :entity_ids
    """

    # Execute the SQL using session
    result_proxy = session.execute(q, {"entity_ids": entity_ids})  # type: ignore

    # Convert the result into a DataFrame
    return pd.DataFrame(result_proxy.fetchall(), columns=result_proxy.keys())


def add_entity_code_and_name(session: Session, df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        df["entityName"] = []
        df["entityCode"] = []
        return df

    entities = _fetch_entities(session, list(df["entityId"].unique()))

    return pd.merge(df, entities, on="entityId")


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


def _load_variable(session: Session, variable_id: int) -> Dict[str, Any]:
    sql = """
    SELECT
        variables.*,
        datasets.name AS datasetName,
        datasets.nonRedistributable AS nonRedistributable,
        datasets.updatePeriodDays,
        datasets.version as datasetVersion,
        sources.name AS sourceName,
        sources.description AS sourceDescription
    FROM variables
    JOIN datasets ON variables.datasetId = datasets.id
    LEFT JOIN sources ON variables.sourceId = sources.id
    WHERE variables.id = :variable_id
    """

    # Using the session to execute raw SQL and fetching one row as a result
    result = session.execute(sql, {"variable_id": variable_id}).fetchone()  # type: ignore

    # Ensure result exists and convert to dictionary
    assert result, f"variableId `{variable_id}` not found"
    return dict(result)


def _load_topic_tags(session: Session, variable_id: int) -> List[str]:
    sql = """
    SELECT
        tags.name
    FROM tags_variables_topic_tags
    JOIN tags ON tags_variables_topic_tags.tagId = tags.id
    WHERE variableId = :variable_id
    """

    # Using the session to execute raw SQL
    result = session.execute(sql, {"variable_id": variable_id}).fetchall()  # type: ignore

    # Extract tag names from the result and return as a list
    return [row[0] for row in result]


def _load_faqs(session: Session, variable_id: int) -> List[Dict[str, Any]]:
    sql = """
    SELECT
        gdocId,
        fragmentId
    FROM posts_gdocs_variables_faqs
    WHERE variableId = :variable_id
    """

    # Using the session to execute raw SQL
    result = session.execute(sql, {"variable_id": variable_id}).fetchall()  # type: ignore

    # Convert the result rows to a list of dictionaries
    return [dict(row) for row in result]


def _load_origins_df(session: Session, variable_id: int) -> pd.DataFrame:
    sql = """
    SELECT
        origins.*
    FROM origins
    JOIN origins_variables ON origins.id = origins_variables.originId
    WHERE origins_variables.variableId = :variable_id
    """

    # Use the session to execute the raw SQL
    result_proxy = session.execute(sql, {"variable_id": variable_id})  # type: ignore

    # Fetch the results into a DataFrame
    df = pd.DataFrame(result_proxy.fetchall(), columns=result_proxy.keys())

    # Process the 'license' column
    df["license"] = df["license"].map(lambda x: json.loads(x) if x else None)

    return df


def _variable_metadata(
    db_variable_row: Dict[str, Any],
    variable_data: pd.DataFrame,
    db_origins_df: pd.DataFrame,
    db_topic_tags: list[str],
    db_faqs: list[dict],
) -> Dict[str, Any]:
    row = db_variable_row

    variable = row
    sourceId = row.pop("sourceId")
    sourceName = row.pop("sourceName")
    sourceDescription = row.pop("sourceDescription")
    nonRedistributable = row.pop("nonRedistributable")
    displayJson = row.pop("display")

    schemaVersion = row.pop("schemaVersion")
    processingLevel = row.pop("processingLevel")
    grapherConfigETLJson = row.pop("grapherConfigETL")
    grapherConfigAdminJson = row.pop("grapherConfigAdmin")
    licenseJson = row.pop("license")
    keyInfoTextJson = row.pop("keyInfoText")

    display = json.loads(displayJson)
    grapherConfigETL = json.loads(grapherConfigETLJson) if grapherConfigETLJson else None
    grapherConfigAdmin = json.loads(grapherConfigAdminJson) if grapherConfigAdminJson else None
    license = json.loads(licenseJson) if licenseJson else None
    keyInfoText = json.loads(keyInfoTextJson) if keyInfoTextJson else None

    # group fields from flat structure into presentation field
    presentation = dict(
        grapherConfigETL=grapherConfigETL,
        grapherConfigAdmin=grapherConfigAdmin,
        titlePublic=row.pop("titlePublic"),
        titleVariant=row.pop("titleVariant"),
        producerShort=row.pop("producerShort"),
        attribution=row.pop("attribution"),
        topicTagsLinks=db_topic_tags,
        faqs=db_faqs,
        keyInfoText=keyInfoText,
        processingInfo=row.pop("processingInfo"),
    )

    variableMetadata = dict(
        **_omit_nullable_values(variable),
        type="mixed",  # precise type will be updated further down
        nonRedistributable=bool(nonRedistributable),
        display=display,
        schemaVersion=schemaVersion,
        processingLevel=processingLevel,
        presentation=_omit_nullable_values(presentation),
        license=license,
        keyInfoText=keyInfoText,
    )

    # add source
    if sourceId:
        partialSource = json.loads(sourceDescription)
        variableMetadata["source"] = dict(
            id=sourceId,
            name=sourceName,
            dataPublishedBy=partialSource.get("dataPublishedBy") or "",
            dataPublisherSource=partialSource.get("dataPublisherSource") or "",
            link=partialSource.get("link") or "",
            retrievedDate=partialSource.get("retrievedDate") or "",
            additionalInfo=partialSource.get("additionalInfo") or "",
        )

    variableMetadata = _omit_nullable_values(variableMetadata)

    entityArray = (
        variable_data[["entityId", "entityName", "entityCode"]]
        .drop_duplicates(["entityId"])
        .rename(columns={"entityId": "id", "entityName": "name", "entityCode": "code"})
        .set_index("id", drop=False)
        .astype(object)
        # avoid NaN in JSON
        .replace(to_replace=np.nan, value=None)
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
    variableMetadata["origins"] = [_omit_nullable_values(d) for d in db_origins_df.to_dict(orient="records")]  # type: ignore

    return variableMetadata


def variable_metadata(session: Session, variable_id: int, variable_data: pd.DataFrame) -> Dict[str, Any]:
    """Fetch metadata for a single variable from database. This function was initially based on the
    one from owid-grapher repository and uses raw SQL commands. It'd be interesting to rewrite it
    using SQLAlchemy ORM in grapher_model.py.
    """
    return _variable_metadata(
        db_variable_row=_load_variable(session, variable_id),
        variable_data=variable_data,
        db_origins_df=_load_origins_df(session, variable_id),
        db_topic_tags=_load_topic_tags(session, variable_id),
        db_faqs=_load_faqs(session, variable_id),
    )


def _infer_variable_type(values: pd.Series) -> str:
    # values don't contain null values
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
    return {k: v for k, v in d.items() if v is not None and (isinstance(v, list) and len(v) or not pd.isna(v))}
