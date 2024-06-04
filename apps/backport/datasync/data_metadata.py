import concurrent.futures
import json
from http.client import RemoteDisconnected
from typing import Any, Dict, List, Union, cast
from urllib.error import HTTPError, URLError

import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session
from structlog import get_logger
from tenacity import Retrying
from tenacity.retry import retry_if_exception_type
from tenacity.stop import stop_after_attempt
from tenacity.wait import wait_fixed

from etl import config
from etl.db import read_sql

log = get_logger()


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
    log.info("1/ getting data")
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        results = list(executor.map(_fetch_data_df_from_s3, variable_ids))

    log.info("2/ building df")
    if isinstance(results, list) and all(isinstance(df, pd.DataFrame) for df in results):
        df = pd.concat(cast(List[pd.DataFrame], results))
    else:
        raise TypeError(f"results must be a list of pd.DataFrame, got {type(results)}")

    # we work with strings and convert to specific types later
    log.info("3/ setting string")
    df["value"] = df["value"].astype(str)

    log.info("4/ adding code/name")
    with Session(engine) as session:
        res = add_entity_code_and_name(session, df)
        log.info("5/ finished")
        return res


def _fetch_entities(session: Session, entity_ids: List[int]) -> pd.DataFrame:
    # Query entities from the database
    q = """
    SELECT
        id AS entityId,
        name AS entityName,
        code AS entityCode
    FROM entities
    WHERE id in %(entity_ids)s
    """
    return read_sql(q, session, params={"entity_ids": entity_ids})


def add_entity_code_and_name(session: Session, df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        df["entityName"] = []
        df["entityCode"] = []
        return df

    unique_entities = df["entityId"].unique()

    entities = _fetch_entities(session, list(unique_entities))

    if set(unique_entities) - set(entities.entityId):
        missing_entities = set(unique_entities) - set(entities.entityId)
        raise ValueError(f"Missing entities in the database: {missing_entities}")

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
    result = session.execute(text(sql), {"variable_id": variable_id}).fetchone()

    # Ensure result exists and convert to dictionary
    assert result, f"variableId `{variable_id}` not found"
    return dict(result._mapping)


def _load_topic_tags(session: Session, variable_id: int) -> List[str]:
    sql = """
    SELECT
        tags.name
    FROM tags_variables_topic_tags
    JOIN tags ON tags_variables_topic_tags.tagId = tags.id
    WHERE variableId = :variable_id
    ORDER BY displayOrder
    """

    # Using the session to execute raw SQL
    result = session.execute(text(sql), {"variable_id": variable_id}).fetchall()

    # Extract tag names from the result and return as a list
    return [row[0] for row in result]


def _load_faqs(session: Session, variable_id: int) -> List[Dict[str, Any]]:
    sql = """
    SELECT
        gdocId,
        fragmentId
    FROM posts_gdocs_variables_faqs
    WHERE variableId = :variable_id
    ORDER BY displayOrder
    """

    # Using the session to execute raw SQL
    result = session.execute(text(sql), {"variable_id": variable_id}).fetchall()

    # Convert the result rows to a list of dictionaries
    return [dict(row._mapping) for row in result]


def _load_origins_df(session: Session, variable_id: int) -> pd.DataFrame:
    sql = """
    SELECT
        origins.*
    FROM origins
    JOIN origins_variables ON origins.id = origins_variables.originId
    WHERE origins_variables.variableId = :variable_id
    ORDER BY displayOrder
    """

    # Use the session to execute the raw SQL
    result_proxy = session.execute(text(sql), {"variable_id": variable_id})

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
    descriptionKeyJson = row.pop("descriptionKey")
    sortJson = row.pop("sort")

    display = json.loads(displayJson)
    grapherConfigETL = json.loads(grapherConfigETLJson) if grapherConfigETLJson else None
    grapherConfigAdmin = json.loads(grapherConfigAdminJson) if grapherConfigAdminJson else None
    license = json.loads(licenseJson) if licenseJson else None
    descriptionKey = json.loads(descriptionKeyJson) if descriptionKeyJson else None
    sort = json.loads(sortJson) if sortJson else None

    # group fields from flat structure into presentation field
    presentation = dict(
        grapherConfigETL=grapherConfigETL,
        grapherConfigAdmin=grapherConfigAdmin,
        titlePublic=row.pop("titlePublic"),
        titleVariant=row.pop("titleVariant"),
        attributionShort=row.pop("attributionShort"),
        attribution=row.pop("attribution"),
        topicTagsLinks=db_topic_tags,
        faqs=db_faqs,
    )

    variableMetadata = dict(
        **_omit_nullable_values(variable),
        nonRedistributable=bool(nonRedistributable),
        display=display,
        schemaVersion=schemaVersion,
        processingLevel=processingLevel,
        presentation=_omit_nullable_values(presentation),
        license=license,
        descriptionKey=descriptionKey,
    )

    assert variableMetadata["type"], "type must be set"

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

    # Create dimensions
    variableMetadata["dimensions"] = {
        "years": {"values": yearArray},
        "entities": {"values": entityArray},
    }
    # Add values for ordinal variables
    if sort:
        dim_values = variableMetadata["dimensions"].get("values", {})
        dim_values["values"] = [{"id": i, "name": v} for i, v in enumerate(sort)]
        variableMetadata["dimensions"]["values"] = dim_values

    # convert timestamp to string
    time_format = "%Y-%m-%dT%H:%M:%S.000Z"
    for col in ("createdAt", "updatedAt"):
        variableMetadata[col] = variableMetadata[col].strftime(time_format)

    # add origins
    variableMetadata["origins"] = _move_population_origin_to_end(
        [_omit_nullable_values(d) for d in db_origins_df.to_dict(orient="records")]
    )

    return variableMetadata


def _move_population_origin_to_end(origins: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Move population origin to the end of the list of origins. This way it gets displayed last on data page."""
    new_origins = []
    pop_origin = None
    for origin in origins:
        if origin.get("title") == "Population" and origin.get("producer") == "Various sources":
            pop_origin = origin
        else:
            new_origins.append(origin)
    if pop_origin:
        new_origins.append(pop_origin)
    return new_origins


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


def _convert_strings_to_numeric(lst: List[str]) -> List[Union[int, float, str]]:
    """Convert strings to numeric values. String `nan` remains as string."""
    result = []
    for item in lst:
        assert isinstance(item, str)
        if item.lower() == "nan":
            num = item
        else:
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
