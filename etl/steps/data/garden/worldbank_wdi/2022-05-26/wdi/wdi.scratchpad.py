"""Scratchpad for peripheral checks related to the World Development
Indicators bulk update.

The code in this file is not intended to be run as part of any etl step.
"""
# flake8: noqa

import json
import os
from pathlib import Path
from typing import List

import pandas as pd
import requests
from dotenv import load_dotenv
from owid.catalog import Dataset

from etl.db import get_connection
from etl.paths import DATA_DIR

raise NotImplementedError("This script is not intended to be executed.")

# -------------------------------------------- #
# examine the extent to which countries in wdi.country_exclude.json are used
# in existing World Development Indicator charts.
q = """
    WITH
    datasets AS (
        SELECT
            id
        FROM datasets
        WHERE name="World Development Indicators - World Bank (2021.07.30)"
    ),
    variables AS (
        SELECT id
        FROM variables
        WHERE datasetId IN (SELECT id FROM datasets)
    ),
    entityIds AS (
        SELECT
            entityId
        FROM data_values
        WHERE variableId IN (SELECT id from variables)
    )

    SELECT *
    FROM entities
    WHERE id IN (SELECT entityId from entityIds)
"""
df = pd.read_sql(q, get_connection())
uniq_entities = df["name"].unique().tolist()

q = """
    WITH
    datasets AS (
        SELECT
            id
        FROM datasets
        WHERE name="World Development Indicators - World Bank (2021.07.30)"
    ),
    variables AS (
        SELECT id
        FROM variables
        WHERE datasetId IN (SELECT id FROM datasets)
    ),
    chart_dims AS (
        SELECT chartId
        FROM chart_dimensions
        WHERE variableId in (SELECT id FROM variables)
    ),
    charts AS (
        SELECT *
        FROM charts
        WHERE id in (SELECT chartId from chart_dims)
    )

    SELECT
        id,
        config -> "$.selectedEntityNames" AS selectedEntityNames
    FROM charts
"""
df = pd.read_sql(q, get_connection())
df["selectedEntityNames"] = df["selectedEntityNames"].apply(lambda x: json.loads(x) if pd.notnull(x) else [])
df_dummies = pd.get_dummies(df["selectedEntityNames"].apply(pd.Series).stack()).sum(level=0)
df_counts = (
    df_dummies.sum(axis=0)
    .reindex(uniq_entities)
    .fillna(0)
    .sort_index()
    .reset_index()
    .rename(columns={"index": "country", 0: "count"})
)

df_counts.query("count == 0")

with pd.option_context("display.max_rows", None, "display.width", None):
    df_counts.sort_values(by="count", ascending=False)


version = Path(__file__).parent.stem
fname = Path(__file__).stem.split(".")[0]
ds = Dataset((DATA_DIR / f"meadow/worldbank_wdi/{version}/{fname}").as_posix())


def load_excluded_countries() -> List[str]:
    fname = Path(__file__).stem.split(".")[0]
    with open(Path(__file__).parent / f"{fname}.country_exclude.json", "r") as f:
        data = json.load(f)
    return data  # type: ignore


excluded_countries = load_excluded_countries()
q = f"""
    WITH
    datasets AS (
        SELECT
            id
        FROM datasets
        WHERE name="World Development Indicators - World Bank (2021.07.30)"
    ),
    variables AS (
        SELECT id
        FROM variables
        WHERE datasetId IN (SELECT id FROM datasets)
    ),
    chart_dims AS (
        SELECT chartId
        FROM chart_dimensions
        WHERE variableId in (SELECT id FROM variables)
    )

    SELECT
        jt.*
    FROM charts,
        JSON_TABLE(
        charts.config,
        '$' COLUMNS(
                chartId INT PATH '$.id',
                NESTED PATH '$.selectedEntityNames[*]' COLUMNS (entityName VARCHAR(255) PATH '$')
                )
        ) AS jt
    WHERE
        id in (SELECT chartId from chart_dims)
        AND jt.entityName IN {tuple(excluded_countries)}
"""
df = pd.read_sql(q, get_connection())
df

# -------------------------------------------- #


# -------------------------------------------- #
# reject pending/flagged suggested chart revisions from a previous dataset version.

load_dotenv(override=True)

CLOUDFLARE_SESSION_ID = os.environ["CLOUDFLARE_SESSION_ID"]
OWID_HOST = os.environ["OWID_HOST"]

suggested_reason = "World Development Indicators (v2021.07.30) bulk dataset update"
q = f"""
    SELECT *
    FROM suggested_chart_revisions
    WHERE (status = "pending" OR status = "flagged")
        AND suggestedReason = "{suggested_reason}"
"""
df_revs = pd.read_sql(q, get_connection())
assert df_revs.shape[0] > 0

for _, rev in df_revs.iterrows():
    resp = requests.post(
        os.path.join(OWID_HOST, f"suggested-chart-revisions/{rev['id']}/update"),
        json={
            "status": "rejected",
            "decisionReason": rev["decisionReason"],
        },
        headers={"cookie": f"sessionid={CLOUDFLARE_SESSION_ID}"},
    )
    result = json.loads(resp.content)
    if resp.ok and result["success"]:
        print(f"Rejected revision {rev['id']} for chart {rev['chartId']}")
    else:
        print(f"Failed to reject revision {rev['id']} for chart {rev['chartId']}")

df_revs2 = pd.read_sql(q, get_connection())
assert df_revs2.shape[0] == 0

# -------------------------------------------- #
