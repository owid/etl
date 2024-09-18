import base64
import datetime as dt
import json
from typing import Literal

import pandas as pd
import requests
from dateutil.parser import parse as date_parse

from etl.db import get_connection


def get_thumbnail_url(grapher_url: str) -> str:
    """
    Turn https://ourworldindata.org/grapher/life-expectancy?country=~CHN"
    Into https://ourworldindata.org/grapher/thumbnail/life-expectancy.png?country=~CHN
    """
    assert grapher_url.startswith("https://ourworldindata.org/grapher/")
    parts = parse.urlparse(grapher_url)

    return f"{parts.scheme}://{parts.netloc}/grapher/thumbnail/{Path(parts.path).name}.png?{parts.query}"


def get_grapher_thumbnail(grapher_url: str) -> str:
    url = get_thumbnail_url(grapher_url)
    data = requests.get(url).content
    return f"data:image/png;base64,{base64.b64encode(data).decode('utf8')}"


def fetch_chart_data(conn, slug: str) -> pd.DataFrame:
    # Use the DB for as much as we can, and the API just for data and metadata
    config = fetch_config(conn, slug)
    return fetch_data(conn, config)


def list_charts(conn) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT slug
            FROM chart_configs
            WHERE
                JSON_EXTRACT(full, '$.isPublished')
                AND slug IS NOT NULL
            ORDER BY slug
            """
        )
        return [slug for (slug,) in cur.fetchall()]


def fetch_config(conn, slug: str) -> dict:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT full
            FROM chart_configs
            WHERE
                slug = %s
                AND JSON_EXTRACT(full, '$.isPublished')
            """,
            (slug,),
        )
        config = json.loads(cur.fetchone()[0])
        if config is None:
            raise ValueError(f"No published chart with slug {slug}")

        return config


def fetch_data(conn, config: dict) -> pd.DataFrame:
    dimensions = set(d["variableId"] for d in config["dimensions"])
    bundle = {dim: _fetch_dimension(dim) for dim in dimensions}
    df = _bundle_to_frame(config, bundle)
    return df


def _indicator_to_frame(indicator: dict) -> pd.DataFrame:
    data = indicator["data"]
    metadata = indicator["metadata"]

    # getting a data frame is easy
    df = pd.DataFrame.from_dict(data)

    # turning entity ids into entity names
    entities = pd.DataFrame.from_records(metadata["dimensions"]["entities"]["values"])
    id_to_name = entities.set_index("id").name.to_dict()
    df["entities"] = df.entities.apply(id_to_name.__getitem__)

    # make the "values" column more interestingly named
    short_name = metadata.get("shortName", f'_{metadata["id"]}')
    df = df.rename(columns={"values": short_name})

    time_col = _detect_time_col_type(metadata)
    if time_col == "dates":
        df["years"] = _convert_years_to_dates(metadata, df["years"])

    # order the columns better
    cols = ["entities", "years"] + sorted(df.columns.difference(["entities", "years"]))
    df = df[cols]

    return df


def _detect_time_col_type(metadata) -> Literal["dates", "years"]:
    if metadata.get("display", {}).get("yearIsDay"):
        return "dates"

    return "years"


def _convert_years_to_dates(metadata, years):
    base_date = date_parse(metadata["display"]["zeroDay"])
    return years.apply(lambda y: base_date + dt.timedelta(days=y))


def _fetch_dimension(id: int) -> dict:
    data = requests.get(f"https://api.ourworldindata.org/v1/indicators/{id}.data.json").json()
    metadata = requests.get(f"https://api.ourworldindata.org/v1/indicators/{id}.metadata.json").json()
    return {"data": data, "metadata": metadata}


def _bundle_to_frame(config, bundle) -> pd.DataFrame:
    # combine all the indicators into a single data frame and one metadata dict
    metadata = {}
    df = None
    for dim in bundle.values():
        to_merge = _indicator_to_frame(dim)
        (value_col,) = to_merge.columns.difference(["entities", "years"])
        metadata[value_col] = dim["metadata"].copy()

        if df is None:
            df = to_merge
        else:
            df = pd.merge(df, to_merge, how="outer", on=["entities", "years"])

    assert df is not None

    # save some useful metadata onto the frame
    assert config
    slug = config["slug"]
    df.attrs["slug"] = slug
    df.attrs["url"] = f"https://ourworldindata.org/grapher/{slug}"
    df.attrs["metadata"] = metadata
    df.attrs["config"] = config

    # if there is only one indicator, we can use the slug as the column name
    if len(df.columns) == 3:
        assert config
        (value_col,) = df.columns.difference(["entities", "years"])
        short_name = slug.replace("-", "_")
        df = df.rename(columns={value_col: short_name})
        df.attrs["metadata"][short_name] = df.attrs["metadata"].pop(value_col)
        df.attrs["value_col"] = short_name

    # we kept using "years" until now to keep the code paths the same, but they could
    # be dates
    if df["years"].astype(str).str.match(r"^\d{4}-\d{2}-\d{2}$").all():
        df = df.rename(columns={"years": "dates"})

    return df
