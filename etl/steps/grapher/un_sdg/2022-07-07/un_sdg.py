import json
import os
import requests
from typing import Iterable
from pathlib import Path
import pandas as pd

from structlog import get_logger
from owid.catalog import Dataset, Table, Source, VariableMeta
from owid.catalog.utils import underscore
from owid.walden import Catalog
from etl.paths import DATA_DIR
from etl import grapher_helpers as gh


log = get_logger()


BASE_URL = "https://unstats.un.org/sdgapi"
VERSION = Path(__file__).parent.stem
FNAME = Path(__file__).stem
NAMESPACE = Path(__file__).parent.parent.stem


def get_grapher_dataset() -> Dataset:
    dataset = Dataset(DATA_DIR / f"garden/{NAMESPACE}/{VERSION}/{FNAME}")
    # short_name should include dataset name and version
    dataset.metadata.short_name = (
        f"{dataset.metadata.short_name}__{VERSION.replace('-', '_')}"
    )
    return dataset


def get_grapher_tables(dataset: Dataset) -> Iterable[Table]:
    clean_source_map = load_clean_source_mapping()
    walden_ds = Catalog().find_one(
        namespace=NAMESPACE, short_name=FNAME, version=VERSION
    )
    ds_garden = Dataset((DATA_DIR / f"garden/{NAMESPACE}/{VERSION}/{FNAME}").as_posix())
    sdg_tables = ds_garden.table_names
    for var in sdg_tables:
        var_df = create_dataframe_with_variable_name(ds_garden, var)
        var_df["source"] = clean_source_name(var_df["source"], clean_source_map)
        var_gr = var_df.groupby("variable_name")
        for var_name, df_var in var_gr:
            print(var_name)
            df_tab = add_metadata_and_prepare_for_grapher(df_var, walden_ds)
            yield from gh.yield_long_table(df_tab)


def clean_source_name(raw_source: pd.Series, clean_source_map: dict) -> pd.Series:
    if len(raw_source.drop_duplicates()) > 1:
        clean_source = "Data from multiple sources compiled by the UN"
    else:
        source_name = raw_source.drop_duplicates().iloc[0]
        assert (
            source_name in clean_source_map
        ), f"{source_name} not in un_sdg.sources.json - please add"
        clean_source = clean_source_map[source_name]

    return clean_source


def add_metadata_and_prepare_for_grapher(
    df_gr: pd.DataFrame, walden_ds: Dataset
) -> Table:

    indicator = df_gr["variable_name"].iloc[0].split("-")[0].strip()
    source_url = get_metadata_link(indicator)
    log.info(
        "Getting the metadata url...",
        url=source_url,
        indicator=indicator,
        var_name=df_gr["variable_name"].iloc[0][0:10],
    )
    source = Source(
        name=df_gr["source"].iloc[0],
        description="Metadata available at: %s" % (source_url),
        url=walden_ds.metadata["url"],
        source_data_url=walden_ds.metadata["source_data_url"],
        owid_data_url=walden_ds.metadata["owid_data_url"],
        date_accessed=walden_ds.metadata["date_accessed"],
        publication_date=walden_ds.metadata["publication_date"],
        publication_year=walden_ds.metadata["publication_year"],
        published_by=walden_ds.metadata["name"],
        publisher_source=df_gr["source"].iloc[0],
    )
    assert (
        "-".join([part.rjust(2, "0") for part in indicator.split(".")])
        in source.description
    )

    df_gr["meta"] = VariableMeta(
        title=df_gr["variable_name"].iloc[0],
        description=df_gr["seriesdescription"].iloc[0],
        sources=[source],
        unit=df_gr["long_unit"].iloc[0],
        short_unit=df_gr["short_unit"].iloc[0],
        additional_info=None,
    )

    # 12.3.1 - Food waste (Tonnes) - AG_FOOD_WST - Households
    # would become
    # _12_3_1__food_waste__tonnes__ag_food_wst__households
    # maybe we'd want to remove the leading indicator?
    df_gr["variable"] = underscore(df_gr["variable_name"].iloc[0])

    df_gr = df_gr[["country", "year", "value", "variable", "meta"]].copy()
    # convert integer values to int but round float to 2 decimal places, string remain as string
    df_gr["value"] = df_gr["value"].apply(value_convert)
    df_gr["entity_id"] = gh.country_to_entity_id(df_gr["country"], create_entities=True)
    df_gr = df_gr.drop(columns=["country"]).set_index(["year", "entity_id"])

    return Table(df_gr)


def create_dataframe_with_variable_name(dataset: Dataset, tab: str) -> pd.DataFrame:

    cols_keep = [
        "country",
        "year",
        "seriescode",
        "seriesdescription",
        "variable_name",
        "value",
        "source",
        "long_unit",
        "short_unit",
    ]

    tab_df = pd.DataFrame(dataset[tab]).reset_index()
    cols = ["indicator", "seriesdescription", "seriescode"]
    if tab_df.shape[1] > 11:
        col_list = tab_df.columns.to_list()
        drop_cols = [
            "country",
            "year",
            "goal",
            "target",
            "indicator",
            "seriescode",
            "seriesdescription",
            "value",
            "source",
            "long_unit",
            "short_unit",
            "variable_name",
        ]
        dim_cols = [x for x in col_list if x not in drop_cols]
        cols_dim = cols + dim_cols
        tab_df["variable_name"] = tab_df[cols_dim].agg(" - ".join, axis=1)
        tab_df = tab_df[cols_keep]
        tab_df["seriescode"] = tab_df["seriescode"].str.lower()
    else:
        tab_df["variable_name"] = tab_df[cols].agg(" - ".join, axis=1)
        tab_df = tab_df[cols_keep]
        tab_df["seriescode"] = tab_df["seriescode"].str.lower()

    return tab_df


def load_clean_source_mapping() -> dict:
    with open("etl/steps/grapher/un_sdg/2022-07-07/un_sdg.sources.json", "r") as f:
        sources = json.load(f)
        return sources


def get_metadata_link(indicator: str) -> None:

    url = os.path.join(
        "https://unstats.un.org/sdgs/metadata/files/", "Metadata-%s.pdf"
    ) % "-".join([part.rjust(2, "0") for part in indicator.split(".")])
    r = requests.head(url)
    ctype = r.headers["Content-Type"]
    if ctype == "application/pdf":
        url_out = url
    elif ctype == "text/html":
        url_a = os.path.join(
            "https://unstats.un.org/sdgs/metadata/files/", "Metadata-%sa.pdf"
        ) % "-".join([part.rjust(2, "0") for part in indicator.split(".")])
        url_b = os.path.join(
            "https://unstats.un.org/sdgs/metadata/files/", "Metadata-%sb.pdf"
        ) % "-".join([part.rjust(2, "0") for part in indicator.split(".")])
        url_out = url_a + " and " + url_b
        url_check = requests.head(url_a)
        ctype_a = url_check.headers["Content-Type"]
        assert ctype_a == "application/pdf", url_a + "does not link to a pdf"
    return url_out


def value_convert(value):
    if isinstance(value, float) or isinstance(value, int):
        if int(value) == value:
            return int(value)
        if float(value) == value:
            value = round(value, 2)
            return value
    else:
        return value
