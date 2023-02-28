import json
import os
from functools import cache
from pathlib import Path
from typing import Any, Dict, cast

import pandas as pd
import requests
from owid import catalog
from owid.catalog import Dataset, Source, Table, VariableMeta
from owid.catalog.utils import underscore
from owid.walden import Catalog
from owid.walden import Dataset as WaldenDataset
from structlog import get_logger

from etl import grapher_helpers as gh
from etl.paths import DATA_DIR

log = get_logger()

CURRENT_DIR = Path(__file__).parent

BASE_URL = "https://unstats.un.org/sdgapi"
VERSION = Path(__file__).parent.stem
FNAME = Path(__file__).stem
NAMESPACE = Path(__file__).parent.parent.stem


def run(dest_dir: str) -> None:
    garden_dataset = catalog.Dataset(DATA_DIR / f"garden/{NAMESPACE}/{VERSION}/{FNAME}")
    dataset = catalog.Dataset.create_empty(dest_dir, garden_dataset.metadata)
    dataset.save()

    # add tables to dataset
    clean_source_map = load_clean_source_mapping()
    # NOTE: we renamed namespace from un_sdg to un, but we still use old walden
    walden_ds = Catalog().find_one(namespace="un_sdg", short_name=FNAME, version=VERSION)
    ds_garden = Dataset((DATA_DIR / f"garden/{NAMESPACE}/{VERSION}/{FNAME}").as_posix())
    sdg_tables = ds_garden.table_names
    for var in sdg_tables:
        var_df = create_dataframe_with_variable_name(ds_garden, var)
        var_df["source"] = clean_source_name(var_df["source"], clean_source_map)

        var_gr = var_df.groupby("variable_name")

        for var_name, df_var in var_gr:
            df_tab = add_metadata_and_prepare_for_grapher(df_var, walden_ds)
            df_tab.metadata.dataset = dataset.metadata

            # NOTE: long format is quite inefficient, we're creating a table for every variable
            # converting it to wide format would be too sparse, but we could move dimensions from
            # variable names to proper dimensions
            # currently we generate ~10000 files with total size 73MB (grapher step runs in 692s
            # and both reindex and publishing is fast, so this is not a real bottleneck besides
            # polluting `grapher` channel in our catalog)
            # see https://github.com/owid/etl/issues/447
            for wide_table in gh.long_to_wide_tables(df_tab):
                # table is generated for every column, use it as a table name
                # shorten it under 255 characteres as this is the limit for file name
                wide_table.metadata.short_name = wide_table.columns[0][:245]
                dataset.add(wide_table)


def clean_source_name(raw_source: pd.Series, clean_source_map: Dict[str, str]) -> str:
    if len(raw_source.drop_duplicates()) > 1:
        clean_source = "Data from multiple sources compiled by the UN"
    else:
        source_name = raw_source.drop_duplicates().iloc[0]
        assert source_name in clean_source_map, f"{source_name} not in un_sdg.sources.json - please add"
        clean_source = clean_source_map[source_name]

    return clean_source


def add_metadata_and_prepare_for_grapher(df_gr: pd.DataFrame, walden_ds: WaldenDataset) -> Table:
    indicator = df_gr["variable_name"].iloc[0].split("-")[0].strip()
    source_url = get_metadata_link(indicator)
    log.info(
        "Getting the metadata url...",
        url=source_url,
        indicator=indicator,
        var_name=df_gr["variable_name"].iloc[0],
    )
    source = Source(
        name=df_gr["source"].iloc[0],
        url=walden_ds.metadata["url"],
        source_data_url=walden_ds.metadata.get("source_data_url"),
        owid_data_url=walden_ds.metadata["owid_data_url"],
        date_accessed=walden_ds.metadata["date_accessed"],
        publication_date=walden_ds.metadata["publication_date"],
        publication_year=walden_ds.metadata["publication_year"],
        published_by=walden_ds.metadata["name"],
        publisher_source=df_gr["source"].iloc[0],
    )

    df_gr["meta"] = VariableMeta(
        title=df_gr["variable_name_meta"].iloc[0],
        description=df_gr["seriesdescription"].iloc[0] + "\n\nFurther information available at: %s" % (source_url),
        sources=[source],
        unit=df_gr["long_unit"].iloc[0],
        short_unit=df_gr["short_unit"].iloc[0],
        additional_info=None,
    )
    # Taking only the first 255 characters of the var name as this is the limit (there is at least one that is too long)
    df_gr["variable"] = underscore(df_gr["variable_name"].iloc[0][0:254])

    df_gr = df_gr[["country", "year", "value", "variable", "meta"]].copy()
    # convert integer values to int but round float to 2 decimal places, string remain as string
    df_gr["value"] = df_gr["value"].apply(value_convert)
    df_gr = df_gr.set_index(["year", "country"])

    return Table(df_gr)


def create_dataframe_with_variable_name(dataset: Dataset, tab: str) -> pd.DataFrame:

    cols_keep = [
        "country",
        "year",
        "seriescode",
        "seriesdescription",
        "variable_name",
        "variable_name_meta",
        "value",
        "source",
        "long_unit",
        "short_unit",
    ]

    tab_df = pd.DataFrame(dataset[tab]).reset_index()
    cols_meta = ["indicator", "seriesdescription", "seriescode"]
    cols = ["indicator", "seriescode"]
    if tab_df.shape[1] > 11:
        col_list = sorted(tab_df.columns.to_list())
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
        cols_meta_dim = cols_meta + dim_cols
        cols_dim = cols + dim_cols
        tab_df["variable_name_meta"] = tab_df[cols_meta_dim].agg(" - ".join, axis=1)
        tab_df["variable_name"] = tab_df[cols_dim].agg(" - ".join, axis=1)
        tab_df = tab_df[cols_keep]
        tab_df["seriescode"] = tab_df["seriescode"].str.lower()
    else:
        tab_df["variable_name_meta"] = tab_df[cols_meta].agg(" - ".join, axis=1)
        tab_df["variable_name"] = tab_df[cols].agg(" - ".join, axis=1)
        tab_df = tab_df[cols_keep]
        tab_df["seriescode"] = tab_df["seriescode"].str.lower()

    return tab_df


def load_clean_source_mapping() -> Dict[str, str]:
    with open(CURRENT_DIR / "un_sdg.sources.json", "r") as f:
        sources = json.load(f)
        return cast(Dict[str, str], sources)


@cache
def get_metadata_link(indicator: str) -> str:

    url = os.path.join("https://unstats.un.org/sdgs/metadata/files/", "Metadata-%s.pdf") % "-".join(
        [part.rjust(2, "0") for part in indicator.split(".")]
    )
    r = requests.head(url)
    ctype = r.headers["Content-Type"]
    if ctype == "application/pdf":
        url_out = url
    elif ctype == "text/html":
        url_a = os.path.join("https://unstats.un.org/sdgs/metadata/files/", "Metadata-%sa.pdf") % "-".join(
            [part.rjust(2, "0") for part in indicator.split(".")]
        )
        url_b = os.path.join("https://unstats.un.org/sdgs/metadata/files/", "Metadata-%sb.pdf") % "-".join(
            [part.rjust(2, "0") for part in indicator.split(".")]
        )
        url_out = url_a + " and " + url_b
        url_check = requests.head(url_a)
        ctype_a = url_check.headers["Content-Type"]
        assert ctype_a == "application/pdf", url_a + "does not link to a pdf"
    else:
        raise NotImplementedError()

    return url_out


def value_convert(value: Any) -> Any:
    if isinstance(value, float) or isinstance(value, int):
        if int(value) == value:
            return int(value)
        if float(value) == value:
            value = round(value, 2)
            return value
    else:
        return value
