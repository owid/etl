"""Load a garden dataset and create a grapher dataset."""
import json
import os
from functools import cache
from typing import Any, Dict, cast

import pandas as pd
import requests
from owid.catalog import Dataset, Source, Table, VariableMeta
from owid.catalog.utils import underscore
from structlog import getLogger

from etl import grapher_helpers as gh
from etl.helpers import PathFinder

log = getLogger()
# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden: Dataset = paths.load_dependency("un_sdg")

    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = Dataset.create_empty(dest_dir, ds_garden.metadata)

    # Add table of processed data to the new dataset.
    # add tables to dataset
    clean_source_map = load_clean_source_mapping()
    for var in ds_garden.table_names:
        var_df = create_dataframe_with_variable_name(ds_garden, var)
        var_df["source"] = clean_source_name(var_df["source"], clean_source_map)
        var_gr = var_df.groupby("variable_name")
        source_desc = load_source_description()
        for var_name, df_var in var_gr:
            df_tab = add_metadata_and_prepare_for_grapher(df_var, ds_garden, source_desc)
            df_tab.metadata.dataset = ds_grapher.metadata

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
                ds_grapher.add(wide_table)

    # Save changes in the new grapher dataset.
    ds_grapher.save()


def clean_source_name(raw_source: pd.Series, clean_source_map: Dict[str, str]) -> str:
    if len(raw_source.drop_duplicates()) > 1:
        clean_source = "Data from multiple sources compiled by the UN"
    else:
        source_name = raw_source.drop_duplicates().iloc[0]
        assert source_name in clean_source_map, f"{source_name} not in un_sdg.sources.json - please add"
        clean_source = clean_source_map[source_name]

    return clean_source


def load_source_description() -> dict:
    """
    Load the existing json which loads a more detailed source description for a selection of sources.
    """
    with open(paths.directory / "un_sdg.source_description.json", "r") as f:
        sources = json.load(f)
        return cast(Dict[str, str], sources)


def create_metadata_desc(indicator: str, series_code: str, source_desc: dict, series_description: str) -> str:
    """
    If series code is in the source description json then combine it with the string showing the metadata url.
    If it's not in the source description json just return the metadata url.
    """
    if series_code in list(source_desc):
        source_desc_out = source_desc[series_code]
    else:
        source_url = get_metadata_link(indicator)
        source_desc_out = series_description + "\n\nFurther information available at: %s" % (source_url)

    return source_desc_out


def add_metadata_and_prepare_for_grapher(df_gr: pd.DataFrame, ds_garden: Dataset, source_desc: dict) -> Table:
    """
    Adding variable name specific metadata - there is an option to add more detailed metadata in the un_sdg.source_description.json
    but the default option is to link out to the metadata pdfs provided by the UN.

    We add the variable name by taking the first 256 characters - some variable names are very long!
    """

    indicator = df_gr["variable_name"].iloc[0].split("-")[0].strip()
    series_code = df_gr["seriescode"].iloc[0].upper()
    series_description = df_gr["seriesdescription"].iloc[0]
    source_desc_out = create_metadata_desc(
        indicator=indicator, series_code=series_code, source_desc=source_desc, series_description=series_description
    )
    log.info(
        "Creating metadata...",
        indicator=indicator,
        var_name=df_gr["variable_name"].iloc[0],
    )
    df_gr = Table(df_gr, short_name=df_gr["variable_name"].iloc[0])

    source = Source(
        name=df_gr["source"].iloc[0],
        url=ds_garden.metadata.sources[0].url,
        source_data_url=ds_garden.metadata.sources[0].source_data_url,
        owid_data_url=ds_garden.metadata.sources[0].owid_data_url,
        date_accessed=ds_garden.metadata.sources[0].date_accessed,
        publication_date=ds_garden.metadata.sources[0].publication_date,
        publication_year=ds_garden.metadata.sources[0].publication_year,
        published_by=ds_garden.metadata.sources[0].published_by,
        publisher_source=df_gr["source"].iloc[0],
    )

    df_gr["meta"] = VariableMeta(
        title=df_gr["variable_name_meta"].iloc[0],
        description=source_desc_out,
        sources=[source],
        unit=df_gr["long_unit"].iloc[0].lower(),
        short_unit=df_gr["short_unit"].iloc[0],
        additional_info=None,
    )

    df_gr["variable"] = underscore(df_gr["variable_name"].iloc[0][0:254])

    df_gr = df_gr[["country", "year", "value", "variable", "meta"]].copy()
    # convert integer values to int but round float to 2 decimal places, string remain as string
    df_gr["value"] = df_gr["value"].apply(value_convert)
    df_gr = df_gr.set_index(["year", "country"])

    return df_gr


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
    """
    Load the existing json which maps the raw sources to a cleaner version of the sources.
    """
    with open(paths.directory / "un_sdg.sources.json", "r") as f:
        sources = json.load(f)
        return cast(Dict[str, str], sources)


@cache
def get_metadata_link(indicator: str) -> str:

    """
    This fetches the link to the metadata pdf. Firstly it tests if the standard expected link works e.g.:
    https://unstats.un.org/sdgs/metadata/files/Metadata-10-01-01.pdf

    If it doesn't it tests if the expected alternative links exist e.g.:

    https://unstats.un.org/sdgs/metadata/files/Metadata-10-01-01a.pdf
    &
    https://unstats.un.org/sdgs/metadata/files/Metadata-10-01-01b.pdf

    """

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
