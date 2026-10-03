"""Load a garden dataset and create a grapher dataset."""

import dataclasses
import json
import os
import re
from functools import cache
from typing import Any, cast

import pandas as pd
import requests
from owid.catalog import Dataset, Origin, Table, VariableMeta
from owid.catalog.core.warnings import DisplayNameWarning, NoOriginsWarning, ignore_warnings
from owid.catalog.utils import underscore
from structlog import getLogger

from etl.grapher import helpers as gh
from etl.helpers import PathFinder, create_dataset

log = getLogger()
# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


# only include tables containing INCLUDE string, this is useful for debugging
# but should be None before merging to master!!
# TODO: set this to None before merging to master
# INCLUDE = "_6_1_1|_6_2_1"
INCLUDE = None


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("un_sdg")

    # Pull the snapshot-level origin off the `value` column of the first garden
    # table; the garden step re-attaches it after the legacy DataFrame cast.
    base_origin = ds_garden[ds_garden.table_names[0]]["value"].metadata.origins[0]

    # Add table of processed data to the new dataset.
    # add tables to dataset
    clean_source_map = load_clean_source_mapping()

    all_tables = []

    for var in ds_garden.table_names:
        if INCLUDE and not re.search(INCLUDE, var):
            log.warning("un_sdg.skip", table_name=var)
            continue

        log.debug("un_sdg.process", table_name=var)

        var_df = create_dataframe_with_variable_name(ds_garden, var)
        var_df["source"] = clean_source_name(var_df["source"], clean_source_map)
        var_gr = var_df.groupby("variable_name")
        source_desc = load_source_description()
        for var_name, df_var in var_gr:
            df_tab = add_metadata_and_prepare_for_grapher(df_var, base_origin, source_desc)

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

                # ds_grapher.add(wide_table)
                all_tables.append(wide_table)

    #
    # Save outputs.
    #
    with ignore_warnings([NoOriginsWarning, DisplayNameWarning]):
        ds_grapher = create_dataset(dest_dir, tables=all_tables, default_metadata=ds_garden.metadata)
        ds_grapher.save()


def clean_source_name(raw_source: pd.Series, clean_source_map: dict[str, str]) -> str:
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
    with open(paths.directory / "un_sdg.source_description.json") as f:
        sources = json.load(f)
        return cast(dict[str, str], sources)


def create_metadata_desc(indicator: str, series_code: str, source_desc: dict, series_description: str) -> str:
    """
    If series code is in the source description json then combine it with the string showing the metadata url.
    If it's not in the source description json just return the metadata url.
    """
    if series_code in list(source_desc):
        source_desc_out = source_desc[series_code]
    else:
        source_url = get_metadata_link(indicator)
        if source_url == "no metadata found":
            source_desc_out = series_description
        else:
            source_desc_out = series_description + f"\n\nFurther information available at: {source_url}"

    return source_desc_out


def add_metadata_and_prepare_for_grapher(df_gr: pd.DataFrame, base_origin: Origin, source_desc: dict) -> Table:
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
    df_gr = Table(df_gr, short_name=df_gr["variable_name"].iloc[0])

    # Per-variable origin: keep the snapshot's UN-level metadata, but override `producer`
    # with the cleaned underlying-source name (FAO, WHO, …) so each chart credits the
    # reporting agency rather than blanket "United Nations".
    origin = dataclasses.replace(base_origin, producer=df_gr["source"].iloc[0])

    df_gr["meta"] = VariableMeta(
        title=df_gr["variable_name_meta"].iloc[0],
        description=source_desc_out,
        origins=[origin],
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


def load_clean_source_mapping() -> dict[str, str]:
    """
    Load the existing json which maps the raw sources to a cleaner version of the sources.
    """
    with open(paths.directory / "un_sdg.sources.json") as f:
        sources = json.load(f)
        return cast(dict[str, str], sources)


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
    url_a = os.path.join("https://unstats.un.org/sdgs/metadata/files/", "Metadata-%sa.pdf") % "-".join(
        [part.rjust(2, "0") for part in indicator.split(".")]
    )
    url_b = os.path.join("https://unstats.un.org/sdgs/metadata/files/", "Metadata-%sb.pdf") % "-".join(
        [part.rjust(2, "0") for part in indicator.split(".")]
    )
    r = requests.head(url)
    if r.status_code == 200:
        url_out = url
        return url_out
    else:
        url_check_a = requests.head(url_a)
        url_check_b = requests.head(url_b)
        if url_check_a.status_code == 200 and url_check_b.status_code == 200:
            url_out = url_a + " and " + url_b
            return url_out
        else:
            return "no metadata found"


def value_convert(value: Any) -> Any:
    if isinstance(value, float) or isinstance(value, int):
        if int(value) == value:
            return int(value)
        if float(value) == value:
            value = round(value, 2)
            return value
    else:
        return value
