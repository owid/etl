"""Load a garden dataset and create a grapher dataset."""

import json
import os
import re
from functools import cache
from typing import Any, Dict, cast

import pandas as pd
import requests
from owid.catalog import Dataset, License, Origin, Table, VariableMeta
from owid.catalog.utils import underscore
from structlog import getLogger

from etl.grapher import helpers as gh
from etl.helpers import PathFinder, create_dataset

log = getLogger()
# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

pd.options.mode.chained_assignment = None


# only include tables containing INCLUDE string, this is useful for debugging
# but should be None before merging to master!!
# TODO: set this to None before merging to master
#INCLUDE = "_6_1_1|_6_2_1|_16|_2_4"
INCLUDE = None


# for origins
DATE_ACCESSED = "2024-08-27"
CURRENT_YEAR = 2024


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("un_sdg")

    # Add table of processed data to the new dataset.
    # add tables to dataset
    all_tables = []

    for var in ds_garden.table_names:
        if INCLUDE and not re.search(INCLUDE, var):
            log.warning("un_sdg.skip", table_name=var)
            continue

        log.info("un_sdg.process", table_name=var)

        tb = ds_garden.read(var, safe_types=False)

        tb = create_table(tb)

        # clean source name - this uses source column and hardcoded mapping in un_sdg.sources.json/ un_sdg.sources_additional.json - check whether this mapping is still up to date here: https://unstats.un.org/sdgs/iaeg-sdgs/tier-classification/.
        tb["source_producer"] = clean_source_name(tb, load_clean_source_mapping(), load_additional_source_mapping())

        # add short attribution, this uses mapping in un_sdg.sources_short.json
        tb["attribution_short"] = add_short_source_name(tb["source_producer"], load_short_source_mapping())

        # add title of data product where applicable, otherwise this defaults to "multiple sources"
        tb["source_title"] = get_source(tb["source"])

        tb_var_gr = tb.groupby("variable_name")

        source_desc = load_source_description()

        for var_name, tb_var in tb_var_gr:
            tb_var = add_metadata_and_prepare_for_grapher(tb_var, ds_garden, source_desc)

            # NOTE: long format is quite inefficient, we're creating a table for every variable
            # converting it to wide format would be too sparse, but we could move dimensions from
            # variable names to proper dimensions
            # currently we generate ~10000 files with total size 73MB (grapher step runs in 692s
            # and both reindex and publishing is fast, so this is not a real bottleneck besides
            # polluting `grapher` channel in our catalog)
            # see https://github.com/owid/etl/issues/447
            for wide_table in gh.long_to_wide_tables(tb_var):
                # table is generated for every column, use it as a table name
                # shorten it under 255 characteres as this is the limit for file name
                wide_table.metadata.short_name = wide_table.columns[0][:245]

                # ds_grapher.add(wide_table)
                all_tables.append(wide_table)

    #
    # Save outputs.
    #
    ds_grapher = create_dataset(dest_dir, tables=all_tables, default_metadata=ds_garden.metadata)
    ds_grapher.save()


def clean_source_name(tb: Table, clean_source_map: Dict[str, str], additional_source_map: Dict[str, str]) -> str:
    unique_srcs = tb["source"].drop_duplicates()
    ind_code = tb["variable_name"].iloc[0].split("-")[0].strip()
    if len(unique_srcs) > 1:
        clean_source = additional_source_map[ind_code]
    else:
        source_name = unique_srcs.iloc[0].strip()
        assert source_name in clean_source_map, f"{repr(source_name)} not in un_sdg.sources.json - please add"
        clean_source = clean_source_map[source_name]

    return clean_source


def add_short_source_name(clean_source: pd.Series, short_source_map: Dict[str, str]) -> str:
    source_name = clean_source.iloc[0]
    assert source_name in short_source_map, f"{repr(source_name)} not in un_sdg.sources_short.json - please add"
    short_source = short_source_map[source_name]

    return short_source


def load_source_description() -> dict:
    """
    Load the existing json which loads a more detailed source description for a selection of sources.
    """
    with open(paths.directory / "un_sdg.source_description.json", "r") as f:
        sources = json.load(f)
        return cast(Dict[str, str], sources)


def get_source(raw_source: pd.Series) -> str:
    """Get source for origin title (not producer!) and citation. Lists up to 3 sources, more are combined into 'Multiple sources'."""
    sources = raw_source.drop_duplicates()
    if len(sources) == 1 and len(sources.iloc[0].strip()) <= 250:
        title = sources.iloc[0].strip()
    elif len(sources) <= 3 and len(", ".join([source_name.strip() for _, source_name in sources.items()])) <= 250:
        title = ", ".join([source_name.strip() for _, source_name in sources.items()])
    else:
        title = "Data from multiple sources"
    return title


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
            source_desc_out = series_description + "\n\nFurther information available at: %s" % (source_url)

    return source_desc_out


def add_metadata_and_prepare_for_grapher(tb: Table, ds_garden: Dataset, source_desc: dict) -> Table:
    """
    Adding variable name specific metadata - there is an option to add more detailed metadata in the un_sdg.source_description.json
    but the default option is to link out to the metadata pdfs provided by the UN.

    We add the variable name by taking the first 256 characters - some variable names are very long!
    """

    indicator = tb["variable_name"].iloc[0].split("-")[0].strip()
    series_code = tb["seriescode"].iloc[0].upper()
    series_description = tb["seriesdescription"].iloc[0]
    source_desc = create_metadata_desc(
        indicator=indicator, series_code=series_code, source_desc=source_desc, series_description=series_description
    )
    tb.short_name = tb["variable_name"].iloc[0]
    source_in_tb = tb["source_producer"].iloc[0]
    title_in_tb = get_source(tb["source_title"])

    # construct citation including link to metadata pdf
    metadata_link = get_metadata_link(indicator)
    citation_for_indicator = f"{source_in_tb} via UN SDG Indicators Database (https://unstats.un.org/sdgs/dataportal), UN Department of Economic and Social Affairs (accessed {CURRENT_YEAR})."
    if metadata_link != "no metadata found":
        citation_for_indicator += f" More information available at: {metadata_link}."

    origin = Origin(
        producer=source_in_tb,
        title=title_in_tb,
        description="The United Nations Sustainable Development Goal (SDG) dataset is the primary collection of data tracking progress towards the SDG indicators, compiled from officially-recognized international sources.",
        citation_full=citation_for_indicator,
        date_accessed=DATE_ACCESSED,
        url_main="https://unstats.un.org/sdgs/dataportal",
        url_download="https://unstats.un.org/sdgapi",
        attribution_short=tb["attribution_short"].iloc[0],
        license=License(name=f"© {CURRENT_YEAR} United Nations", url="https://www.un.org/en/about-us/terms-of-use"),
    )

    tb["meta"] = VariableMeta(
        title=tb["variable_name_meta"].iloc[0],
        description_short=series_description,
        description_from_producer=source_desc,
        origins=[origin],
        unit=tb["long_unit"].iloc[0].lower(),
        short_unit=tb["short_unit"].iloc[0],
    )

    # hotfix - for some reason underscore function does not work as expected
    var_name = underscore(tb["variable_name"].iloc[0][0:254], validate=False)
    special_chars = ["(", ")", "+", "!", ","]
    for char in special_chars:
        if char in var_name:
            var_name = var_name.replace(char, "_")
    tb["variable"] = var_name

    tb = Table(tb[["country", "year", "value", "variable", "meta"]])
    # convert integer values to int but round float to 2 decimal places, string remain as string
    tb["value"] = tb["value"].apply(value_convert)
    tb = tb.set_index(["year", "country"])

    return tb


def create_table(tb: Table) -> Table:
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

    cols_meta = ["indicator", "seriesdescription", "seriescode"]
    cols = ["indicator", "seriescode"]
    if tb.shape[1] > 11:
        col_list = sorted(tb.columns.to_list())
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
        tb["variable_name_meta"] = tb[cols_meta_dim].agg(" - ".join, axis=1)
        tb["variable_name"] = tb[cols_dim].agg(" - ".join, axis=1)
        tb = Table(tb[cols_keep])
        tb["seriescode"] = tb["seriescode"].str.lower()
    else:
        tb["variable_name_meta"] = tb[cols_meta].agg(" - ".join, axis=1)
        tb["variable_name"] = tb[cols].agg(" - ".join, axis=1)
        tb = Table(tb[cols_keep])
        tb["seriescode"] = tb["seriescode"].str.lower()

    return tb


def load_clean_source_mapping() -> Dict[str, str]:
    """
    Load the existing json which maps the raw sources to a cleaner version of the sources.
    """
    with open(paths.directory / "un_sdg.sources.json", "r") as f:
        sources = json.load(f)
        return cast(Dict[str, str], sources)


def load_additional_source_mapping() -> Dict[str, str]:
    """
    Load the existing json which maps the raw sources to a cleaner version of the sources.
    """
    with open(paths.directory / "un_sdg.sources_additional.json", "r") as f:
        sources = json.load(f)
        return cast(Dict[str, str], sources)


def load_short_source_mapping() -> Dict[str, str]:
    """
    Load the existing json which maps the raw sources to a cleaner version of the sources.
    """
    with open(paths.directory / "un_sdg.sources_short.json", "r") as f:
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
