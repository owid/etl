import json
import os
import requests
from typing import Iterable
from pathlib import Path
import pandas as pd

from structlog import get_logger
from owid.catalog import Dataset, Table, Source, VariableMeta
from owid.walden import Catalog
from etl.paths import DATA_DIR
from etl import grapher_helpers as gh


log = get_logger()


BASE_URL = "https://unstats.un.org/sdgapi"
VERSION = Path(__file__).parent.stem
FNAME = Path(__file__).stem
NAMESPACE = Path(__file__).parent.parent.stem

VERSION = "2022-07-07"
FNAME = "un_sdg"
NAMESPACE = "un_sdg"


def get_grapher_dataset() -> Dataset:
    dataset = Dataset(DATA_DIR / f"garden/{NAMESPACE}/{VERSION}/{FNAME}")
    # short_name should include dataset name and version
    dataset.metadata.short_name = (
        f"{dataset.metadata.short_name}__{VERSION.replace('-', '_')}"
    )
    return dataset


dataset = get_grapher_dataset()


def get_grapher_tables(dataset: Dataset) -> Iterable[Table]:
    clean_source_map = load_clean_source_mapping()
    walden_ds = Catalog().find_one(
        namespace=NAMESPACE, short_name=FNAME, version=VERSION
    )
    ds_garden = Dataset((DATA_DIR / f"garden/{NAMESPACE}/{VERSION}/{FNAME}").as_posix())
    sdg_tables = ds_garden.table_names
    for var in sdg_tables:
        log.info(
            "Loading data from garden and creating a dataframe with variable names to match grapher..."
        )
        var_df = create_dataframe_with_variable_name(ds_garden, var)
        indicator = var_df["variable_name"].iloc[0].split("-")[0].strip()
        print(indicator)
        if len(var_df["variable_name"].drop_duplicates()) > 1:
            var_gr = var_df.groupby("variable_name")
            for var_name, df_var in var_gr:

                if len(df_var["source"].drop_duplicates()) > 1:
                    clean_source = "Data from multiple sources compiled by the UN"
                else:
                    source_name = df_var["source"].drop_duplicates().iloc[0]
                    assert (
                        source_name in clean_source_map
                    ), f"{source_name} not in un_sdg.sources.json - please add"
                    clean_source = clean_source_map[source_name]

                source = Source(
                    name="UN SDG",
                    description="%s: %s"
                    % ("Metadata available at", get_metadata_link(indicator)),
                    url=walden_ds.metadata["url"],
                    source_data_url=walden_ds.metadata["source_data_url"],
                    owid_data_url=walden_ds.metadata["owid_data_url"],
                    date_accessed=walden_ds.metadata["date_accessed"],
                    publication_date=walden_ds.metadata["publication_date"],
                    publication_year=walden_ds.metadata["publication_year"],
                    published_by=walden_ds.metadata["name"],
                    publisher_source=clean_source,
                )

                df_var = Table(df_var)
                df_var.metadata = VariableMeta(
                    title=var_name,
                    description="",
                    sources=[source],
                    unit=df_var["long_unit"].iloc[0],
                    short_unit=df_var["short_unit"].iloc[0],
                    additional_info=None,
                )
                df_var = df_var[["country", "year", "value"]]
                if df_var["value"].apply(isinstance, args=[float]).all():
                    df_var["value"] = df_var["value"].round(2)
                df_var["entity_id"] = gh.country_to_entity_id(
                    df_var["country"], create_entities=True
                )
                df_var = df_var.drop(columns=["country"]).set_index(
                    ["year", "entity_id"]
                )

            yield df_var


def create_dataframe_with_variable_name(dataset: Dataset, tab: str) -> pd.DataFrame:

    cols_keep = [
        "country",
        "year",
        "seriescode",
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
