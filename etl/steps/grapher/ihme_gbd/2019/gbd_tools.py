from typing import List

import pandas as pd
from owid.catalog import Dataset, Table, VariableMeta
from owid.catalog.utils import underscore

from etl import grapher_helpers as gh
from etl.db import get_connection, get_dataset_id, get_variables_in_dataset


def create_var_name(df: pd.DataFrame) -> pd.DataFrame:

    age_dict = {
        "Early Neonatal": "0-6 days",
        "Late Neonatal": "7-27 days",
        "Post Neonatal": "28-364 days",
        "1 to 4": "1-4 years",
    }

    df = df.replace({"age": age_dict}, regex=False)
    # For risk factor variables we want to include the risk factor and the cause of death so need a slightly different variable format
    try:
        if "rei" in df.columns:
            df[["measure", "cause", "rei", "sex", "age", "metric"]] = df[
                ["measure", "cause", "rei", "sex", "age", "metric"]
            ].astype(str)
            df["variable"] = (
                df["measure"]
                + " - Cause: "
                + df["cause"]
                + " - Risk: "
                + df["rei"]
                + " - Sex: "
                + df["sex"]
                + " - Age: "
                + df["age"]
                + " ("
                + df["metric"]
                + ")"
            )
        else:
            df[["measure", "cause", "sex", "age", "metric"]] = df[["measure", "cause", "sex", "age", "metric"]].astype(
                str
            )
            df["variable"] = (
                df["measure"]
                + " - "
                + df["cause"]
                + " - Sex: "
                + df["sex"]
                + " - Age: "
                + df["age"]
                + " ("
                + df["metric"]
                + ")"
            )
    except ValueError:
        pass

    assert "variable" in df.columns
    return df


def add_metadata_and_prepare_for_grapher(df_gr: pd.DataFrame, garden_ds: Dataset) -> Table:

    df_gr["meta"] = VariableMeta(
        title=df_gr["variable"].iloc[0],
        sources=[garden_ds.metadata.sources],
        unit=df_gr["unit"].iloc[0],
        short_unit=df_gr["unit"].iloc[0],
        additional_info=None,
    )
    # Taking only the first 255 characters of the var name as this is the limit (there is at least one that is too long)
    df_gr["variable"] = underscore(df_gr["variable"].iloc[0])

    df_gr = df_gr[["country", "year", "value", "variable", "meta"]].copy()
    # convert integer values to int but round float to 2 decimal places, string remain as string
    df_gr["entity_id"] = gh.country_to_entity_id(df_gr["country"], create_entities=True)
    df_gr = df_gr.drop(columns=["country"]).set_index(["year", "entity_id"])

    return Table(df_gr)


def run_wrapper(garden_dataset: Dataset, dataset: Dataset, old_dataset_name: str) -> None:
    # add tables to dataset
    variables_in_charts = get_variables_used_in_charts(old_dataset_name)
    tables = garden_dataset.table_names
    for table in tables:
        df = garden_dataset[table]
        df = create_var_name(df)
        # Get variables used in existing charts
        df = df[df["variable"].isin(variables_in_charts)]

        if df.shape[0] > 0:
            df["source"] = "Institute for Health Metrics and Evaluation - Global Burden of Disease (2019)"

            var_gr = df.groupby("variable")

            for var_name, df_var in var_gr:
                df_tab = add_metadata_and_prepare_for_grapher(df_var, garden_dataset)
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
                    wide_table.metadata.short_name = wide_table.columns[0]
                    dataset.add(wide_table)


def get_variables_used_in_charts(old_dataset_name: str) -> List:
    with get_connection() as db_conn:
        old_dataset_id = get_dataset_id(db_conn=db_conn, dataset_name=old_dataset_name)
        old_variables = get_variables_in_dataset(db_conn=db_conn, dataset_id=old_dataset_id, only_used_in_charts=True)
        old_variable_names = old_variables["name"].tolist()
    return old_variable_names
