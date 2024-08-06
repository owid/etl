"""Load a grapher dataset and create an explorer dataset with its tsv file."""
import pandas as pd
from config_dict import CONFIG_DICT  # type: ignore

from etl.helpers import PathFinder, create_explorer

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load minerals grapher dataset and read its main table.
    ds_unicef = paths.load_dataset("child_migration")
    ds_unhcr = paths.load_dataset("refugee_data")
    ds_undesa = paths.load_dataset("migrant_stock")
    ds_un_wpp = paths.load_dataset("un_wpp_full")
    ds_wdi = paths.load_dataset("wdi")

    tb_child_mig = ds_unicef.read_table("child_migration")
    tb_refugee_data = ds_unhcr.read_table("refugee_data")
    tb_migrant_stock = ds_undesa.read_table("migrant_stock")
    tb_un_wpp_full = ds_un_wpp.read_table("migration")
    tb_wdi = ds_wdi.read_table("wdi")

    #
    # Process data.
    #
    # Prepare graphers table of explorer.
    df_graphers = pd.DataFrame()
    variable_ids = []
    metric_dropdown = []
    period_radio = []
    sub_metric_radio = []
    age_radio = []
    processing_radio = []

    list_of_config_lists = [variable_ids, metric_dropdown, period_radio, sub_metric_radio, age_radio, processing_radio]

    list_of_config_lists = create_rows_for_tb(list_of_config_lists, tb_child_mig, ds_unicef)
    list_of_config_lists = create_rows_for_tb(list_of_config_lists, tb_refugee_data, ds_unhcr)
    list_of_config_lists = create_rows_for_tb(list_of_config_lists, tb_migrant_stock, ds_undesa)
    list_of_config_lists = create_rows_for_tb(list_of_config_lists, tb_un_wpp_full, ds_un_wpp)
    list_of_config_lists = create_rows_for_tb(list_of_config_lists, tb_wdi, ds_wdi)

    variable_ids, metric_dropdown, period_radio, sub_metric_radio, age_radio, processing_radio = list_of_config_lists

    df_graphers["yVariableIds"] = variable_ids
    df_graphers["Metric Dropdown"] = metric_dropdown
    df_graphers["Period Radio"] = period_radio
    df_graphers["Sub-Metric Radio"] = sub_metric_radio
    df_graphers["Age Radio"] = age_radio

    # Add a map tab to all indicators.
    df_graphers["hasMapTab"] = True

    # Sanity check.
    error = "Duplicated rows in explorer."
    assert df_graphers[
        df_graphers.duplicated(subset=["Metric Dropdown", "Period Radio", "Sub-Metric Radio", "Age Radio"], keep=False)
    ].empty, error

    # Sort rows conveniently.
    df_graphers = df_graphers.sort_values(
        ["Metric Dropdown", "Period Radio", "Sub-Metric Radio", "Age Radio", "Processing Radio"]
    ).reset_index(drop=True)

    # Prepare explorer metadata.
    config = {
        "explorerTitle": "Migration, Refugees, and Asylum Seekers",
        "explorerSubtitle": "Explore the migration of people across the world.",
        "selection": [
            "Canada",
            "France",
            "Germany",
            "United Kingdom",
            "United States",
            "India",
            "China",
            "Syria",
            "Yemen",
        ],
    }

    #
    # Save outputs.
    #
    # Create a new explorers dataset and tsv file.
    print(df_graphers)

    ds_explorer = create_explorer(dest_dir=dest_dir, config=config, df_graphers=df_graphers)
    ds_explorer.save()


def create_rows_for_tb(list_of_config_lists, tb, ds):
    # read out of list
    variable_ids, metric_dropdown, period_radio, sub_metric_radio, age_radio, processing_radio = list_of_config_lists
    for column in tb.drop(columns=["country", "year"]).columns:
        if tb[column].notnull().any():
            if column not in CONFIG_DICT.keys():
                continue
            metric = CONFIG_DICT[column]["metric"]
            # append configuration values
            period_radio.append(CONFIG_DICT[column]["period_radio"])
            sub_metric_radio.append(CONFIG_DICT[column]["sub_metric_radio"])
            age_radio.append(CONFIG_DICT[column]["age_radio"])
            metric_dropdown.append(metric)
            processing_radio.append(CONFIG_DICT[column]["processing_radio"])

            variable_ids.append([f"{ds.metadata.uri}/{tb.metadata.short_name}#{column}"])
    return [variable_ids, metric_dropdown, period_radio, sub_metric_radio, age_radio, processing_radio]
