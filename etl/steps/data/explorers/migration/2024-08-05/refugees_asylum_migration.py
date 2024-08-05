"""Load a grapher dataset and create an explorer dataset with its tsv file."""
import pandas as pd

from etl.helpers import PathFinder, create_explorer

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Options for drop down/ radio buttons.
P_TOTAL = "Total"
P_CHANGE = "Five-year change"
P_NEW = "Annual/ New"

SM_TOTAL = "Total"
SM_SHARE = "Per capita/ Share of population"

A_TOTAL = "Total"
A_UNDER_18 = "Under 18"
A_UNDER_15 = "Under 15"


# Define configuration for each indicator.
CONFIG_DICT = {
    # UNICEF values (TODO: check new idps and think about adding shares to idp data)
    "international_migrants_under_18_dest": {
        "period_radio": P_TOTAL,
        "sub_metric_radio": SM_TOTAL,
        "age_radio": A_UNDER_18,
    },
    "migrants_under_18_dest_per_1000": {"period_radio": P_TOTAL, "sub_metric_radio": SM_SHARE, "age_radio": A_UNDER_18},
    "refugees_under_18_asylum": {"period_radio": P_TOTAL, "sub_metric_radio": SM_TOTAL, "age_radio": A_UNDER_18},
    "refugees_under_18_origin": {"period_radio": P_TOTAL, "sub_metric_radio": SM_TOTAL, "age_radio": A_UNDER_18},
    "refugees_under_18_asylum_per_1000": {
        "period_radio": P_TOTAL,
        "sub_metric_radio": SM_SHARE,
        "age_radio": A_UNDER_18,
    },
    "refugees_under_18_origin_per_1000": {
        "period_radio": P_TOTAL,
        "sub_metric_radio": SM_SHARE,
        "age_radio": A_UNDER_18,
    },
    "idps_under_18_conflict_violence": {"period_radio": P_TOTAL, "sub_metric_radio": SM_TOTAL, "age_radio": A_UNDER_18},
    "idps_under_18_disaster": {"period_radio": P_TOTAL, "sub_metric_radio": SM_TOTAL, "age_radio": A_UNDER_18},
    "idps_under_18_total": {"period_radio": P_TOTAL, "sub_metric_radio": SM_TOTAL, "age_radio": A_UNDER_18},
    "new_idps_under_18_conflict_violence": {
        "period_radio": P_NEW,
        "sub_metric_radio": SM_TOTAL,
        "age_radio": A_UNDER_18,
    },
    "new_idps_under_18_disaster": {"period_radio": P_NEW, "sub_metric_radio": SM_TOTAL, "age_radio": A_UNDER_18},
    "new_idps_under_18_total": {"period_radio": P_NEW, "sub_metric_radio": SM_TOTAL, "age_radio": A_UNDER_18},
    # UNHCR values (TODO: think about adding stateless people, international protection, host community)
    "refugees_under_unhcrs_mandate_origin": {
        "period_radio": P_TOTAL,
        "sub_metric_radio": SM_TOTAL,
        "age_radio": A_TOTAL,
    },
    "asylum_seekers_origin": {"period_radio": P_TOTAL, "sub_metric_radio": SM_TOTAL, "age_radio": A_TOTAL},
    "refugees_under_unhcrs_mandate_asylum": {
        "period_radio": P_TOTAL,
        "sub_metric_radio": SM_TOTAL,
        "age_radio": A_TOTAL,
    },
    "asylum_seekers_asylum": {"period_radio": P_TOTAL, "sub_metric_radio": SM_TOTAL, "age_radio": A_TOTAL},
    "returned_refugees_origin": {"period_radio": P_TOTAL, "sub_metric_radio": SM_TOTAL, "age_radio": A_TOTAL},
    "resettlement_arrivals_origin": {"period_radio": P_TOTAL, "sub_metric_radio": SM_TOTAL, "age_radio": A_TOTAL},
    "returned_refugees_dest": {"period_radio": P_TOTAL, "sub_metric_radio": SM_TOTAL, "age_radio": A_TOTAL},
    "resettlement_arrivals_dest": {"period_radio": P_TOTAL, "sub_metric_radio": SM_TOTAL, "age_radio": A_TOTAL},
    "refugees_per_1000_pop_origin": {"period_radio": P_TOTAL, "sub_metric_radio": SM_SHARE, "age_radio": A_TOTAL},
    "refugees_per_1000_pop_asylum": {"period_radio": P_TOTAL, "sub_metric_radio": SM_SHARE, "age_radio": A_TOTAL},
    "asylum_seekers_per_100k_pop_origin": {"period_radio": P_TOTAL, "sub_metric_radio": SM_SHARE, "age_radio": A_TOTAL},
    "asylum_seekers_per_100k_pop_asylum": {"period_radio": P_TOTAL, "sub_metric_radio": SM_SHARE, "age_radio": A_TOTAL},
    "resettlement_per_100k_origin": {"period_radio": P_TOTAL, "sub_metric_radio": SM_SHARE, "age_radio": A_TOTAL},
    "resettlement_per_100k_dest": {"period_radio": P_TOTAL, "sub_metric_radio": SM_SHARE, "age_radio": A_TOTAL},
    "refugees_origin_5y_avg": {"period_radio": P_CHANGE, "sub_metric_radio": SM_TOTAL, "age_radio": A_TOTAL},
    "refugees_asylum_5y_avg": {"period_radio": P_CHANGE, "sub_metric_radio": SM_TOTAL, "age_radio": A_TOTAL},
    "asylum_seekers_origin_5y_avg": {"period_radio": P_CHANGE, "sub_metric_radio": SM_TOTAL, "age_radio": A_TOTAL},
    "asylum_seekers_asylum_5y_avg": {"period_radio": P_CHANGE, "sub_metric_radio": SM_TOTAL, "age_radio": A_TOTAL},
    "refugees_origin_5y_avg_per_1000_pop": {
        "period_radio": P_CHANGE,
        "sub_metric_radio": SM_SHARE,
        "age_radio": A_TOTAL,
    },
    "refugees_asylum_5y_avg_per_1000_pop": {
        "period_radio": P_CHANGE,
        "sub_metric_radio": SM_SHARE,
        "age_radio": A_TOTAL,
    },
    "asylum_seekers_origin_5y_avg_per_100k_pop": {
        "period_radio": P_CHANGE,
        "sub_metric_radio": SM_SHARE,
        "age_radio": A_TOTAL,
    },
    "asylum_seekers_asylum_5y_avg_per_100k_pop": {
        "period_radio": P_CHANGE,
        "sub_metric_radio": SM_SHARE,
        "age_radio": A_TOTAL,
    },
    "resettlement_origin_5y_avg": {"period_radio": P_CHANGE, "sub_metric_radio": SM_TOTAL, "age_radio": A_TOTAL},
    "resettlement_dest_5y_avg": {"period_radio": P_CHANGE, "sub_metric_radio": SM_TOTAL, "age_radio": A_TOTAL},
    "resettlement_origin_5y_avg_per_100k_pop": {
        "period_radio": P_CHANGE,
        "sub_metric_radio": SM_SHARE,
        "age_radio": A_TOTAL,
    },
    "resettlement_dest_5y_avg_per_100k_pop": {
        "period_radio": P_CHANGE,
        "sub_metric_radio": SM_SHARE,
        "age_radio": A_TOTAL,
    },
    # UNDESA values (TODO: think about female/ male/ age groups)
    "immigrants_all": {"period_radio": P_TOTAL, "sub_metric_radio": SM_TOTAL, "age_radio": A_TOTAL},
    "emigrants_all": {"period_radio": P_TOTAL, "sub_metric_radio": SM_TOTAL, "age_radio": A_TOTAL},
    "immigrant_share_of_dest_population_all": {
        "period_radio": P_TOTAL,
        "sub_metric_radio": SM_SHARE,
        "age_radio": A_TOTAL,
    },
    "emigrants_share_of_total_population": {
        "period_radio": P_TOTAL,
        "sub_metric_radio": SM_SHARE,
        "age_radio": A_TOTAL,
    },
    "immigrants_under_15": {"period_radio": P_TOTAL, "sub_metric_radio": SM_TOTAL, "age_radio": A_UNDER_15},
    "immigrants_under_15_per_1000_people": {
        "period_radio": P_TOTAL,
        "sub_metric_radio": SM_SHARE,
        "age_radio": A_UNDER_15,
    },
    # UN WPP values
    "net_migration": {"period_radio": P_TOTAL, "sub_metric_radio": SM_TOTAL, "age_radio": A_TOTAL},
    "net_migration_rate": {"period_radio": P_TOTAL, "sub_metric_radio": SM_SHARE, "age_radio": A_TOTAL},
    # World Bank
    # average remittance cost sending to country
    "si_rmt_cost_ib_zs": {"period_radio": P_TOTAL, "sub_metric_radio": SM_TOTAL, "age_radio": A_TOTAL},
    # average remittance cost sending from country
    "si_rmt_cost_ob_zs": {"period_radio": P_TOTAL, "sub_metric_radio": SM_TOTAL, "age_radio": A_TOTAL},
    # remittance as share of GDP
    "bx_trf_pwkr_dt_gd_zs": {"period_radio": P_TOTAL, "sub_metric_radio": SM_SHARE, "age_radio": A_TOTAL},
}


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

    list_of_config_lists = [variable_ids, metric_dropdown, period_radio, sub_metric_radio, age_radio]

    list_of_config_lists = create_rows_for_tb(list_of_config_lists, tb_child_mig, ds_unicef)
    list_of_config_lists = create_rows_for_tb(list_of_config_lists, tb_refugee_data, ds_unhcr)
    list_of_config_lists = create_rows_for_tb(list_of_config_lists, tb_migrant_stock, ds_undesa)
    list_of_config_lists = create_rows_for_tb(list_of_config_lists, tb_un_wpp_full, ds_un_wpp)
    list_of_config_lists = create_rows_for_tb(list_of_config_lists, tb_wdi, ds_wdi)

    variable_ids, metric_dropdown, period_radio, sub_metric_radio, age_radio = list_of_config_lists

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
        ["Metric Dropdown", "Period Radio", "Sub-Metric Radio", "Age Radio"]
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
    ds_explorer = create_explorer(dest_dir=dest_dir, config=config, df_graphers=df_graphers)
    ds_explorer.save()


def create_rows_for_tb(list_of_config_lists, tb, ds):
    # read out of list
    variable_ids, metric_dropdown, period_radio, sub_metric_radio, age_radio = list_of_config_lists
    for column in tb.drop(columns=["country", "year"]).columns:
        if tb[column].notnull().any():
            if column not in CONFIG_DICT.keys():
                continue
            metric = tb[column].metadata.title
            # append configuration values
            period_radio.append(CONFIG_DICT[column]["period_radio"])
            sub_metric_radio.append(CONFIG_DICT[column]["sub_metric_radio"])
            age_radio.append(CONFIG_DICT[column]["age_radio"])
            metric_dropdown.append(metric)

            variable_ids.append([f"{ds.metadata.uri}/{tb.metadata.short_name}#{column}"])
    return [variable_ids, metric_dropdown, period_radio, sub_metric_radio, age_radio]
