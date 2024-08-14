"""Load a grapher dataset and create an explorer dataset with its tsv file."""
from pathlib import Path

import pandas as pd
from migration_config_dict import ADDITIONAL_DESCRIPTIONS, CONFIG_DICT, MAP_BRACKETS, SORTER  # type: ignore

from etl.config import EXPLORERS_DIR
from etl.explorer_helpers import Explorer
from etl.helpers import PathFinder, create_explorer

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# whether to use existing map brackets from old explorer, if True this overrides hardcoded map brackets
USE_EXISTING_MAP_BRACKETS = False


# used to sort the metrics in explorer
def sort_metrics(x):
    return SORTER.index(x)


def sort_age(x):
    age_order = ["Total", "Under 18"]
    return age_order.index(x)


def sort_period(x):
    period_order = ["Total", "Five-year change", "Annual / New"]
    return period_order.index(x)


def sort_sub_metric(x):
    sub_metric_order = ["Total", "Per capita / Share of population"]
    return sub_metric_order.index(x)


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
    ds_idmc = paths.load_dataset("internal_displacement")

    tb_child_mig = ds_unicef.read_table("child_migration")
    tb_refugee_data = ds_unhcr.read_table("refugee_data")
    tb_migrant_stock = ds_undesa.read_table("migrant_stock")
    tb_un_wpp_full = ds_un_wpp.read_table("migration")
    tb_wdi = ds_wdi.read_table("wdi")
    tb_idmc = ds_idmc.read_table("internal_displacement")

    tbs_and_ds = [
        (tb_child_mig, ds_unicef),
        (tb_refugee_data, ds_unhcr),
        (tb_migrant_stock, ds_undesa),
        (tb_un_wpp_full, ds_un_wpp),
        (tb_wdi, ds_wdi),
        (tb_idmc, ds_idmc),
    ]

    # try to get map brackets from old explorer
    if USE_EXISTING_MAP_BRACKETS:
        expl_path = (Path(EXPLORERS_DIR) / paths.short_name).with_suffix(".explorer.tsv")
        if expl_path.exists():
            old_explorer = Explorer(paths.short_name)
            df_columns = old_explorer.df_columns
            for _, row in df_columns.iterrows():
                var_title = row["slug"]
                if var_title in MAP_BRACKETS.keys() and "ColorScaleNumericBins" in row.keys():
                    MAP_BRACKETS[var_title]["colorScaleNumericBins"] = [
                        float(bracket) for bracket in row["colorScaleNumericBins"].split(";")
                    ]
                if var_title in MAP_BRACKETS.keys() and "ColorScaleScheme" in row.keys():
                    MAP_BRACKETS[var_title]["colorScaleScheme"] = row["colorScaleScheme"]

    #
    # Process data.
    #
    # Prepare graphers table of explorer.
    graphers_dicts = []

    for tb, ds in tbs_and_ds:
        graphers_dicts = create_graphers_rows(graphers_dicts, tb, ds)

    df_graphers = pd.DataFrame(graphers_dicts)

    # Add a map tab to all indicators.
    df_graphers["hasMapTab"] = "true"
    # show map tab by default
    df_graphers["tab"] = "map"
    # set yAxis to start at 0
    df_graphers["yAxisMin"] = 0
    # hide automatic time/ entity in chart title
    df_graphers["hideAnnotationFieldsInTitle"] = "true"
    # set current year als maximum year
    df_graphers["timelineMaxTime"] = 2024

    # Sanity check.
    error = "Duplicated rows in explorer."
    assert df_graphers[
        df_graphers.duplicated(subset=["Metric Dropdown", "Period Radio", "Sub-Metric Radio", "Age Radio"], keep=False)
    ].empty, error

    # Sort rows conveniently
    df_graphers["sort_order_metrics"] = df_graphers["Metric Dropdown"].apply(sort_metrics)
    df_graphers["sort_order_age"] = df_graphers["Age Radio"].apply(sort_age)
    df_graphers["sort_order_period"] = df_graphers["Period Radio"].apply(sort_period)
    df_graphers["sort_order_sub_metric"] = df_graphers["Sub-Metric Radio"].apply(sort_sub_metric)

    df_graphers = df_graphers.sort_values(
        by=["sort_order_metrics", "sort_order_age", "sort_order_period", "sort_order_sub_metric"],
        ascending=True,
    ).reset_index(drop=True)

    df_graphers = df_graphers.drop(
        columns=["sort_order_metrics", "sort_order_age", "sort_order_period", "sort_order_sub_metric"]
    )

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

    # create columns for the explorer
    col_dicts = []

    for tb, ds in tbs_and_ds:
        col_dicts = create_column_rows(col_dicts, tb, ds)

    df_columns = pd.DataFrame(col_dicts)

    df_columns["colorScaleNumericMinValue"] = 0
    df_columns["colorScaleEqualSizeBins"] = "true"

    # Save outputs.
    #
    # Create a new explorers dataset and tsv file.

    # df_graphers.to_csv("/Users/tunaacisu/Data/Test/explorer.tsv", sep="\t", index=False)

    # print(df_graphers)
    # print(df_columns)

    ds_explorer = create_explorer(dest_dir=dest_dir, config=config, df_graphers=df_graphers, df_columns=df_columns)
    ds_explorer.save()


def create_graphers_rows(graphers_dicts, tb, ds):
    # read out of list
    for column in tb.drop(columns=["country", "year"]).columns:
        if tb[column].notnull().any():
            if column not in CONFIG_DICT.keys():
                continue
            graphers_row_dict = {}

            meta = tb[column].metadata
            origin = meta.origins[0]

            config = CONFIG_DICT[column]
            if column in ["net_migration", "net_migration_rate"]:
                graphers_row_dict["yVariableIds"] = [
                    f"{ds.metadata.uri}/{tb.metadata.short_name}#{column}__sex_all__age_all__variant_medium"
                ]
            else:
                graphers_row_dict["yVariableIds"] = [f"{ds.metadata.uri}/{tb.metadata.short_name}#{column}"]
            graphers_row_dict["Metric Dropdown"] = config["metric"]
            graphers_row_dict["Period Radio"] = config["period_radio"]
            graphers_row_dict["Sub-Metric Radio"] = config["sub_metric_radio"]
            graphers_row_dict["Age Radio"] = config["age_radio"]

            if column in ADDITIONAL_DESCRIPTIONS.keys():
                graphers_row_dict["subtitle"] = ADDITIONAL_DESCRIPTIONS[column]["description"]
                graphers_row_dict["title"] = ADDITIONAL_DESCRIPTIONS[column]["title"]
            else:
                graphers_row_dict["subtitle"] = meta.description_short
                graphers_row_dict["title"] = meta.title

            if meta.processing_level == "minor":
                graphers_row_dict["sourceDesc"] = f"{origin.producer} ({origin.date_published[:4]})"
            elif meta.processing_level == "major":
                graphers_row_dict[
                    "sourceDesc"
                ] = f"Our World in Data based on {origin.producer} ({origin.date_published[:4]})"

            graphers_dicts.append(graphers_row_dict)

    return graphers_dicts


def create_column_rows(col_dicts, tb, ds):
    for column in tb.drop(columns=["country", "year"]).columns:
        if tb[column].notnull().any():
            if column not in CONFIG_DICT.keys():
                continue
            col_row_dict = {}
            meta = tb[column].metadata
            origin = meta.origins[0]

            # net migration and net migration rate are split again in grapher by sex/ age/ variant - need to add this to the catalog path
            if column in ["net_migration", "net_migration_rate"]:
                col_row_dict["catalogPath"] = (
                    f"{ds.metadata.uri}/{tb.metadata.short_name}#{column}" + "__sex_all__age_all__variant_medium"
                )
            else:
                col_row_dict["catalogPath"] = f"{ds.metadata.uri}/{tb.metadata.short_name}#{column}"

            # some indicators don't have any/ a fitting description in the metadata: add them manually
            if column in ADDITIONAL_DESCRIPTIONS.keys():
                col_row_dict["description"] = ADDITIONAL_DESCRIPTIONS[column]["description"]
            else:
                col_row_dict["description"] = meta.description_short

            col_row_dict["name"] = meta.title
            col_row_dict["slug"] = column
            col_row_dict["sourceLink"] = origin.url_main
            col_row_dict["unit"] = meta.unit
            col_row_dict["shortUnit"] = meta.short_unit
            if meta.short_unit == "%":
                col_row_dict["type"] = "Percentage"
            else:
                col_row_dict["type"] = "Numeric"
            col_row_dict["retrievedDate"] = origin.date_accessed
            # col_row_dict["additionalInfo"] = [meta.description_from_producer, meta.description_key,meta.description_processing]
            col_row_dict["colorScaleScheme"] = MAP_BRACKETS[column]["colorScaleScheme"]
            col_row_dict["colorScaleNumericBins"] = MAP_BRACKETS[column]["colorScaleNumericBins"]
            col_dicts.append(col_row_dict)

    return col_dicts
