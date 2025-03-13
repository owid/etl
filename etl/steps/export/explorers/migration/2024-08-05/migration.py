"""Load a grapher dataset and create an explorer dataset with its tsv file."""

from pathlib import Path

import pandas as pd
from migration_config_dict import ADDITIONAL_DESCRIPTIONS, CONFIG_DICT, MAP_BRACKETS, SORTER  # type: ignore

from etl.explorer import Explorer
from etl.helpers import PathFinder, create_explorer
from etl.paths import EXPLORERS_DIR

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# whether to use existing map brackets from old explorer, if True existing map brackets are used & prioritized, if False map brackets from MAP_BRACKETS override existing
USE_EXISTING_MAP_BRACKETS = False


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load grapher datasets for all migration data
    ds_unicef = paths.load_dataset("child_migration")
    ds_unhcr = paths.load_dataset("refugee_data")
    ds_undesa = paths.load_dataset("migrant_stock")
    ds_un_wpp = paths.load_dataset("un_wpp_full")
    ds_wdi = paths.load_dataset("wdi")
    ds_idmc = paths.load_dataset("internal_displacement")

    tb_child_mig = ds_unicef.read("child_migration")
    tb_refugee_data = ds_unhcr.read("refugee_data")
    tb_migrant_stock = ds_undesa.read("migrant_stock")
    tb_un_wpp_full = ds_un_wpp.read("migration")
    tb_wdi = ds_wdi.read("wdi")
    tb_idmc = ds_idmc.read("internal_displacement")

    tbs_and_ds = [
        (tb_child_mig, ds_unicef),
        (tb_refugee_data, ds_unhcr),
        (tb_migrant_stock, ds_undesa),
        (tb_un_wpp_full, ds_un_wpp),
        (tb_wdi, ds_wdi),
        (tb_idmc, ds_idmc),
    ]

    # try to get map brackets from old explorer - only if USE_EXISTING_MAP_BRACKETS is True
    # code left in for eventual update of explorer - might need to change expl_path if explorer is renamed
    if USE_EXISTING_MAP_BRACKETS:
        expl_path = (Path(EXPLORERS_DIR) / paths.short_name).with_suffix(".explorer.tsv")
        if expl_path.exists():
            old_explorer = Explorer.from_owid_content(paths.short_name)
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
    df_graphers["hasMapTab"] = True
    # show map tab by default
    df_graphers["tab"] = "map"
    # set yAxis to start at 0
    df_graphers["yAxisMin"] = 0
    # set current year als maximum year
    df_graphers["timelineMaxTime"] = paths.version[0:4]

    df_graphers["relatedQuestionText"] = "Migration Data: Our sources and definitions"
    df_graphers["relatedQuestionUrl"] = "https://ourworldindata.org/migration-definition"

    # Sanity check.
    error = "Duplicated rows in explorer."
    assert df_graphers[
        df_graphers.duplicated(subset=["Metric Dropdown", "Period Radio", "Sub-Metric Radio", "Age Radio"], keep=False)
    ].empty, error

    # Sort rows conveniently
    df_graphers["Metric Dropdown"] = pd.Categorical(df_graphers["Metric Dropdown"], categories=SORTER, ordered=True)
    df_graphers["Period Radio"] = pd.Categorical(
        df_graphers["Period Radio"], categories=["Total number", "Five-year change", "Annual change"], ordered=True
    )
    df_graphers["Sub-Metric Radio"] = pd.Categorical(
        df_graphers["Sub-Metric Radio"], categories=["Total", "Per capita / Share of population"], ordered=True
    )
    df_graphers["Age Radio"] = pd.Categorical(
        df_graphers["Age Radio"], categories=["All ages", "Under 18"], ordered=True
    )

    df_graphers = df_graphers.sort_values(
        by=["Metric Dropdown", "Age Radio", "Period Radio", "Sub-Metric Radio"], ascending=True
    ).reset_index(drop=True)

    # Prepare explorer metadata.
    config = {
        "explorerTitle": "Migration, Refugees, and Asylum Seekers",
        "explorerSubtitle": "Explore the migration of people across the world.",
        "thumbnail": "https://assets.ourworldindata.org/uploads/2022/03/Migration-Data-Explorer.png",
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
        "hideAlertBanner": "true",
        "wpBlockId": 49910,
        "hasMapTab": "true",
        "yAxisMin": 0,
        "hideAnnotationFieldsInTitle": "true",
        "tab": "map",
    }

    # create columns for the explorer
    col_dicts = []

    for tb, ds in tbs_and_ds:
        col_dicts = create_column_rows(col_dicts, tb, ds)

    df_columns = pd.DataFrame(col_dicts)

    df_columns["colorScaleNumericMinValue"] = 0
    df_columns["colorScaleEqualSizeBins"] = True  # equal size bins for all indicators

    # Save outputs.
    ds_explorer = create_explorer(dest_dir=dest_dir, config=config, df_graphers=df_graphers, df_columns=df_columns)
    ds_explorer.save()


def create_graphers_rows(graphers_dicts, tb, ds):
    # read out of list
    for column in tb.drop(columns=["country", "year"]).columns:
        if tb[column].notnull().any():
            if column not in CONFIG_DICT.keys():
                continue
            graphers_row_dict = {}

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

            graphers_dicts.append(graphers_row_dict)

    return graphers_dicts


def create_column_rows(col_dicts, tb, ds):
    for column in tb.drop(columns=["country", "year"]).columns:
        if tb[column].notnull().any():
            if column not in CONFIG_DICT.keys():
                continue
            col_row_dict = {}
            meta = tb[column].metadata

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

            col_row_dict["slug"] = column
            if meta.short_unit == "%":
                col_row_dict["type"] = "Percentage"
            else:
                col_row_dict["type"] = "Numeric"
            col_row_dict["colorScaleScheme"] = MAP_BRACKETS[column]["colorScaleScheme"]
            col_row_dict["colorScaleNumericBins"] = MAP_BRACKETS[column]["colorScaleNumericBins"]
            col_dicts.append(col_row_dict)

    return col_dicts
