"""Load a snapshot and create a meadow dataset."""

from zipfile import ZipFile

import owid.catalog.processing as pr
import pandas as pd

# from owid.catalog import Table
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

COUNTRY_MAP = {
    "Australia": {"file_name": "ISS-Australia_120111.xls", "sheet_name": "Table2"},
    "Austria": {"file_name": "ISS-Austria_111024.xls", "sheet_name": "Table2"},
    "Belgium": {"file_name": "ISS-Belgium_130404.xls", "sheet_name": "Table2"},
    "Bulgaria": {"file_name": "ISS-Bulgaria_160705.xls", "sheet_name": "Table2"},
    "Canada": {"file_name": "ISS-Canada_120111.xls", "sheet_name": "Table2"},
    "Czechoslovakia": {"file_name": "ISS-Czechoslovakia_160705.xls", "sheet_name": "Table2.1_CZSL"},
    "Czech Republic": {"file_name": "ISS-Czechoslovakia_160705.xls", "sheet_name": "Table2.2_CZER"},
    "Slovakia": {"file_name": "ISS-Czechoslovakia_160705.xls", "sheet_name": "Table2.3_SLOK"},
    "Denmark": {"file_name": "ISS-Denmark_111122.xls", "sheet_name": "Table2"},
    "Finland": {"file_name": "ISS-Finland_110704.xls", "sheet_name": "Table2"},
    "France": {"file_name": "ISS-France_111024.xls", "sheet_name": "Table2"},
    # special case, Germany's table starts a few lines earlier
    "Germany": {"file_name": "ISS-Germany_161102.xls", "sheet_name": "Table2"},
    "West Germany": {"file_name": "ISS-Germany_161102.xls", "sheet_name": "Table2_West"},
    "East Germany": {"file_name": "ISS-Germany_161102.xls", "sheet_name": "Table2_East"},
    "Greece": {"file_name": "ISS-Greece_150728.xls", "sheet_name": "Table2"},
    "Hungary": {"file_name": "ISS-Hungary_131105.xls", "sheet_name": "Table2"},
    "Iceland": {"file_name": "ISS-Iceland_161219.xls", "sheet_name": "Table2"},
    "Ireland": {"file_name": "ISS-Ireland_131105.xls", "sheet_name": "Table2"},
    "Israel": {"file_name": "ISS-Israel_161102.xls", "sheet_name": "Table2"},
    "Italy": {"file_name": "ISS-Italy_111024.xls", "sheet_name": "Table2"},
    "Japan": {"file_name": "ISS-Japan_161219.xls", "sheet_name": "Table2"},
    "Netherlands": {"file_name": "ISS-Netherlands_140710.xls", "sheet_name": "Table2"},
    "New Zealand": {"file_name": "ISS-NewZealand_111024.xls", "sheet_name": "Table2"},
    "Norway": {"file_name": "ISS-Norway_121004.xls", "sheet_name": "Table2"},
    "Poland": {"file_name": "ISS-Poland_140429.xls", "sheet_name": "Table2"},
    "Portugal": {"file_name": "ISS-Portugal_150211.xls", "sheet_name": "Table2"},
    "Romania": {"file_name": "ISS-Romania_160705.xls", "sheet_name": "Table2"},
    "Spain": {"file_name": "ISS-Spain_111007.xls", "sheet_name": "Table2"},
    "Sweden": {"file_name": "ISS-Sweden_111024.xls", "sheet_name": "Table2"},
    "Switzerland": {"file_name": "ISS-Switzerland_111024.xls", "sheet_name": "Table2"},
    "USA": {"file_name": "ISS-USA_151219.xls", "sheet_name": "Table2"},
    # special case, formatting is different for USSR
    "USSR and fSU": {"file_name": "ISS-USSR_160705.xls", "sheet_name": "Table2.1_USSR"},
    "Estonia": {"file_name": "ISS-USSR_160705.xls", "sheet_name": "Table2.2_ESTO"},
    "Latvia": {"file_name": "ISS-USSR_160705.xls", "sheet_name": "Table2.3_LATV"},
    "Lithuania": {"file_name": "ISS-USSR_160705.xls", "sheet_name": "Table2.4_LITH"},
    # special case: Armenia - Uzbekistan are in one sheet with a different format
    "Armenia": {"file_name": "ISS-USSR_160705.xls", "sheet_name": "Table2.5_USSR"},
    "Azerbaijan": {"file_name": "ISS-USSR_160705.xls", "sheet_name": "Table2.5_USSR"},
    "Belarus": {"file_name": "ISS-USSR_160705.xls", "sheet_name": "Table2.5_USSR"},
    "Georgia": {"file_name": "ISS-USSR_160705.xls", "sheet_name": "Table2.5_USSR"},
    "Kazakhstan": {"file_name": "ISS-USSR_160705.xls", "sheet_name": "Table2.5_USSR"},
    "Kyrgyzstan": {"file_name": "ISS-USSR_160705.xls", "sheet_name": "Table2.5_USSR"},
    "Moldova": {"file_name": "ISS-USSR_160705.xls", "sheet_name": "Table2.5_USSR"},
    "Russia": {"file_name": "ISS-USSR_160705.xls", "sheet_name": "Table2.5_USSR"},
    "Tajikistan": {"file_name": "ISS-USSR_160705.xls", "sheet_name": "Table2.5_USSR"},
    "Turkmenistan": {"file_name": "ISS-USSR_160705.xls", "sheet_name": "Table2.5_USSR"},
    "Ukraine": {"file_name": "ISS-USSR_160705.xls", "sheet_name": "Table2.5_USSR"},
    "Uzbekistan": {"file_name": "ISS-USSR_160705.xls", "sheet_name": "Table2.5_USSR"},
    "United Kingdom": {"file_name": "ISS-UnitedKingdom_160317.xls", "sheet_name": "Table2"},
    "Yugoslavia": {"file_name": "ISS-Yugoslavia_160705.xls", "sheet_name": "Table2.1_YUGF"},
    "Croatia": {"file_name": "ISS-Yugoslavia_160705.xls", "sheet_name": "Table2.2_CROA"},
    "Slovenia": {"file_name": "ISS-Yugoslavia_160705.xls", "sheet_name": "Table2.3_SLON"},
    # special case: Bosnia, Macedonia and Yugoslavia are in one sheet with a different format
    "Bosnia & Herzegovina": {"file_name": "ISS-Yugoslavia_160705.xls", "sheet_name": "Table2.4_YUGF"},
    "Macedonia": {"file_name": "ISS-Yugoslavia_160705.xls", "sheet_name": "Table2.4_YUGF"},
    "Federal Republic of Yugoslavia": {"file_name": "ISS-Yugoslavia_160705.xls", "sheet_name": "Table2.4_YUGF"},
}

SPECIAL_CASES = [
    "Germany",
    "USSR and fSU",
    "Armenia",
    "Azerbaijan",
    "Belarus",
    "Georgia",
    "Kazakhstan",
    "Kyrgyzstan",
    "Moldova",
    "Russia",
    "Tajikistan",
    "Turkmenistan",
    "Ukraine",
    "Uzbekistan",
    "Bosnia & Herzegovina",
    "Macedonia",
    "Federal Republic of Yugoslavia",
]

FORMAT_SPECIAL_CASES = {
    "Armenia": {"header_row": 9, "cols": "A,B,C", "num_rows": 13},
    "Azerbaijan": {"header_row": 9, "cols": "A,E,F", "num_rows": 13},
    "Belarus": {"header_row": 9, "cols": "A,H,I", "num_rows": 13},
    "Georgia": {"header_row": 9, "cols": "A,K,L", "num_rows": 14},
    "Kazakhstan": {"header_row": 25, "cols": "A,B,C", "num_rows": 15},
    "Kyrgyzstan": {"header_row": 25, "cols": "A,E,F", "num_rows": 13},
    "Moldova": {"header_row": 25, "cols": "A,H,I", "num_rows": 14},
    "Russia": {"header_row": 25, "cols": "A,K,L", "num_rows": 19},
    "Tajikistan": {"header_row": 46, "cols": "A,B,C", "num_rows": 13},
    "Turkmenistan": {"header_row": 46, "cols": "A,E,F", "num_rows": 13},
    "Ukraine": {"header_row": 46, "cols": "A,H,I", "num_rows": 18},
    "Uzbekistan": {"header_row": 46, "cols": "A,K,L", "num_rows": 15},
    "Bosnia & Herzegovina": {"header_row": 9, "cols": "A,B,C", "num_rows": 3},
    "Macedonia": {"header_row": 14, "cols": "A,B,C", "num_rows": 4},
    "Federal Republic of Yugoslavia": {"header_row": 20, "cols": "A,B,C", "num_rows": 9},
}

NEW_COLUMNS = [
    "year",
    "manufactured_cigarettes_millions",
    "manufactured_cigarettes_per_adult_per_day",
    "handrolled_cigarettes_millions",
    "handrolled_cigarettes_per_adult_per_day",
    "total_cigarettes_millions",
    "total_cigarettes_per_adult_per_day",
    "all_tobacco_products_tonnes",
    "all_tobacco_products_grams_per_adult_per_day",
]

ORIGINAL_COLUMNS = [
    "Year",
    "Manufactured cigarettes",
    "Unnamed: 2",
    "Unnamed: 3",
    "Hand-rolled cigarettes",
    "Unnamed: 5",
    "Unnamed: 6",
    "Total cigarettes",
    "Unnamed: 8",
    "Unnamed: 9",
    "All tobacco products",
    "Unnamed: 11",
    "Unnamed: 12",
]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("cigarette_sales.zip")
    snap_meta = snap.to_table_metadata()

    # Load data from snapshot.
    zf = ZipFile(snap.path)
    folder_name = zf.namelist()[0]

    # Process data

    # Load country tables from excel files one by one (to concatenate them later)
    country_tables_ls = []
    # list needed to pad tables so they have the same format
    addl_cols = [
        "Unnamed: 3",
        "Unnamed: 4",
        "Unnamed: 5",
        "Unnamed: 6",
        "Unnamed: 7",
        "Unnamed: 8",
        "Unnamed: 9",
        "Unnamed: 10",
        "Unnamed: 11",
        "Unnamed: 12",
    ]

    # tb_from_excel = Table()

    for cty, cty_sheet in COUNTRY_MAP.items():
        if cty not in SPECIAL_CASES:
            # open excel file
            tb_from_excel = pr.read_excel(
                zf.open(f"{folder_name}{cty_sheet['file_name']}"),
                metadata=snap_meta,
                origin=snap.metadata.origin,
                sheet_name=cty_sheet["sheet_name"],
                header=9,
                names=ORIGINAL_COLUMNS,
            )

        elif cty == "Germany":
            tb_from_excel = pr.read_excel(
                zf.open(f"{folder_name}{cty_sheet['file_name']}"),
                sheet_name=cty_sheet["sheet_name"],
                metadata=snap_meta,
                origin=snap.metadata.origin,
                header=2,
            )

        elif cty == "USSR and fSU":
            tb_from_excel = pr.read_excel(
                zf.open(f"{folder_name}{cty_sheet['file_name']}"),
                sheet_name=cty_sheet["sheet_name"],
                metadata=snap_meta,
                origin=snap.metadata.origin,
                header=9,
            )
            tb_from_excel["Unnamed: 4"] = pd.NA
            tb_from_excel["Unnamed: 5"] = pd.NA

        elif cty in FORMAT_SPECIAL_CASES.keys():
            format = FORMAT_SPECIAL_CASES[cty]
            tb_from_excel = pr.read_excel(
                zf.open(f"{folder_name}{cty_sheet['file_name']}"),
                sheet_name=cty_sheet["sheet_name"],
                metadata=snap_meta,
                origin=snap.metadata.origin,
                header=format["header_row"],
                usecols=format["cols"],
            )
            tb_from_excel.columns = [
                "Year",
                "Manufactured cigarettes - Millions",
                "Manufactured cigarettes - Number/adult/day",
            ]
            for col in addl_cols:
                tb_from_excel[col] = "NaN"
            tb_from_excel = tb_from_excel.head(format["num_rows"])

        # drop empty columns
        tb_from_excel = tb_from_excel.drop(
            columns=["Unnamed: 3", "Unnamed: 6", "Unnamed: 9", "Unnamed: 12"], axis=1, errors="raise"
        )

        # change header and remove sub-header columns
        tb_from_excel.drop(labels=[0, 1], axis=0, inplace=True)
        tb_from_excel.columns = NEW_COLUMNS

        tb_from_excel["country"] = cty
        country_tables_ls.append(tb_from_excel)

    # concatenate tables and delete rows without data
    tb_smoking = pr.concat(country_tables_ls)

    # remove repeating headers
    tb_smoking = tb_smoking.dropna(subset=["year"])
    tb_smoking = tb_smoking[tb_smoking["manufactured_cigarettes_millions"].apply(lambda x: not isinstance(x, str))]

    # change data types to string (since there are some special characters/ ranges in the data e.g. "1950-1954")
    tb_smoking["year"] = tb_smoking["year"].astype(str)

    # remove duplicate data
    tb_smoking = tb_smoking.drop_duplicates(subset=["country", "year"], keep="last")

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb_smoking = tb_smoking.format(["country", "year"])

    # Save outputs.
    #
    # Create a new meadow dataset with thex same metadata as the snapshot.
    ds_meadow = create_dataset(
        dest_dir,
        tables=[tb_smoking],
        default_metadata=snap.metadata,
        check_variables_metadata=True,
    )

    # Save changes in the new meadow dataset.
    ds_meadow.save()
