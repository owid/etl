"""Load a snapshot and create a meadow dataset."""

from zipfile import ZipFile

import owid.catalog.processing as pr
import pandas as pd

from etl.helpers import PathFinder  # , create_dataset

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
    "Germany": {"file_name": "ISS-Germany_161102.xls", "sheet_name": "Table2"},
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
    "USSR and fSU": {
        "file_name": "ISS-USSR_160705.xls",
        "sheet_name": "Table2.1_USSR",
    },  # special case, formatting is different
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

NEW_COLUMNS = [
    "Year",
    "Manufactured cigarettes - Total annual millions",
    "Manufactured cigarettes - Number/adult/day",
    "Hand-rolled cigarettes - Total annual millions",
    "Hand-rolled cigarettes - Number/adult/day",
    "Total cigarettes - Total annual millions",
    "Total cigarettes - Number/adult/day",
    "All tobacco products - Total annual tonnes",
    "All tobacco products - Grams/adult/day",
]


# def run(dest_dir: str) -> None:
def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("cigarette_sales.zip")

    # Load data from snapshot.
    zf = ZipFile(snap.path)
    folder_name = zf.namelist()[0]
    # Load the Excel files for all countries
    country_tables = []

    for cty, cty_sheet in COUNTRY_MAP.items():
        if cty not in SPECIAL_CASES:
            # open excel file
            tb_from_excel = pr.read_excel(
                zf.open("{}{}".format(folder_name, cty_sheet["file_name"])),
                sheet_name=cty_sheet["sheet_name"],
                header=9,
            )
            # fix header (concatenate header row and sub headers)

            tb_from_excel.drop(labels=["Unnamed: 3", "Unnamed: 6", "Unnamed: 9", "Unnamed: 12"], axis=1, inplace=True)

            concat_columns = []
            for idx in range(len(tb_from_excel.columns)):
                if idx == 0:
                    concat_columns.append(list(tb_from_excel.columns)[idx])
                elif idx in [2, 4, 6, 8]:
                    concat_columns.append(
                        "{} - {}{}".format(
                            list(tb_from_excel.columns)[idx - 1],
                            list(tb_from_excel.iloc[0])[idx],
                            list(tb_from_excel.iloc[1])[idx],
                        )
                    )
                elif idx in [1, 3, 5, 7]:
                    concat_columns.append(
                        "{} - {} {}".format(
                            list(tb_from_excel.columns)[idx],
                            list(tb_from_excel.iloc[0])[idx],
                            list(tb_from_excel.iloc[1])[idx],
                        )
                    )
                else:
                    concat_columns.append(list(tb_from_excel.columns)[idx - 1])

            tb_from_excel.drop(labels=[0, 1], axis=0, inplace=True)
            tb_from_excel.columns = [c.lower() for c in concat_columns]
            tb_from_excel["Country"] = cty

            country_tables.append(tb_from_excel)
            print(cty)
            print(tb_from_excel.columns)

    smoking_data_tb = pd.concat(country_tables)
    print(smoking_data_tb.columns)

    # Load data from snapshot.
    # tb = snap.read()
    #
    # Process data.
    #
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    # tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.

    # ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    # ds_meadow.save()


if __name__ == "__main__":
    run()
