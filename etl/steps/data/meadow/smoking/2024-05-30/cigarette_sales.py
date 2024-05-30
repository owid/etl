"""Load a snapshot and create a meadow dataset."""

from zipfile import ZipFile

import owid.catalog.processing as pr

from etl.helpers import PathFinder  # , create_dataset

# import pandas as pd


# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

COUNTRY_MAP = {"Australia": {"file_name": "ISS-Australia_120111.xls", "sheet_name": "Table2"}}


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
    country_tables = {}
    for cty, cty_sheet in COUNTRY_MAP.items():
        # open excel file
        tb_from_excel = pr.read_excel(
            zf.open("{}{}".format(folder_name, cty_sheet["file_name"])), sheet_name=cty_sheet["sheet_name"], header=9
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
        tb_from_excel.drop(labels=[0, 1], axis=0, inplace=True)
        tb_from_excel.columns = concat_columns
        tb_from_excel["Country"] = cty
        country_tables[cty] = tb_from_excel
        print(tb_from_excel.head)

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
