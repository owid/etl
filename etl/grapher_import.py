"""Imports a dataset and associated data sources, variables, and data points
into the SQL database.

Usage:

    >>> from standard_importer import import_dataset
    >>> dataset_dir = "worldbank_wdi"
    >>> dataset_namespace = "worldbank_wdi@2021.05.25"
    >>> import_dataset.main(dataset_dir, dataset_namespace)
"""

import os
from dotenv import load_dotenv

from db import get_connection
from db_utils import DBUtils
from owid import catalog
import pandas as pd

import logging

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

load_dotenv()
DEBUG = os.getenv("DEBUG") == "True"
# USER_ID = int(os.getenv("USER_ID"))  # type: ignore

CURRENT_DIR = os.path.dirname(__file__)
# CURRENT_DIR = os.path.join(os.getcwd(), 'standard_importer')


def upsert_dataset(dataset: catalog.Dataset, dataset_namespace: str):
    # This function would have to create the dataset table row, a namespace row
    # and the sources table row. There is a bit of an open question if we should
    # map one dataset with N tables to one namespace and N datasets in
    # mysql or if we should just flatten it into one dataset?
    # The metadata of the dataset should be used to feed the other instances where
    # possible
    pass


def upsert_table(table: catalog.Table, dataset_id):
    # This function is used to put one ready to go formatted Table (i.e.
    # in the format (year, entityId, value)) into mysql. The metadata
    # of the variable should be used to fill the required fields if possible.
    test = pd.DataFrame()
    test.index

    assert set(table.index.names) == {
        "year",
        "entity_id",
    }, f"Tables to be upserted must have only 2 indices: year and entity_id. Instead they have: {table.index.names}"
    assert (
        len(table.columns) == 1
    ), f"Tables to be upserted must have only 1 column. Instead they have: {table.columns.names}"
    assert (
        table.index.dtypes[0] == "int"
    ), f"year must be of an integer type but was: {table.index.dtypes[0]}"
    assert (
        table.index.dtypes[1] == "int"
    ), f"entity_id must be of an integer type but was: {table.index.dtypes[1]}"

    connection = None
    cursor = None
    try:

        connection = get_connection()
        connection.autocommit(False)
        cursor = connection.cursor()
        db = DBUtils(cursor)
        variable = db.fetch_one_or_none(
            "select * from variables v where v.datasetId = %(datasetid)s",
            {"datasetid": str(dataset_id)},
        )

        if variable is not None:
            print("Variable already exists in Mysql")
        else:
            print("Variable does not yet exist")

        # Upsert variables
        logger.info("---\nUpserting variable...")

        # variable = table["value"]

        years = table.index.unique(level="year").values
        min_year = min(years)
        max_year = max(years)
        timespan = f"{min_year}-{max_year}"
        print(timespan)
        # TODO: this is actually an insert always as id is not used, correct below accordingly
        # db_variable_id = db.upsert_variable(
        #     name=variable.short_name,
        #     source_id="?",
        #     dataset_id=dataset_id,
        #     description=variable.metadata.description,
        #     code="?",
        #     unit=variable.metadata.unit,
        #     short_unit=variable.metadata.short_unit,
        #     timespan=timespan,
        #     coverage="?",
        #     display=variable.metadata.display,
        #     original_metadata="?",
        # )
        # table["db_variable_id"] = db_variable_id

        # db.cursor.execute(
        #     """
        #     DELETE FROM data_values WHERE variableId=%s
        # """,
        #     [int(db_variable_id)],
        # )

        # query = """
        #     INSERT INTO data_values
        #         (value, year, entityId, variableId)
        #     VALUES (%s, %s, %s, %s)
        #     ON DUPLICATE KEY UPDATE
        #         value = VALUES(value),
        #         year = VALUES(year),
        #         entityId = VALUES(entityId),
        #         variableId = VALUES(variableId)
        # """
        # db.upsert_many(
        #     query,
        #     ((row.value, row.year, row.entity_id, row.db_variable_id) for row in table),
        # )
        logger.info(f"Upserted {len(table)} datapoints.")

        connection.commit()

    except Exception as e:
        logger.error(f"Error encountered during import: {e}")
        logger.error("Rolling back changes...")
        if connection:
            connection.rollback()
        if DEBUG:
            traceback.print_exc()
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
