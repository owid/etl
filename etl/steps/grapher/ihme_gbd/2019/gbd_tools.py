from typing import List

import pandas as pd
from owid.catalog import Dataset, Source
from structlog import get_logger

from etl import grapher_helpers as gh
from etl.db import get_connection, get_dataset_id, get_variables_in_dataset

log = get_logger()


def map_age(age: pd.Series) -> pd.Series:
    age_dict = {
        "Early Neonatal": "0-6 days",
        "Late Neonatal": "7-27 days",
        "Post Neonatal": "28-364 days",
        "1 to 4": "1-4 years",
    }
    return age.replace(age_dict, regex=False)


def run_wrapper(garden_dataset: Dataset, dataset: Dataset, dims: List[str]) -> None:
    # variables_in_charts = get_variables_used_in_charts(old_dataset_name)

    # NOTE: it was `Global Burden of Disease Study (2019) - Deaths and DALYs` originally
    # all variables will inherit this source from dataset
    dataset.metadata.sources = [Source(name="Global Burden of Disease Study (2019) - Deaths and DALYs")]

    # add tables to dataset
    tables = garden_dataset.table_names
    for table in tables:
        tab = garden_dataset[table]

        tab.reset_index(inplace=True)

        # NOTE: we no longer need `create_var_name`, variable names will be created automatically from dimensions
        tab["age"] = map_age(tab["age"])

        # create entity_id from country
        tab = gh.adapt_table_for_grapher(tab)

        # add more dimensions
        tab = tab.set_index(dims, append=True)

        dataset.add(tab)



def get_variables_used_in_charts(old_dataset_name: str) -> List[str]:
    with get_connection() as db_conn:
        old_dataset_id = get_dataset_id(db_conn=db_conn, dataset_name=old_dataset_name)
        old_variables = get_variables_in_dataset(db_conn=db_conn, dataset_id=old_dataset_id, only_used_in_charts=True)
        old_variable_names = old_variables["name"].tolist()
    return old_variable_names
