from typing import List

import pandas as pd
from owid.catalog import Dataset
from structlog import get_logger

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
    # add tables to dataset
    tables = garden_dataset.table_names
    for table in tables:
        tab = garden_dataset[table]

        tab.reset_index(inplace=True)

        tab["age"] = map_age(tab["age"])

        # add more dimensions
        tab.set_index(dims, inplace=True)

        dataset.add(tab)
