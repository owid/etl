from owid import catalog
from collections.abc import Iterable
import pandas as pd

from etl.command import DATA_DIR


def get_grapher_tables() -> Iterable[catalog.Table]:
    dataset = catalog.Dataset(DATA_DIR / "garden" / "who" / "2021-07-01" / "ghe")

    table = dataset["estimates"]

    expected_primary_keys = [
        "country_code",
        "year",
        "ghe_cause_title",
        "sex_code",
        "agegroup_code",
    ]
    if table.primary_key != expected_primary_keys:
        raise Exception(
            f"GHE Table to transform to grapher contained unexpected primary key dimensions: {table.primary_key} instead of {expected_primary_keys}"
        )

    # I tried to do this generically but itertools.product only does 2D cross product and here we need 3D
    for ghe_cause_title in table.index.unique(level="ghe_cause_title").values:
        for sex_code in table.index.unique(level="sex_code").values:
            for agegroup_code in table.index.unique(level="agegroup_code").values:
                print(f"{ghe_cause_title} - {sex_code} - {agegroup_code}")
                # This is supposed to fix all dimensions except year and country_code to one excact value,
                # collapsing this part of the dataframe so that for exactly this dimension tuple all countries
                # and years are retrained and a Table with this subset is yielded
                idx = pd.IndexSlice
                yield table.loc[idx[:, :, ghe_cause_title, sex_code, agegroup_code], :]
