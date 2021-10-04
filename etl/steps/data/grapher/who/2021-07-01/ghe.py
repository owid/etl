from owid import catalog
from collections.abc import Iterable
from itertools import product
import pandas as pd


def to_grapher_table(table: catalog.Table) -> Iterable[catalog.Table]:
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
    dimension_values_to_flatten = [
        table[dimension].unique() for dimension in expected_primary_keys[2:]
    ]
    # This is supposed to create the N-dimensional cross product as tuples of all
    # values in all dimensions except year and country_code and iterate over these tuples
    for dimension_tuple in product(dimension_values_to_flatten):
        # This is supposed to fix all dimensions except year and country_code to one excact value,
        # collapsing this part of the dataframe so that for exactly this dimension tuple all countries
        # and years are retrained and a Table with this subset is yielded
        idx = pd.IndexSlice
        yield table.loc[
            idx[:, :, dimension_tuple[0], dimension_tuple[1], dimension_tuple[2]], :
        ]
