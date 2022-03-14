import pandas as pd
from etl.db import get_engine
from owid.catalog import DatasetMeta, VariableMeta
from etl.steps.data.converters import convert_grapher_dataset, convert_grapher_variable
from typing import Tuple, Literal


class DBBackport:
    def __init__(self) -> None:
        self.engine = get_engine()

    def find_dataset(self, name: str) -> Tuple[int, DatasetMeta]:
        # TODO: use params instead of f-strings
        q = f"""
        select * from datasets
        where name = '{name}'
        """
        df = pd.read_sql(q, self.engine)
        assert len(df) == 1
        ds = dict(df.iloc[0])
        return ds["id"], convert_grapher_dataset(ds)

    def find_variables(
        self, dataset_id: int, where: str = "1 = 1"
    ) -> Tuple[list[int], list[VariableMeta]]:
        q = f"""
        select * from variables
        where datasetId = {dataset_id} and {where}
        """
        df = pd.read_sql(q, self.engine)
        return list(df["id"]), [
            convert_grapher_variable(v) for v in df.to_dict(orient="records")
        ]

    def find_values(
        self, variable_ids: list[int], format: Literal["wide", "long"] = "wide"
    ) -> pd.DataFrame:
        q = f"""
        select
            v.name as var_name,
            d.year,
            e.name as entity_name,
            CAST(d.value as DECIMAL(10,2)) as value
        from data_values as d
        join entities as e on e.id = d.entityId
        join variables as v on v.id = d.variableId
        where v.id in ({','.join(map(str, variable_ids))})
        """
        df: pd.DataFrame = pd.read_sql(q, self.engine)
        if format == "wide":
            df = df.pivot(
                index=["entity_name", "year"], columns="var_name", values="value"
            )
        return df
