"""The functions below are a bit more specific to this step, so maybe harder to generalize."""

from typing import Any, Dict, List, Optional, Union, cast

from etl.collection.explorer import Explorer
from etl.helpers import PathFinder


class ExplorerCreator:
    """This class is particular to this step.

    This step relies on two datasets that are particular. One contains just estimates (1950-2023), and the other contains projections (1950-2100).
    """

    def __init__(self, paths, ds, ds_proj):
        self.paths: PathFinder = paths
        self.ds = ds
        self.ds_proj = ds_proj
        self.tbs = {"proj": {}, "estimates": {}}

    @property
    def all_tables(self):
        return [tt for t in self.tbs.values() for tt in t.values()]

    def table(self, table_name: str):
        if table_name not in self.tbs:
            self.tbs["estimates"][table_name] = self.ds.read(table_name, load_data=False)
        return self.tbs["estimates"][table_name]

    def table_proj(self, table_name: str):
        if table_name not in self.tbs:
            self.tbs["proj"][table_name] = self.ds_proj.read(table_name, load_data=False)
        return self.tbs["proj"][table_name]

    def create_manual(self, config: Dict[str, Any], **kwargs) -> Explorer:
        explorer = self.paths.create_collection(
            config=config,
            indicator_as_dimension=True,
            explorer=True,
            **kwargs,
        )
        return cast(Explorer, explorer)

    def create(
        self,
        table_name: str,
        dimensions: Dict[str, Union[List[str], str]],
        dimensions_proj: Optional[Dict[str, Union[List[str], str]]] = None,
        **kwargs,
    ) -> Explorer:
        """Creates an explorer based on `tb` (1950-2023) and `tb_proj` (1950-2100)."""
        self.paths.log.info(f"Creating explorer for {table_name}")

        if "config" not in kwargs:
            raise ValueError("The config is required to create the explorer. Please provide it in the kwargs.")

        # Load tables
        tb = self.table(table_name)
        tb_proj = self.table_proj(table_name)

        # Explorer with projections
        dimensions_ = {**dimensions, **(dimensions_proj or {"variant": ["medium", "high", "low"]})}

        explorer = self.paths.create_collection(
            tb=[tb, tb_proj],
            dimensions=[dimensions, dimensions_],
            indicator_as_dimension=True,
            explorer=True,
            **kwargs,
        )

        return explorer
