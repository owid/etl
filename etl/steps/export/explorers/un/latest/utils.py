"""The functions below are a bit more specific to this step, so maybe harder to generalize."""

from typing import Any, Dict, List, Optional, Union

from etl.collections.beta import combine_explorers, create_explorer_experimental


class ExplorerCreator:
    """This class is particular to this step.

    This step relies on two datasets that are particular. One contains just estimates (1950-2023), and the other contains projections (1950-2100).
    """

    def __init__(self, paths, ds, ds_proj):
        self.paths = paths
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

    def create_manual(self, config: Dict[str, Any], **kwargs):
        explorer = create_explorer_experimental(self.paths, config, indicator_as_dimension=True, **kwargs)
        return explorer

    def create(
        self,
        table_name: str,
        dimensions: Dict[str, Union[List[str], str]],
        dimensions_proj: Optional[Dict[str, Union[List[str], str]]] = None,
        **kwargs,
    ):
        """Creates an explorer based on `tb` (1950-2023) and `tb_proj` (1950-2100)."""
        self.paths.log.info(f"Creating explorer for {table_name}")

        # Load tables
        tb = self.table(table_name)
        tb_proj = self.table_proj(table_name)

        # Explorer with estimates
        explorer = create_explorer_experimental(
            self.paths, tb=tb, dimensions=dimensions, indicator_as_dimension=True, **kwargs
        )

        # Explorer with projections
        if dimensions_proj is not None:
            dimensions.update(dimensions_proj)
        else:
            dimensions["variant"] = ["medium", "high", "low"]

        explorer_proj = create_explorer_experimental(
            self.paths, tb=tb_proj, dimensions=dimensions, indicator_as_dimension=True, **kwargs
        )

        explorer = combine_explorers(
            explorers=[explorer, explorer_proj],
            explorer_name=explorer.explorer_name,
            config=explorer.config,
        )

        return explorer
