#
#  datasets.py
#

from os.path import join, isdir, exists
from os import mkdir
from dataclasses import dataclass
import shutil
from typing import Optional

from etl import tables
from etl.properties import metadata_property
from etl.meta import DatasetMeta


@dataclass
class Dataset:
    """
    A dataset is a folder full of data tables, with metadata available at `index.json`.
    """

    path: str
    metadata: "DatasetMeta"

    def __init__(self, path: str) -> None:
        self.path = path
        self.metadata = DatasetMeta.load(self._index_file)

    @classmethod
    def create_empty(
        cls, path: str, metadata: Optional["DatasetMeta"] = None
    ) -> "Dataset":
        if isdir(path):
            if not exists(join(path, "index.json")):
                raise Exception(f"refuse to overwrite non-dataset dir at: {path}")
            shutil.rmtree(path)

        mkdir(path)

        index_file = join(path, "index.json")
        DatasetMeta().save(index_file)

        return Dataset(path)

    def add(self, table: tables.Table) -> None:
        "Add this table to the dataset by saving it in the dataset's folder."
        if not table.name:
            raise ValueError("table must be named to be added to a dataset")

        table_filename = join(self.path, table.name + ".feather")
        table.to_feather(table_filename)

    def __getitem__(self, name: str) -> tables.Table:
        table_filename = join(self.path, name + ".feather")
        if not exists(table_filename):
            raise KeyError(name)

        return tables.Table.read_feather(table_filename)

    def __contains__(self, name: str) -> bool:
        table_filename = join(self.path, name + ".feather")
        return exists(table_filename)

    @property
    def _index_file(self) -> str:
        return join(self.path, "index.json")

    def save(self) -> None:
        self.metadata.save(self._index_file)


for k in DatasetMeta.__dataclass_fields__:  # type: ignore
    setattr(Dataset, k, metadata_property(k))
