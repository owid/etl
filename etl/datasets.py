#
#  datasets.py
#

from etl import tables
from etl.properties import metadata_property
from os.path import join, isdir, exists
from os import mkdir
from dataclasses import dataclass
import shutil
from typing import Any, Optional, Dict
import json

from dataclasses_json import dataclass_json


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
        DatasetMeta.create_empty(index_file)

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


@dataclass_json
@dataclass
class DatasetMeta:
    """
    The metadata for this entire dataset kept in JSON (e.g. mydataset/index.json).

    The number of fields is limited, but should handle everything that we get from
    Walden. There is a lot more opportunity to store more metadata at the table and
    the variable level.

    Metadata is eagerly saved to disk, i.e. synced to disk every time you change a field.
    """

    # path of the index.json file containing this metadata
    _filename: str
    _save_count: int = 0
    _initialised: bool = False

    # the metadata itself
    namespace: Optional[str] = None
    name: Optional[str] = None
    title: Optional[str] = None
    description: Optional[str] = None

    def __post_init__(self) -> None:
        super().__setattr__("_initialised", True)

    @classmethod
    def load(cls, filename: str) -> "DatasetMeta":
        with open(filename) as istream:
            doc = json.load(istream)

        doc["_filename"] = filename
        return DatasetMeta.from_dict(doc)  # type: ignore

    @classmethod
    def create_empty(cls, filename: str) -> "DatasetMeta":
        with open(filename, "w") as ostream:
            ostream.write("{}")

        return DatasetMeta.load(filename)

    def to_dict(self) -> Dict[str, Any]:
        # only keep non-null non-private variables
        return {k: v for k, v in self.to_dict().items() if not k.startswith("_") and v}

    def save(self) -> None:
        doc = self.to_dict()

        with open(self._filename, "w") as ostream:
            json.dump(doc, ostream, indent=2)

        # for debugging
        super().__setattr__("_save_count", self._save_count + 1)

    def __setattr__(self, name: str, value: Any) -> None:
        super().__setattr__(name, value)

        # save to disk again every time you set an attribute
        if self._initialised:
            self.save()

    def __eq__(self, rhs: object) -> bool:
        return isinstance(rhs, DatasetMeta) and self.to_dict() == rhs.to_dict()


for k in DatasetMeta.__dataclass_fields__:  # type: ignore
    setattr(Dataset, k, metadata_property(k))
