import pandas as pd

from etl.helpers import PathFinder, create_dataset, get_metadata_path
from etl.snapshot import Snapshot

paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    # load snapshot
    snap = Snapshot("fasttrack/2023-08-07/animal_lives_per_kilogram.csv")

    # load data
    tb = snap.read_csv()

    ####################################################################################################################
    # Fix indicators with mixed types.
    for column in ["days_per_kg_direct", "days_per_kg_total", "kilograms_per_animal_direct"]:
        tb[column] = tb[column].str.replace(",", "").astype(float)
    assert all(pd.api.types.is_numeric_dtype(tb[column]) for column in tb.drop(columns=["country", "year"]).columns)
    ####################################################################################################################

    # add table, update metadata from *.meta.yml and save
    ds = create_dataset(dest_dir, tables=[tb.set_index(["country", "year"])], default_metadata=snap.metadata)

    # override metadata if necessary
    meta_path = get_metadata_path(dest_dir).with_suffix(".override.yml")
    if meta_path.exists():
        ds.update_metadata(meta_path)

    ds.save()
