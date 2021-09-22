#
#  data/example.py
#

from owid.catalog import Dataset


def run(dest_dir: str) -> None:
    Dataset.create_empty(dest_dir)
