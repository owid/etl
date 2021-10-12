#
#  covid19.py
#  owid/latest/key_indicators
#

from owid.catalog import Dataset


def run(dest_dir: str) -> None:
    Dataset.create_empty(dest_dir)
