import json
from pathlib import Path
from typing import Dict, cast

from owid.catalog import Dataset

from etl.paths import DATA_DIR

MAPPING_FILE = Path(__file__).with_suffix(".mapping.json")


def run(dest_dir: str) -> None:
    harmonize_countries("meadow/un/2019/un_wpp", dest_dir)


def harmonize_countries(source_ds_path: str, dest_dir: str) -> None:
    """
    Harmonize the country field of every table in the source dataset.
    """
    # this is deliberately done in a generic way as a demonstration
    source_ds = Dataset(DATA_DIR / source_ds_path)
    mapping = load_mapping()

    ds = Dataset.create_empty(dest_dir, metadata=source_ds.metadata)
    ds.metadata.short_name = "un_wpp"
    ds.metadata.namespace = "un"

    for table in source_ds:
        # harmonize this specific table
        names = table.index.names
        table = table.reset_index()

        # these tables don't need harmonization
        if table.metadata.short_name in ("location_codes", "variant_codes"):
            ds.add(table)
            continue

        # drop locations with suffix `(and dependencies)`
        table = table[~table.location.str.endswith("(and dependencies)")]

        # continents are duplicated for unknown reason
        table = table.drop_duplicates()

        dimensions = [n for n in names if n != "location"]

        # harmonize countries; drop locations without a match (typically WB defined regions)
        table = (
            table.assign(country=table.location.map(mapping))
            .dropna(subset=["country"])
            .drop(["location"], axis=1)
            .set_index(["country"] + dimensions)
        )

        # make sure we don't have duplicate countries
        assert table[table.index.duplicated()].empty

        ds.add(table)

    ds.save()


def load_mapping() -> Dict[str, str]:
    country_mapping_file = MAPPING_FILE
    with open(country_mapping_file) as istream:
        mapping = json.load(istream)

    return cast(Dict[str, str], mapping)
