from pathlib import Path

import structlog
from owid.catalog import Dataset

from etl.paths import DATA_DIR

log = structlog.get_logger()


def run(dest_dir: str) -> None:
    version = Path(__file__).parent.stem
    fname = Path(__file__).stem
    namespace = Path(__file__).parent.parent.stem

    garden_dataset = Dataset(DATA_DIR / f"garden/{namespace}/{version}/{fname}")
    dataset = Dataset.create_empty(dest_dir, garden_dataset.metadata)
    dataset.save()

    fname = Path(__file__).stem
    table = garden_dataset[fname]
    assert len(table.metadata.primary_key) == 2

    table.reset_index(inplace=True)
    dataset.add(table)
