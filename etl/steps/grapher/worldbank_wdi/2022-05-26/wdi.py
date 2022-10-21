from pathlib import Path

import structlog
from owid.catalog import Dataset, Source

from etl.paths import DATA_DIR

log = structlog.get_logger()


def run(dest_dir: str) -> None:
    version = Path(__file__).parent.stem
    fname = Path(__file__).stem
    namespace = Path(__file__).parent.parent.stem

    garden_dataset = Dataset(DATA_DIR / f"garden/{namespace}/{version}/{fname}")
    dataset = Dataset.create_empty(dest_dir, garden_dataset.metadata)
    # every variable has its own source, dataset source won't be visible anywhere in the admin
    dataset.metadata.sources = [
        Source(
            name="World Bank",
            url="https://datacatalog.worldbank.org/search/dataset/0037712/World-Development-Indicators",
            source_data_url="http://databank.worldbank.org/data/download/WDI_csv.zip",
        )
    ]

    dataset.save()

    fname = Path(__file__).stem
    table = garden_dataset[fname]
    assert len(table.metadata.primary_key) == 2

    table.reset_index(inplace=True)
    dataset.add(table)
