"""Bridge from Owl catalog datasets to Grapher MySQL.

This intentionally delegates to the existing ETL Grapher upsert machinery, so
Owl does not grow a parallel implementation of dataset/variable/origin upserts.
"""

from __future__ import annotations

import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed

from apps.chart_sync.admin_api import AdminAPI
from etl import config
from etl.config import OWID_ENV
from etl.db import get_engine
from etl.grapher import helpers as gh
from etl.grapher import to_db as db
from owid import catalog
from tqdm import tqdm

from owl.dataset import Dataset as OwlDataset
from owl.project import parse_step_file


def upsert_dataset(dataset: OwlDataset, *, workers: int | None = None) -> int:
    """Publish an Owl dataset to Grapher through the Admin API.

    Delegates to ETL's Grapher machinery, so Owl doesn't grow a parallel implementation.
    """
    dataset.run()

    ds = catalog.Dataset(dataset._data_path)
    ds.metadata = gh._adapt_dataset_metadata_for_grapher(ds.metadata)

    engine = get_engine()
    admin_api = AdminAPI(OWID_ENV)

    info = parse_step_file(dataset._source_file)
    namespace = ds.metadata.namespace or info.namespace
    grapher_path = f"grapher/{namespace}/{info.version}/{dataset.name}"

    warnings.filterwarnings("ignore", category=DeprecationWarning)

    prepared_by_path: dict[str, db.PreparedIndicator] = {}
    for table in tqdm(ds):
        assert not table.empty, f"table {table.metadata.short_name} is empty"
        table = gh._adapt_table_for_grapher(table, engine)
        db.check_table(table)
        for one_variable_table in gh._yield_wide_table(table, na_action="drop"):
            assert len(one_variable_table.columns) == 1
            catalog_path = f"{grapher_path}/{table.metadata.short_name}#{one_variable_table.columns[0]}"
            prepared_by_path[catalog_path] = db.prepare_indicator(
                one_variable_table,
                catalog_path=catalog_path,
                dimensions=(one_variable_table.iloc[:, 0].metadata.additional_info or {}).get("dimensions"),
            )

    declared = admin_api.put_dataset(
        grapher_path,
        gh._dataset_metadata_payload(ds.metadata),
        list(prepared_by_path),
    )
    if not db.blocked_indicators_allow_run(declared["blocked"]):
        return 0

    max_workers = workers or config.GRAPHER_INSERT_WORKERS
    published: dict[str, str] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as thread_pool:
        futures = []
        batch: list[db.PreparedIndicator] = []
        batch_bytes = 0
        for item in prepared_by_path.values():
            batch.append(item)
            batch_bytes += item.payload_bytes
            if batch_bytes >= config.GRAPHER_UPSERT_BATCH_BYTES:
                futures.append(thread_pool.submit(db.flush_indicator_batch, admin_api, grapher_path, batch))
                batch, batch_bytes = [], 0
        if batch:
            futures.append(thread_pool.submit(db.flush_indicator_batch, admin_api, grapher_path, batch))
        for future in as_completed(futures):
            published.update(future.result())

    admin_api.put_dataset_checksum(grapher_path, ds.checksum(), published)

    return 0
