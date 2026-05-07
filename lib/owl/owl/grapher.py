"""Bridge from Owl catalog datasets to Grapher MySQL.

This intentionally delegates to the existing ETL Grapher upsert machinery, so
Owl does not grow a parallel implementation of dataset/variable/origin upserts.
"""

from __future__ import annotations

import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed

from owid import catalog
from sqlalchemy.orm import Session
from tqdm import tqdm

from apps.chart_sync.admin_api import AdminAPI
from etl import config
from etl.config import OWID_ENV
from etl.db import get_engine
from etl.grapher import helpers as gh
from etl.grapher import model as gm
from etl.grapher import to_db as db
from owl.dataset import Dataset as OwlDataset
from owl.project import parse_step_file


def upsert_dataset(dataset: OwlDataset, *, workers: int | None = None) -> int:
    """Upsert an Owl dataset to Grapher using ETL's existing Grapher code.

    Args:
        dataset: Owl ``@Dataset`` object whose catalog output already exists.
        workers: Optional override for variable upload concurrency.

    Returns:
        Grapher dataset id.
    """
    dataset.run()

    ds = catalog.Dataset(dataset._data_path)
    ds.metadata = gh._adapt_dataset_metadata_for_grapher(ds.metadata)

    engine = get_engine()
    admin_api = AdminAPI(OWID_ENV)

    info = parse_step_file(dataset._source_file)
    namespace = ds.metadata.namespace or info.namespace
    grapher_path = f"grapher/{namespace}/{info.version}/{dataset.name}"

    dataset_upsert_result = db.upsert_dataset(engine, ds, namespace, ds.metadata.sources)
    preloaded_checksums = db.load_dataset_variables(dataset_upsert_result.dataset_id, engine)

    catalog_paths: list[str] = []
    uploaded_sources = list(dataset_upsert_result.source_ids.values())
    max_workers = workers or config.GRAPHER_INSERT_WORKERS

    warnings.filterwarnings("ignore", category=DeprecationWarning)

    with ThreadPoolExecutor(max_workers=max_workers) as thread_pool:
        futures = []
        verbose = True
        count = 0

        for table in tqdm(ds):
            assert not table.empty, f"table {table.metadata.short_name} is empty"
            table = gh._adapt_table_for_grapher(table, engine)
            db.check_table(table)

            with Session(engine, expire_on_commit=False) as session:
                db_origins = db.upsert_origins(session, table)

            for one_variable_table in gh._yield_wide_table(table, na_action="drop"):
                count += 1
                assert len(one_variable_table.columns) == 1
                variable_name = one_variable_table.columns[0]
                catalog_path = f"{grapher_path}/{table.metadata.short_name}#{variable_name}"
                catalog_paths.append(catalog_path)

                if count > 20 and verbose:
                    verbose = False

                futures.append(
                    thread_pool.submit(
                        db.upsert_table,
                        engine,
                        admin_api,
                        one_variable_table,
                        dataset_upsert_result,
                        catalog_path=catalog_path,
                        dimensions=(one_variable_table.iloc[:, 0].metadata.additional_info or {}).get("dimensions"),
                        checksums=preloaded_checksums.get(catalog_path, {}),
                        db_origins=[db_origins[origin] for origin in one_variable_table.iloc[:, 0].origins],
                        verbose=verbose,
                    )
                )

        [future.result() for future in as_completed(futures)]

    with Session(engine) as session:
        upserted_variable_ids = list(gm.Variable.catalog_paths_to_variable_ids(session, catalog_paths).values())

    cleanup_ok = db.cleanup_ghost_variables(engine, dataset_upsert_result.dataset_id, upserted_variable_ids)
    db.cleanup_ghost_sources(engine, dataset_upsert_result.dataset_id, uploaded_sources)
    db.set_dataset_checksum_and_editedAt(
        dataset_upsert_result.dataset_id, ds.checksum() if cleanup_ok else "to_be_rerun"
    )

    return dataset_upsert_result.dataset_id
