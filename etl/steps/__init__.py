#
#  __init__.py
#  steps
#
#  Re-exports from owid.etl.steps for backwards compatibility.
#  GrapherStep is defined here (not in owid.etl) because it's OWID-specific.
#
import os
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List

import structlog
from owid import catalog

# Re-export DAG utilities from owid.etl.dag
from owid.etl.dag import Graph as DAG  # noqa: F401
from owid.etl.dag import (
    filter_to_subgraph,  # noqa: F401
    graph_nodes,  # noqa: F401
    reverse_graph,  # noqa: F401
    to_dependency_order,  # noqa: F401
    traverse,  # noqa: F401
)
from owid.etl.steps import (
    DEFAULT_KEEP_MODULES,
    INSTANT_METADATA_DIFF,
    STEP_REGISTRY,
    DataStep,
    DataStepPrivate,
    ETagStep,
    ExportStep,
    GithubRepo,
    GithubStep,
    PrivateMixin,
    SnapshotStep,
    SnapshotStepPrivate,
    Step,
    checksum_file,
    compile_steps,
    extract_step_attributes,
    get_etag,
    isolated_env,
    load_from_uri,
    parse_step,
    register_step,
    run_module_run,
    select_dirty_steps,
    walk_files,
)
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from apps.chart_sync.admin_api import AdminAPI
from etl import config
from etl.config import OWID_ENV
from etl.db import get_engine
from etl.grapher import helpers as gh

log = structlog.get_logger()


@register_step("grapher")
class GrapherStep(Step):
    """
    A step which ingests data from grapher channel into a local mysql database.

    If the dataset with the same short name already exists, it will be updated.
    All variables and sources related to the dataset
    """

    path: str
    data_step: DataStep
    dependencies: List[Step]

    def __init__(self, path: str, dependencies: List[Step]) -> None:
        # GrapherStep should have exactly one DataStep dependency
        assert len(dependencies) == 1
        assert path == dependencies[0].path
        assert isinstance(dependencies[0], DataStep)
        self.dependencies = dependencies
        self.path = path
        self.data_step = dependencies[0]

    def __str__(self) -> str:
        return f"grapher://{self.path}"

    @property
    def version(self) -> str:
        # channel / namspace / version / dataset
        return self.path.split("/")[2]

    @property
    def dataset(self) -> catalog.Dataset:
        """Grapher dataset we are upserting."""
        return self.data_step._output_dataset

    def is_dirty(self) -> bool:
        import etl.grapher.to_db as db

        if self.data_step.is_dirty():
            return True

        # dataset exists, but it is possible that we haven't inserted everything into DB
        dataset = self.dataset
        return db.fetch_db_checksum(dataset) != self.checksum_input()

    def run(self) -> None:
        import etl.grapher.to_db as db

        if "DATA_API_ENV" not in os.environ:
            warnings.warn(f"DATA_API_ENV not set, using '{config.DATA_API_ENV}'")

        # save dataset to grapher DB
        dataset = self.dataset

        dataset.metadata = gh._adapt_dataset_metadata_for_grapher(dataset.metadata)

        engine = get_engine()
        admin_api = AdminAPI(OWID_ENV)

        assert dataset.metadata.namespace
        dataset_upsert_results = db.upsert_dataset(
            engine,
            dataset,
            dataset.metadata.namespace,
            dataset.metadata.sources,
        )

        # We sometimes get a warning, but it's unclear where it is coming from
        # Passing a BlockManager to Table is deprecated and will raise in a future version. Use public APIs instead.
        warnings.filterwarnings("ignore", category=DeprecationWarning)

        catalog_paths = []

        with ThreadPoolExecutor(max_workers=config.GRAPHER_INSERT_WORKERS) as thread_pool:
            futures = []
            verbose = True
            i = 0

            # Get checksums for all variables in a dataset
            preloaded_checksums = db.load_dataset_variables(dataset_upsert_results.dataset_id, engine)

            # NOTE: multiple tables will be saved under a single dataset, this could cause problems if someone
            # is fetching the whole dataset from data-api as they would receive all tables merged in a single
            # table. This won't be a problem after we introduce the concept of "tables"
            for table in dataset:
                assert not table.empty, f"table {table.metadata.short_name} is empty"

                # if SUBSET is set, only upsert matching variables
                if config.SUBSET:
                    cols_regex = config.SUBSET
                # if INSTANT is set, only upsert variables with changed metadata
                elif config.INSTANT:
                    dataset_name = dataset.m.short_name
                    table_name = table.metadata.short_name

                    # dataset wasn't run with instant, rerun it
                    if dataset_name not in INSTANT_METADATA_DIFF:
                        cols_regex = None
                    else:
                        instant_variables = INSTANT_METADATA_DIFF[dataset_name].get(table_name)
                        if not instant_variables:
                            # no changes in table, skip it
                            continue
                        else:
                            cols_regex = "|".join(instant_variables)
                else:
                    cols_regex = None

                if cols_regex:
                    cols = table.filter(regex=cols_regex).columns.tolist()
                    if not cols:
                        continue
                    cols += [c for c in table.columns if c in {"year", "date", "country"} and c not in cols]
                    table = table.loc[:, cols]

                table = gh._adapt_table_for_grapher(table, engine)

                # Validation
                db.check_table(table)

                # Upsert origins
                with Session(engine, expire_on_commit=False) as session:
                    db_origins = db.upsert_origins(session, table)

                for t in gh._yield_wide_table(table, na_action="drop"):
                    i += 1
                    assert len(t.columns) == 1
                    catalog_path = f"{self.path}/{table.metadata.short_name}#{t.columns[0]}"
                    catalog_paths.append(catalog_path)

                    # stop logging to stop cluttering logs
                    if i > 20 and verbose:
                        verbose = False
                        log.info("showing only the first 20 logs")

                    # generate table with entity_id, year and value for every column
                    futures.append(
                        thread_pool.submit(
                            db.upsert_table,
                            engine,
                            admin_api,
                            t,
                            dataset_upsert_results,
                            catalog_path=catalog_path,
                            dimensions=(t.iloc[:, 0].metadata.additional_info or {}).get("dimensions"),
                            checksums=preloaded_checksums.get(catalog_path, {}),
                            db_origins=[db_origins[origin] for origin in t.iloc[:, 0].origins],
                            verbose=verbose,
                        )
                    )

            # wait for all tables to be inserted
            [future.result() for future in as_completed(futures)]

        # If INSTANT flag is set, don't clean ghost variables, but update the checksum (with _instant suffix)
        if INSTANT_METADATA_DIFF:
            db.set_dataset_checksum_and_editedAt(dataset_upsert_results.dataset_id, self.checksum_input())

        # If filtering is on, don't set checksum. Allow the next ETL run to set it
        elif config.SUBSET:
            pass

        # Otherwise, clean up ghost resources and set checksum
        else:
            # cleaning up ghost resources could be unsuccessful if someone renamed short_name of a variable
            # and remapped it in chart-sync. In that case, we cannot delete old variables because they are still
            # needed for remapping. However, we can delete it on next ETL run
            success = self._cleanup_ghost_resources(
                engine, dataset_upsert_results, catalog_paths, list(dataset_upsert_results.source_ids.values())
            )

            # set checksum and updatedAt timestamps after all data got inserted
            if success:
                checksum = self.checksum_input()
            # if cleanup was not successful, don't set checksum and let ETL rerun it on its next try
            else:
                checksum = "to_be_rerun"

            db.set_dataset_checksum_and_editedAt(dataset_upsert_results.dataset_id, checksum)

    def checksum_input(self) -> str:
        return self.data_step.checksum_input()

    def checksum_output(self) -> str:
        """Checksum of a grapher step is the same as checksum of the underyling data://grapher step."""
        return self.data_step.checksum_input()

    @classmethod
    def _cleanup_ghost_resources(
        cls,
        engine: Engine,
        dataset_upsert_results,
        catalog_paths: List[str],
        dataset_upserted_source_ids: List[int],
    ) -> bool:
        """
        Cleanup all ghost variables that weren't upserted
        NOTE: we can't just remove all dataset variables before starting this step because
        there could be charts that use them and we can't remove and recreate with a new ID

        Return True if cleanup was successfull, False otherwise.
        """
        import etl.grapher.to_db as db

        success = True

        # get all variables from the dataset
        db_variables = db.fetch_dataset_variables(dataset_upsert_results.dataset_id, engine)

        # delete those not in catalog_paths
        for db_var in db_variables:
            if db_var.catalogPath not in catalog_paths:
                log.warning(
                    "grapher_step.ghost_variable",
                    id=db_var.id,
                    catalogPath=db_var.catalogPath,
                )
                try:
                    db.cleanup_ghost_variables(engine, [db_var.id])
                except Exception as e:
                    log.warning(
                        "grapher_step.ghost_variable.failed",
                        id=db_var.id,
                        catalogPath=db_var.catalogPath,
                        error=str(e),
                    )
                    success = False

        # delete ghost sources (sources that were upserted, but don't belong to any variable)
        db.cleanup_ghost_sources(engine, dataset_upserted_source_ids)

        # delete ghost origins (origins that don't belong to any variable)
        db.cleanup_ghost_origins(engine)

        return success


__all__ = [
    # Step types
    "Step",
    "DataStep",
    "DataStepPrivate",
    "SnapshotStep",
    "SnapshotStepPrivate",
    "GrapherStep",
    "ExportStep",
    "ETagStep",
    "GithubStep",
    "GithubRepo",
    "PrivateMixin",
    # Registry
    "STEP_REGISTRY",
    "register_step",
    # Functions
    "compile_steps",
    "parse_step",
    "extract_step_attributes",
    "load_from_uri",
    "select_dirty_steps",
    "get_etag",
    "checksum_file",
    "walk_files",
    "isolated_env",
    "run_module_run",
    # Constants
    "DEFAULT_KEEP_MODULES",
    "INSTANT_METADATA_DIFF",
    # DAG utilities
    "DAG",
    "filter_to_subgraph",
    "graph_nodes",
    "reverse_graph",
    "to_dependency_order",
    "traverse",
]
