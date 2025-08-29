#
#  helpers.py
#  etl
#
import re
import time
from functools import cache
from pathlib import Path
from typing import Any, Callable, Iterable, Literal, Optional, overload

import deprecated
import pandas as pd
import structlog
from owid import catalog
from owid.catalog import CHANNEL, DatasetMeta, Table
from owid.catalog.datasets import DEFAULT_FORMATS, FileFormat
from owid.catalog.meta import SOURCE_EXISTS_OPTIONS
from owid.catalog.tables import (
    combine_tables_description,
    combine_tables_title,
    combine_tables_update_period_days,
    get_unique_licenses_from_tables,
    get_unique_sources_from_tables,
)
from owid.datautils.common import ExceptionFromDocstring, ExceptionFromDocstringWithKwargs

from etl import paths
from etl.collection import Collection, CollectionSet
from etl.collection.core.create import Listable, create_collection
from etl.collection.explorer import Explorer, ExplorerLegacy, create_explorer_legacy
from etl.dag_helpers import load_dag
from etl.data_helpers.geo import RegionAggregator, Regions
from etl.grapher.helpers import grapher_checks
from etl.snapshot import Snapshot, SnapshotMeta

log = structlog.get_logger()


__all__ = ["grapher_checks"]


def _set_metadata_from_dest_dir(ds: catalog.Dataset, dest_dir: str | Path) -> catalog.Dataset:
    """Set channel, namespace, version and short_name from the destination directory."""
    pattern = (
        r"\/"
        + r"\/".join(
            [
                "(?P<channel>[^/]*)",
                "(?P<namespace>[^/]*)",
                r"(?P<version>\d{4}\-\d{2}\-\d{2}|\d{4}|latest)",
                "(?P<short_name>.*?)",
            ]
        )
        + "$"
    )

    match = re.search(pattern, str(dest_dir))
    assert match, f"Could not parse path {str(dest_dir)}"

    for k, v in match.groupdict().items():
        setattr(ds.metadata, k, v)

    return ds


def create_dataset(
    dest_dir: str | Path,
    tables: Iterable[catalog.Table],
    default_metadata: SnapshotMeta | catalog.DatasetMeta | None = None,
    underscore_table: bool = True,
    camel_to_snake: bool = False,
    long_to_wide: bool | None = None,
    formats: list[FileFormat] = DEFAULT_FORMATS,
    check_variables_metadata: bool = True,
    run_grapher_checks: bool = True,
    yaml_params: dict[str, Any] | None = None,
    if_origins_exist: SOURCE_EXISTS_OPTIONS = "replace",
    errors: Literal["ignore", "warn", "raise"] = "raise",
    repack: bool = True,
) -> catalog.Dataset:
    """Create a dataset and add a list of tables. The dataset metadata is inferred from
    default_metadata and the dest_dir (which is in the form `channel/namespace/version/short_name`).
    If there's an accompanying metadata file (i.e. `[short_name].meta.yml`), it will be used to
    update the existing metadata.

    One of the benefits of using this function is that it you don't have to set any of the
    channel/namespace/version/short_name manually.

    :param dest_dir: The destination directory for the dataset, usually argument of `run` function.
    :param tables: A list of tables to add to the dataset.
    :param default_metadata: The default metadata to use for the dataset, could be either SnapshotMeta or DatasetMeta.
    :param underscore_table: Whether to underscore the table name before adding it to the dataset.
    :param camel_to_snake: Whether to convert camel case to snake case for the table name.
    :param long_to_wide: Convert data in long format (with dimensions) to wide format (flattened).
    :param check_variables_metadata: Check that all variables in tables have metadata; raise a warning otherwise.
    :param run_grapher_checks: Run grapher checks on the dataset, only applies to grapher channel.
    :param yaml_params: Dictionary of parameters that can be used in the metadata yaml file.
    :param if_origins_exist: What to do if origins already exist in the dataset metadata.
    :param repack: Repack dataframe before adding it to the dataset.

    Usage:
        ds = create_dataset(dest_dir, [table_a, table_b], default_metadata=snap.metadata)
        ds.save()
    """
    from etl.steps.data.converters import convert_snapshot_metadata

    if default_metadata is None:
        # Get titles and descriptions from the tables.
        # Note: If there are different titles or description, the result will be None.
        title = combine_tables_title(tables=tables)
        description = combine_tables_description(tables=tables)
        # If not defined, gather origins and licenses from the metadata of the tables.
        licenses = get_unique_licenses_from_tables(tables=tables)
        if any(["origins" in table[column].metadata.to_dict() for table in tables for column in table.columns]):
            # If any of the variables contains "origins" this means that it is a recently created dataset.
            update_period_days_combined = combine_tables_update_period_days(tables=tables)
            default_metadata = DatasetMeta(
                licenses=licenses, title=title, description=description, update_period_days=update_period_days_combined
            )
        else:
            # None of the variables includes "origins", which means it is an old dataset, with "sources".
            sources = get_unique_sources_from_tables(tables=tables)
            default_metadata = DatasetMeta(licenses=licenses, sources=sources, title=title, description=description)
    elif isinstance(default_metadata, SnapshotMeta):
        # convert snapshot SnapshotMeta to DatasetMeta
        default_metadata = convert_snapshot_metadata(default_metadata)

    if check_variables_metadata:
        catalog.tables.check_all_variables_have_metadata(tables=tables)

    # create new dataset with new metadata
    ds = catalog.Dataset.create_empty(dest_dir, metadata=default_metadata)

    ds = _set_metadata_from_dest_dir(ds, dest_dir)

    meta_path = get_metadata_path(str(dest_dir))

    # Raise an error if there's a variable in YAML that is not in the dataset
    extra_variables = "raise"

    # add tables to dataset
    used_short_names = set()
    for table in tables:
        if underscore_table:
            table = catalog.utils.underscore_table(table, camel_to_snake=camel_to_snake)
        if table.metadata.short_name in used_short_names:
            raise ValueError(f"Table short name `{table.metadata.short_name}` is already in use.")
        used_short_names.add(table.metadata.short_name)

        from etl.grapher import helpers as gh

        # Default long_to_wide for grapher channel is true
        if long_to_wide is None:
            long_to_wide = ds.metadata.channel == "grapher"

        # Expand long to wide
        if long_to_wide:
            if ds.metadata.channel != "grapher":
                log.warning("It is recommended to use long_to_wide=True only in the grapher channel")

            dim_names = set(table.index.names) - {"country", "code", "year", "date", None}
            if dim_names:
                # First pass to update metadata from YAML
                if meta_path.exists():
                    table.update_metadata_from_yaml(meta_path, table.m.short_name)  # type: ignore
                log.info("long_to_wide.start", shape=table.shape, short_name=table.m.short_name, dim_names=dim_names)
                t = time.time()
                table = gh.long_to_wide(table)
                log.info("long_to_wide.end", shape=table.shape, short_name=table.m.short_name, t=time.time() - t)

                # Ignore extra variables for the following pass of metadata
                extra_variables = "ignore"
            else:
                log.info("long_to_wide.skip", short_name=table.m.short_name)

        ds.add(table, formats=formats, repack=repack)

    if meta_path.exists():
        ds.update_metadata(
            meta_path,
            if_origins_exist=if_origins_exist,
            yaml_params=yaml_params,
            errors=errors,
            extra_variables=extra_variables,
        )

    # another override YAML file with higher priority
    meta_override_path = get_metadata_path(str(dest_dir)).with_suffix(".override.yml")
    if meta_override_path.exists():
        ds.update_metadata(meta_override_path, if_origins_exist=if_origins_exist, extra_variables=extra_variables)

    # run grapher checks
    if ds.metadata.channel == "grapher" and run_grapher_checks:
        grapher_checks(ds)

    return ds


def get_metadata_path(dest_dir: str) -> Path:
    N_archive = PathFinder(str(paths.STEP_DIR / "archive" / Path(dest_dir).relative_to(Path(dest_dir).parents[3])))
    if N_archive.metadata_path.exists():
        N = N_archive
    else:
        N = PathFinder(str(paths.STEP_DIR / "data" / Path(dest_dir).relative_to(Path(dest_dir).parents[3])))
    return N.metadata_path


class CurrentFileMustBeAStep(ExceptionFromDocstring):
    """Current file must be an ETL step."""


class CurrentStepMustBeInDag(ExceptionFromDocstring):
    """Current step must be listed in the dag."""


class NoMatchingStepsAmongDependencies(ExceptionFromDocstringWithKwargs):
    """No steps found among dependencies of current ETL step, that match the given specifications.
    Add those missing datasets as dependencies of the current step in the DAG."""


class MultipleMatchingStepsAmongDependencies(ExceptionFromDocstringWithKwargs):
    """Multiple steps found among dependencies of current ETL step, that match the given specifications."""


class UnknownChannel(ExceptionFromDocstring):
    """Unknown channel name. Valid channels are 'examples', 'snapshot', 'meadow', 'garden', or 'grapher'."""


class WrongStepName(ExceptionFromDocstring):
    """Wrong step name. If this step was in the dag, it should be corrected."""


# loading DAG can take up to 1 second, so cache it
load_dag_cached = cache(load_dag)


class PathFinder:
    """Helper object with naming conventions. It uses your module path (__file__) and
    extracts from it commonly used attributes like channel / namespace / version / short_name or
    paths to datasets from different channels.

    Usage:
        paths = PathFinder(__file__)
        ds_garden = paths.garden_dataset
    """

    def __init__(self, __file__: str, is_private: bool | None = None):
        self.f = Path(__file__)

        # Lazy load dag when needed.
        self._dag = None

        # Check if this is a snapshot file or a regular ETL step
        self._is_snapshot_file = self.f.as_posix().startswith(paths.SNAPSHOTS_DIR.as_posix())

        if self._is_snapshot_file:
            # For snapshot files, validate they're in the snapshots directory
            if not self.f.as_posix().startswith(paths.SNAPSHOTS_DIR.as_posix()):
                raise CurrentFileMustBeAStep
        else:
            # Current file should be a data step.
            if not self.f.as_posix().startswith(paths.STEP_DIR.as_posix()):
                raise CurrentFileMustBeAStep

            # It could be either called from a module with short_name.py or __init__.py inside short_name/ dir.
            if len(self.f.relative_to(paths.STEP_DIR).parts) == 6:
                self.f = self.f.parent

        # If is_private is not specified, start by assuming the current step is public.
        # Then, if the step is not found in the dag, but it's found as private, is_private will be set to True.
        if is_private is None:
            self.is_private = False

        # Default logger
        self.log = structlog.get_logger(step=f"{self.namespace}/{self.channel}/{self.version}/{self.short_name}")

    @property
    def dag(self):
        """Lazy loading of DAG."""
        if self._dag is None:
            if "/archive/" in str(self.f):
                self._dag = load_dag_cached(paths.DAG_ARCHIVE_FILE)
            else:
                self._dag = load_dag_cached()
        return self._dag

    @property
    def step_type(self) -> str:
        if self._is_snapshot_file:
            return "snapshot"
        return self.f.parent.parent.parent.parent.name

    @property
    def channel(self) -> CHANNEL:
        if self._is_snapshot_file:
            return "snapshot"  # type: ignore
        return self.f.parent.parent.parent.name  # type: ignore

    @property
    def namespace(self) -> str:
        return self.f.parent.parent.name

    @property
    def version(self) -> str:
        return self.f.parent.name

    @property
    def short_name(self) -> str:
        return self.f.stem

    @property
    def country_mapping_path(self) -> Path:
        return self.directory / (self.short_name + ".countries.json")

    @property
    def excluded_countries_path(self) -> Path:
        return self.directory / (self.short_name + ".excluded_countries.json")

    @property
    def metadata_path(self) -> Path:
        return self.directory / (self.short_name + ".meta.yml")

    @property
    def collection_path(self) -> Path:
        """TODO: worth aligning with `metadata_path` (add missing '.meta'), maybe even just deprecate this and use `metadata_path`."""
        assert "multidim" in str(self.directory), "MDIM path is only available for multidim steps!"
        return self.directory / (self.short_name + ".yml")

    @property
    def config_path(self) -> Path:
        """Config file. Used in `multidim` and `explorer` ETL steps."""
        # assert "multidim" in str(self.directory), "MDIM path is only available for multidim steps!"
        return self.directory / (self.short_name + ".config.yml")

    @property
    def directory(self) -> Path:
        # If the current file is a directory, it's a step with multiple files.
        if self.f.is_dir():
            return self.f
        else:
            return self.f.parent

    @property
    def meadow_dataset(self) -> catalog.Dataset:
        return catalog.Dataset(paths.DATA_DIR / f"meadow/{self.namespace}/{self.version}/{self.short_name}")

    @property
    def garden_dataset(self) -> catalog.Dataset:
        return catalog.Dataset(paths.DATA_DIR / f"garden/{self.namespace}/{self.version}/{self.short_name}")

    @property
    def regions(self):
        """Get Regions helper for the specific use of an ETL step."""
        if not hasattr(self, "_regions"):
            try:
                ds_regions = self.load_dataset("regions")
            except NoMatchingStepsAmongDependencies:
                ds_regions = None
            try:
                ds_income_groups = self.load_dataset("income_groups")
            except NoMatchingStepsAmongDependencies:
                ds_income_groups = None
            try:
                ds_population = self.load_dataset("population")
            except NoMatchingStepsAmongDependencies:
                ds_population = None

            self._regions = Regions(
                ds_regions=ds_regions,
                ds_income_groups=ds_income_groups,
                ds_population=ds_population,
                countries_file=self.country_mapping_path,
                auto_load_datasets=False,
            )
        return self._regions

    def region_aggregator(
        self,
        regions: list[str] | dict[str, Any] | None = None,
        index_columns: list[str] | None = None,
        country_col: str = "country",
        year_col: str = "year",
        population_col: str = "population",
    ) -> RegionAggregator:
        """Create a RegionAggregator that will be used on a specific table."""
        return RegionAggregator(
            regions=regions,
            ds_regions=self.regions.ds_regions,
            ds_income_groups=self.regions._ds_income_groups,
            ds_population=self.regions._ds_population,
            regions_all=self.regions.regions_all,
            index_columns=index_columns,
            country_col=country_col,
            year_col=year_col,
            population_col=population_col,
        )

    @property
    def snapshot_dir(self) -> Path:
        return paths.SNAPSHOTS_DIR / self.namespace / self.version

    @property
    def step_name(self) -> str:
        """Return step name."""
        return self.create_step_name(
            short_name=self.short_name,
            channel=self.channel,  # type: ignore
            namespace=self.namespace,
            version=self.version,
            step_type=self.step_type,
        )

    @staticmethod
    def create_step_name(
        short_name: str,
        channel: CHANNEL | None = None,
        namespace: str | None = None,
        version: int | str | None = None,
        is_private: bool | None = False,
        step_type: str = "data",
    ) -> str:
        """Create the step name (as it appears in the dag) given its attributes.

        If attributes are not specified, return a regular expression that should be able to find the specified step.
        """
        if namespace is None:
            # If namespace is not specified, catch any name that does not contain "/".
            namespace = r"[^/]+"

        if version is None:
            # If version is not specified, catch any version, which could be either a date, a year, or "latest".
            version = r"(?:\d{4}\-\d{2}\-\d{2}|\d{4}|latest)"

        # Suffix to add to, e.g. "data" if step is private.
        is_private_suffix = "-private" if is_private else ""

        if step_type == "export":
            step_name = f"export://{channel}/{namespace}/{version}/{short_name}"
        elif channel == "snapshot":
            # match also on snapshot short_names without extension
            step_name = f"{channel}{is_private_suffix}://{namespace}/{version}/{short_name}(.\\w+)?"
        elif channel in CHANNEL.__args__:
            step_name = f"data{is_private_suffix}://{channel}/{namespace}/{version}/{short_name}"
        elif channel is None:
            step_name = rf"(?:snapshot{is_private_suffix}:/|data{is_private_suffix}://meadow|data{is_private_suffix}://garden|data{is_private_suffix}://grapher|data://explorers)/{namespace}/{version}/{short_name}$"
        else:
            raise UnknownChannel

        return step_name

    def _create_current_step_name(self):
        return self.create_step_name(
            short_name=self.short_name,
            channel=self.channel,
            namespace=self.namespace,
            version=self.version,
            is_private=self.is_private,
            step_type=self.step_type,
        )

    @staticmethod
    def _get_attributes_from_step_name(step_name: str) -> dict[str, str]:
        """Get attributes (channel, namespace, version, short name and is_private) from the step name (as it appears in the dag)."""
        channel_type, path = step_name.split("://")
        if channel_type.startswith(("snapshot",)):
            channel = channel_type
            namespace, version, short_name = path.split("/")
        elif channel_type.startswith(("data", "export")):
            channel, namespace, version, short_name = path.split("/")
        else:
            raise WrongStepName

        if channel_type.endswith("-private"):
            is_private = True
            channel = channel.replace("-private", "")
        else:
            is_private = False

        attributes = {
            "channel": channel,
            "namespace": namespace,
            "version": version,
            "short_name": short_name,
            "is_private": is_private,
        }

        return attributes

    @property
    def step(self) -> str:
        # First assume current step is public.
        _step = self._create_current_step_name()
        if _step in self.dag:
            return _step
        else:
            # If step is not found in the dag, check if it is private.
            self.is_private = True
            _step = self._create_current_step_name()
            if _step not in self.dag:
                raise CurrentStepMustBeInDag(_step)
            else:
                return _step

    @property
    def dest_dir(self) -> Path:
        # Local destination folder, where the data generated by this step will be saved.
        # For typical data steps, this folder will be "data/..." (from the base ETL folder).
        # For snapshots, this should be the snapshots folder.
        if self._is_snapshot_file:
            return self.f.parent
        return paths.BASE_DIR / self.f.relative_to(paths.STEP_DIR).with_suffix("")

    def side_file(self, filename: str) -> Path:
        return self.directory / filename

    @property
    def dependencies(self) -> set[str]:
        # Current step should be in the dag.
        if self.step not in self.dag:
            raise CurrentStepMustBeInDag

        return self.dag[self.step]

    def get_dependency_step_name(
        self,
        short_name: str,
        step_type: str = "data",
        channel: CHANNEL | None = None,
        namespace: str | None = None,
        version: str | int | None = None,
        is_private: bool | None = None,
    ) -> str:
        """Get dependency step name (as it appears in the dag) given its attributes (at least its short name)."""

        pattern = self.create_step_name(
            step_type=step_type,
            channel=channel,
            namespace=namespace,
            version=version,
            short_name=short_name,
            is_private=is_private,
        )
        deps = self.dependencies
        matches = _match_dependencies(pattern, deps)

        # If no step was found and is_private was not specified, try again assuming step is private.
        if (len(matches) == 0) and (is_private is None):
            pattern = self.create_step_name(
                step_type=step_type,
                channel=channel,
                namespace=namespace,
                version=version,
                short_name=short_name,
                is_private=True,
            )
            matches = _match_dependencies(pattern, self.dependencies)

        # If not step was found and channel is "grapher", try again assuming this is a grapher://grapher step.
        if (len(matches) == 0) and (channel == "grapher"):
            pattern = self.create_step_name(
                step_type=step_type,
                channel="grapher",
                namespace=namespace,
                version=version,
                short_name=short_name,
                is_private=is_private,
            )
            matches = _match_dependencies(pattern, self.dependencies)

        if len(matches) == 0:
            raise NoMatchingStepsAmongDependencies(step_name=self.step_name)
        elif len(matches) > 1:
            raise MultipleMatchingStepsAmongDependencies(step_name=self.step_name)

        dependency = next(iter(matches))

        return dependency

    def load_dependency(
        self,
        short_name: str,
        step_type: str = "data",
        channel: CHANNEL | None = None,
        namespace: str | None = None,
        version: str | int | None = None,
        is_private: bool | None = None,
    ) -> catalog.Dataset | Snapshot | CollectionSet:
        """Load a (dataset or export) dependency, given its attributes (at least its short name)."""
        dependency_step_name = self.get_dependency_step_name(
            step_type=step_type,
            short_name=short_name,
            channel=channel,
            namespace=namespace,
            version=version,
            is_private=is_private,
        )
        dependency = self._get_attributes_from_step_name(step_name=dependency_step_name)
        if dependency["channel"] == "snapshot":
            dataset = Snapshot(f"{dependency['namespace']}/{dependency['version']}/{dependency['short_name']}")
        elif (step_type == "export") and (dependency["channel"] in "multidim"):
            collection_path = (
                paths.EXPORT_MDIMS_DIR / f"{dependency['namespace']}/{dependency['version']}/{dependency['short_name']}"
            )
            return CollectionSet(collection_path)
        else:
            dataset_path = (
                paths.DATA_DIR
                / f"{dependency['channel']}/{dependency['namespace']}/{dependency['version']}/{dependency['short_name']}"
            )
            dataset = catalog.Dataset(dataset_path)

        return dataset  # type: ignore[reportReturnType]

    def load_snapshot(self, short_name: str | None = None, **kwargs) -> Snapshot:
        """Load snapshot dependency. short_name defaults to the current step's short_name."""
        snap = self.load_dependency(channel="snapshot", short_name=short_name or self.short_name, **kwargs)
        assert isinstance(snap, Snapshot)
        return snap

    def read_snap_table(self, short_name: str | None = None, **kwargs) -> Table:
        """Load snapshot dependency. short_name defaults to the current step's short_name."""
        snap = self.load_snapshot(short_name=short_name)
        tb = snap.read(**kwargs)
        return tb

    def load_dataset(
        self,
        short_name: str | None = None,
        channel: CHANNEL | None = None,
        namespace: str | None = None,
        version: str | int | None = None,
    ) -> catalog.Dataset:
        """Load dataset dependency. short_name defaults to the current step's short_name."""
        dataset = self.load_dependency(
            short_name=short_name or self.short_name, channel=channel, namespace=namespace, version=version
        )
        assert isinstance(dataset, catalog.Dataset)
        return dataset

    def load_collectionset(
        self,
        short_name: str | None = None,
        namespace: str | None = None,
        version: str | int | None = None,
    ) -> CollectionSet:
        cs = self.load_dependency(
            step_type="export",
            short_name=short_name or self.short_name,
            channel="multidim",
            namespace=namespace,
            version=version,
        )
        assert isinstance(cs, CollectionSet)
        return cs

    def init_snapshot(self, filename: Optional[str] = None) -> Snapshot:
        """Create a snapshot using the current step's location to determine namespace, version, and optionally filename.

        Args:
            filename: Optional filename for the snapshot. If not provided, will look for a .dvc file
                     that matches the step name (e.g. unwto_gdp.py -> unwto_gdp.*.dvc).

        Returns:
            Snapshot object ready for use.

        Usage:
            # Automatically detect filename from matching .dvc file
            snap = paths.init_snapshot()

            # Or specify filename explicitly
            snap = paths.init_snapshot("unwto_gdp.xlsx")
        """
        if filename is None:
            # Look for .dvc file that matches the step name
            dvc_pattern = f"{self.short_name}.*.dvc"
            dvc_files = list(self.snapshot_dir.glob(dvc_pattern))

            if len(dvc_files) == 0:
                raise ValueError(f"No .dvc file found matching pattern '{dvc_pattern}' in {self.snapshot_dir}")
            elif len(dvc_files) > 1:
                raise ValueError(
                    f"Multiple .dvc files found matching pattern '{dvc_pattern}' in {self.snapshot_dir}: {[f.name for f in dvc_files]}. Please specify filename explicitly."
                )

            # Use the basename of the .dvc file (without .dvc extension)
            filename = dvc_files[0].name[:-4]  # Remove .dvc extension

        return Snapshot(f"{self.namespace}/{self.version}/{filename}")

    def load_etag_url(self) -> str:
        """Load etag url dependency and return its URL."""
        deps = [dep for dep in self.dependencies if dep.startswith("etag://")]
        assert len(deps) == 1
        return deps[0].replace("etag://", "https://")

    def load_config(self, filename: str | None = None, path: str | Path | None = None) -> dict[str, Any]:
        if filename is not None:
            path = self.directory / Path(filename)
        elif path is None:
            path = self.config_path
        try:
            config = catalog.utils.dynamic_yaml_to_dict(catalog.utils.dynamic_yaml_load(path))
        except AttributeError as e:
            raise AttributeError(f"There was a problem loading config from {path}, please review!. Original error: {e}")
        return config

    def load_collection_config(self, filename: str | None = None, path: str | Path | None = None) -> dict[str, Any]:
        """Replace code to use `self.load_config`."""
        return self.load_config(filename, path)

    def create_dataset(
        self,
        tables: Iterable[catalog.Table],
        default_metadata: SnapshotMeta | catalog.DatasetMeta | None = None,
        underscore_table: bool = True,
        camel_to_snake: bool = False,
        long_to_wide: bool | None = None,
        formats: list[FileFormat] = DEFAULT_FORMATS,
        check_variables_metadata: bool = True,
        run_grapher_checks: bool = True,
        yaml_params: dict[str, Any] | None = None,
        if_origins_exist: SOURCE_EXISTS_OPTIONS = "replace",
        errors: Literal["ignore", "warn", "raise"] = "raise",
        repack: bool = True,
    ) -> catalog.Dataset:
        return create_dataset(
            dest_dir=self.dest_dir,
            tables=tables,
            default_metadata=default_metadata,
            underscore_table=underscore_table,
            camel_to_snake=camel_to_snake,
            long_to_wide=long_to_wide,
            formats=formats,
            check_variables_metadata=check_variables_metadata,
            run_grapher_checks=run_grapher_checks,
            yaml_params=yaml_params,
            if_origins_exist=if_origins_exist,
            errors=errors,
            repack=repack,
        )

    @overload
    def create_collection(
        self,
        config: dict[str, Any],
        short_name: str | None = None,
        tb: list[Table] | Table | None = None,
        indicator_names: Listable[list[str] | None] | str = None,
        dimensions: Listable[list[str] | dict[str, list[str] | str] | None] = None,
        common_view_config: Listable[dict[str, Any] | None] = None,
        indicators_slug: str | None = None,
        indicator_as_dimension: bool = False,
        choice_renames: Listable[dict[str, dict[str, str] | Callable] | None] = None,
        catalog_path_full: bool = False,
        *,  # Force keyword-only arguments after this
        explorer: Literal[True],
    ) -> Explorer: ...

    @overload
    def create_collection(
        self,
        config: dict[str, Any],
        short_name: str | None = None,
        tb: list[Table] | Table | None = None,
        indicator_names: Listable[list[str] | None] | str = None,
        dimensions: Listable[list[str] | dict[str, list[str] | str] | None] = None,
        common_view_config: Listable[dict[str, Any] | None] = None,
        indicators_slug: str | None = None,
        indicator_as_dimension: bool = False,
        choice_renames: Listable[dict[str, dict[str, str] | Callable] | None] = None,
        catalog_path_full: bool = False,
        *,  # Force keyword-only arguments after this
        explorer: Literal[False] = False,
    ) -> Collection: ...

    def create_collection(
        self,
        config: dict[str, Any],
        short_name: str | None = None,
        tb: list[Table] | Table | None = None,
        indicator_names: Listable[list[str] | None] | str = None,
        dimensions: Listable[list[str] | dict[str, list[str] | str] | None] = None,
        common_view_config: Listable[dict[str, Any] | None] = None,
        indicators_slug: str | None = None,
        indicator_as_dimension: bool = False,
        choice_renames: Listable[dict[str, dict[str, str] | Callable] | None] = None,
        catalog_path_full: bool = False,
        explorer: bool = False,
    ) -> Explorer | Collection:
        """Create a collection with the given configuration and data.

        This function creates a Collection based on the provided configuration and table data. It supports both single and multiple table inputs with flexible indicator and dimension specification.

        You can create a collection purely based on manually crafted configuration (via YAML) by just using the `config` parameter, or you can provide a table (`tb`) with data to be expanded for the given indicators and dimensions. You can also combine both approaches, where the configuration from `config` will overwrite that automatically generated from the table data.

        A typical strategy is to define the high-level collection configuration and dimension specifications (e.g. dimension names, description, etc.) in the YAML file, and then use the `tb` parameter to automatically expand dimensional indicators.

        Note: You can also expand multiple tables by passing a list of `Table` objects to the `tb` parameter.

        Parameters
        ----------
        config : dict[str, Any]
            Configuration YAML dictionary for the explorer/collection. Typically loaded from a YAML file with `paths.load_collection_config()`.

        short_name : str | None, default None
            Short name of the Collection. Defaults to the short name of the step.

        tb : list[Table] | Table | None, default None
            Table object(s) with dimensional data. It can be a single `Table` or list of `Table`. The function will programmatically generate the collection configuration based on the available indicators in `tb`. To customize which indicators and dimensions to expand, refer to the `indicator_names` and `dimensions` parameters.

        indicator_names : Listable[list[str] | None] | str, default None
            Specifies which indicators from the table(s) to expand for the collection. Multiple formats supported:

                * `None`: All indicators from `tb` are used (also applies when `tb` is list).
                * `str`: Only the indicator with given name is used (also applies when `tb` is list).
                * `list[str]`: Only indicators with the given names are used (also applies when `tb` is list).
                * `list[list[str] | None]`: List where each element corresponds to a different table in `tb` (element _i_ in list corresponds to table _i_). If an element in the list is None, all indicators are used for that table. List length must match `tb` length.

        dimensions : Listable[list[str] | dict[str, list[str] | str] | None], default None
            Specifies which dimensions to use and, optionally, which choices. Multiple formats supported:
                * `None`: All dimensions are used (also applies when `tb` is list). Applies to all tables if `tb` is a list.
                * `list[str]`: Only dimensions with given names (all must be present). Applies to all tables if `tb` is a list.
                * `dict[str, list[str] | str]`: Keys are dimension names, values are choices to use. Use "*" as value to include all choices for a given dimension. Must contain all available dimensions as keys. Applies to all tables if `tb` is a list.
                * `list[list[str] | dict[str, list[str] | str]]`: List where each element corresponds to a different table in `tb` (element _i_ in list corresponds to table _i_). If an element in the list is None, all dimensions are used for that table. List length must match `tb` length.

        common_view_config : Listable[dict[str, Any] | None], default None
            Common view configuration applied to all views. Applies to all tables if `tb` is a list.

            If given as a list, each element corresponds to a different table in `tb` (element _i_ in list corresponds to table _i_). If an element in the list is None, no common view configuration is applied for that table. List length must match `tb` length.

        indicators_slug : str | None, default None
            Custom slug for indicators. Uses a default name if not provided.

        indicator_as_dimension : bool, default False
            If True, the indicator name is treated as a dimension. This means that the indicator will be included in the dimensions of the collection, allowing it to be used as a filter or in views.

        choice_renames : Listable[dict[str, dict[str, str] | Callable] | None], default None
            Use this to rename the names of the dimension choices. Multiple formats supported:
                * `None`: No renames are applied (also applies when `tb` is list).
                * `dict[str, dict[str, str]]`: Key is the dimension slug, value is a mapping
                    with original choice slug as key and the new choice name as value.
                * `dict[str, Callable]`: Key is the dimension slug, value is a function that
                    returns the new name for given slug (returns None to keep original)
                * For multiple tables: Can be list matching `tb` length with any of the above formats.

        catalog_path_full : bool, default False
            If True, it uses full catalog path. If False, uses shorter version (e.g., `table#indicator` or `dataset/table#indicator`).

        explorer : bool, default False
            Use this flag to create an explorer (True).

        Returns
        -------
        Explorer | Collection
            Returns an Explorer or MDIM Collection based on the provided configuration and data.

        Notes
        -----
        When `tb` is a list of tables, the parameters `indicator_names`, `dimensions`, `common_view_config`, and `choice_renames` can also be lists where each element corresponds to a table in `tb`. List lengths must match `tb` length. If these parameters are not lists, the same value is applied to all tables.
        """
        return create_collection(
            config_yaml=config,
            dependencies=self.dependencies,
            catalog_path=f"{self.namespace}/{self.version}/{self.short_name}#{short_name or self.short_name}",
            tb=tb,
            indicator_names=indicator_names,
            dimensions=dimensions,
            common_view_config=common_view_config,
            indicators_slug=indicators_slug,
            indicator_as_dimension=indicator_as_dimension,
            choice_renames=choice_renames,
            catalog_path_full=catalog_path_full,
            explorer=explorer,
        )

    @deprecated.deprecated(
        reason="We should slowly migrate to YAML-based explorers, and use `paths.create_collection` instead."
    )
    def create_explorer_legacy(
        self,
        config: dict[str, Any],
        df_graphers: pd.DataFrame,
        df_columns: pd.DataFrame | None = None,
        reset: bool = False,
    ) -> ExplorerLegacy:
        """NOTE: We should slowly migrate to YAML-based explorers, and use `paths.create_collection` instead.

        This function is used to create an Explorer object using the legacy configuration.

        To use the new tools, first migrate the explorer to use the new MDIM-based configuration.

        Param `reset` is False by default, because many explorers have manually set map brackets or fields like
        pickerColumnSlugs. Ideally, everything should be set in ETL.
        """
        log.warning(
            "This function is operative, but relies on legacy configuration. To use latest tools, consider migrating your explorer to use MDIM-based configuration."
        )
        # If the name of the explorer is specified in config, take that, otherwise use the step's short_name.
        # NOTE: This is the expected name of the explorer tsv file.
        if "name" in config:
            explorer_name = config["name"]
        else:
            explorer_name = self.short_name
        assert isinstance(explorer_name, str)

        explorer_catalog_path = f"{self.namespace}/{self.version}/{self.short_name}#{explorer_name}"

        explorer = create_explorer_legacy(
            catalog_path=explorer_catalog_path,
            config=config,
            df_graphers=df_graphers,
            explorer_name=explorer_name,
            df_columns=df_columns,
            reset=reset,
        )

        return explorer


def _match_dependencies(pattern: str, dependencies: set[str]) -> set[str]:
    regex = re.compile(pattern)
    return {dependency for dependency in dependencies if regex.match(dependency)}
