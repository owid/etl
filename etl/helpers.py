#
#  helpers.py
#  etl
#
import re
import time
from functools import cache
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Literal, Optional, Union

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
from etl.collection.explorer import Explorer, ExplorerLegacy, create_explorer_legacy
from etl.collection.model import Collection
from etl.collection.multidim import CollectionSet, create_collection, create_collection_from_config
from etl.dag_helpers import load_dag
from etl.grapher.helpers import grapher_checks
from etl.snapshot import Snapshot, SnapshotMeta

log = structlog.get_logger()


__all__ = ["grapher_checks"]


def _set_metadata_from_dest_dir(ds: catalog.Dataset, dest_dir: Union[str, Path]) -> catalog.Dataset:
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
    dest_dir: Union[str, Path],
    tables: Iterable[catalog.Table],
    default_metadata: Optional[Union[SnapshotMeta, catalog.DatasetMeta]] = None,
    underscore_table: bool = True,
    camel_to_snake: bool = False,
    long_to_wide: Optional[bool] = None,
    formats: List[FileFormat] = DEFAULT_FORMATS,
    check_variables_metadata: bool = True,
    run_grapher_checks: bool = True,
    yaml_params: Optional[Dict[str, Any]] = None,
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

    def __init__(self, __file__: str, is_private: Optional[bool] = None):
        self.f = Path(__file__)

        # Lazy load dag when needed.
        self._dag = None

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
        return self.f.parent.parent.parent.parent.name

    @property
    def channel(self) -> CHANNEL:
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
    def mdim_path(self) -> Path:
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
        channel: Optional[CHANNEL] = None,
        namespace: Optional[str] = None,
        version: Optional[Union[int, str]] = None,
        is_private: Optional[bool] = False,
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
    def _get_attributes_from_step_name(step_name: str) -> Dict[str, str]:
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
        channel: Optional[CHANNEL] = None,
        namespace: Optional[str] = None,
        version: Optional[Union[str, int]] = None,
        is_private: Optional[bool] = None,
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
        channel: Optional[CHANNEL] = None,
        namespace: Optional[str] = None,
        version: Optional[Union[str, int]] = None,
        is_private: Optional[bool] = None,
    ) -> Union[catalog.Dataset, Snapshot, CollectionSet]:
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
            mdims_path = (
                paths.EXPORT_MDIMS_DIR / f"{dependency['namespace']}/{dependency['version']}/{dependency['short_name']}"
            )
            return CollectionSet(mdims_path)
        else:
            dataset_path = (
                paths.DATA_DIR
                / f"{dependency['channel']}/{dependency['namespace']}/{dependency['version']}/{dependency['short_name']}"
            )
            dataset = catalog.Dataset(dataset_path)

        return dataset  # type: ignore[reportReturnType]

    def load_snapshot(self, short_name: Optional[str] = None, **kwargs) -> Snapshot:
        """Load snapshot dependency. short_name defaults to the current step's short_name."""
        snap = self.load_dependency(channel="snapshot", short_name=short_name or self.short_name, **kwargs)
        assert isinstance(snap, Snapshot)
        return snap

    def read_snap_table(self, short_name: Optional[str] = None, **kwargs) -> Table:
        """Load snapshot dependency. short_name defaults to the current step's short_name."""
        snap = self.load_snapshot(short_name=short_name)
        tb = snap.read(**kwargs)
        return tb

    def load_dataset(
        self,
        short_name: Optional[str] = None,
        channel: Optional[CHANNEL] = None,
        namespace: Optional[str] = None,
        version: Optional[Union[str, int]] = None,
    ) -> catalog.Dataset:
        """Load dataset dependency. short_name defaults to the current step's short_name."""
        dataset = self.load_dependency(
            short_name=short_name or self.short_name, channel=channel, namespace=namespace, version=version
        )
        assert isinstance(dataset, catalog.Dataset)
        return dataset

    def load_collectionset(
        self,
        short_name: Optional[str] = None,
        namespace: Optional[str] = None,
        version: Optional[Union[str, int]] = None,
    ) -> CollectionSet:
        mdims = self.load_dependency(
            step_type="export",
            short_name=short_name or self.short_name,
            channel="multidim",
            namespace=namespace,
            version=version,
        )
        assert isinstance(mdims, CollectionSet)
        return mdims

    def load_etag_url(self) -> str:
        """Load etag url dependency and return its URL."""
        deps = [dep for dep in self.dependencies if dep.startswith("etag://")]
        assert len(deps) == 1
        return deps[0].replace("etag://", "https://")

    def load_config(self, filename: Optional[str] = None, path: Optional[str | Path] = None) -> Dict[str, Any]:
        if filename is not None:
            path = self.directory / Path(filename)
        elif path is None:
            path = self.config_path
        try:
            config = catalog.utils.dynamic_yaml_to_dict(catalog.utils.dynamic_yaml_load(path))
        except AttributeError as e:
            raise AttributeError(f"There was a problem loading config from {path}, please review!. Original error: {e}")
        return config

    def load_mdim_config(self, filename: Optional[str] = None, path: Optional[str | Path] = None) -> Dict[str, Any]:
        """Replace code to use `self.load_config`."""
        return self.load_config(filename, path)

    def load_explorer_config(self, filename: Optional[str] = None, path: Optional[str | Path] = None) -> Dict[str, Any]:
        return self.load_config(filename, path)
        # Check that it can be loaded as an Explorer object.
        # explorer = Explorer.from_dict(config)
        # return explorer.to_dict(drop_definitions=False)

    def create_dataset(
        self,
        tables: Iterable[catalog.Table],
        default_metadata: Optional[Union[SnapshotMeta, catalog.DatasetMeta]] = None,
        underscore_table: bool = True,
        camel_to_snake: bool = False,
        long_to_wide: Optional[bool] = None,
        formats: List[FileFormat] = DEFAULT_FORMATS,
        check_variables_metadata: bool = True,
        run_grapher_checks: bool = True,
        yaml_params: Optional[Dict[str, Any]] = None,
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

    def create_explorer(
        self,
        config,
        explorer_name: Optional[str] = None,
    ) -> Explorer:
        """Create an Explorer object.

        Args:
        -----
        config: Dict[str, Any]
            Configuration of the explorer.
        explorer_name: str
            Name of the explorer. If none is provided, it will use the short_name from the explorer catalog path.
        """
        # Create Explorer object
        explorer = create_collection_from_config(
            config=config,
            dependencies=self.dependencies,
            catalog_path=f"{self.namespace}/{self.version}/{self.short_name}#{explorer_name or self.short_name}",
            validate_schema=False,
            explorer=True,
        )

        assert isinstance(explorer, Explorer), "Unexpected type of explorer object. Expected Explorer."

        return explorer

    def create_collection_legacy(
        self,
        config,
        short_name: Optional[str] = None,
        explorer: bool = False,
    ):
        if explorer:
            # Create Collection object
            c = create_collection_from_config(
                config,
                self.dependencies,
                catalog_path=f"{self.namespace}/{self.version}/{self.short_name}#{short_name or self.short_name}",
            )
        else:
            c = create_collection_from_config(
                config=config,
                dependencies=self.dependencies,
                catalog_path=f"{self.namespace}/{self.version}/{self.short_name}#{short_name or self.short_name}",
                validate_schema=False,
                explorer=True,
            )

            assert isinstance(c, Explorer), "Unexpected type of explorer object. Expected Explorer."

        return c

    def create_collection(
        self,
        config: Dict[str, Any],
        short_name: Optional[str] = None,
        tb: Optional[Table] = None,
        indicator_names: Optional[Union[str, List[str]]] = None,
        dimensions: Optional[Union[List[str], Dict[str, Union[List[str], str]]]] = None,
        common_view_config: Optional[Dict[str, Any]] = None,
        indicators_slug: Optional[str] = None,
        indicator_as_dimension: bool = False,
        choice_renames: Optional[Dict[str, Union[Dict[str, str], Callable]]] = None,
        catalog_path_full: bool = False,
        explorer: bool = False,
    ) -> Collection:
        """Experimental smarter explorer creation.

        Args:
        -----
        config: dict
            Configuration YAML for the explorer.
        short_name: str
            Name of the MDIM page. Default is short_name from mdim catalog path.
        catalog_path: str
            Catalog path for the MDIM. This is used to create the MDIM in the database.
        tb: Table
            Table object with data. This data will be expanded for the given indicators and dimensions.
        indicator_names: Optional[Union[str, List[str]]]
            Name of the indicators to be used. If None, all indicators are used.
        dimensions: Optional[Union[List[str], Dict[str, Union[List[str], str]]]]
            Dimensions to be used. If None, all dimensions are used. If a list, all dimensions are used with the given names. If a dict, key represent dimensions to use and values choices to use. Note that if a list or dictionary is given, all dimensions must be present.
        common_view_config: Optional[Dict[str, Any]]
            Common view configuration to be used for all views.
        indicators_slug: Optional[str]
            Slug to be used for the indicators. A default is used.
        indicator_as_dimension: bool
            If True, the indicator is treated as a dimension.
        choice_renames: Optional[Dict[str, Union[Dict[str, str], Callable]]]
            Renames for choices. If a dictionary, the key is the dimension slug and the value is a dictionary with the original slug as key and the new name as value. If a callable, the function should return the new name for the given slug. NOTE: If the callable returns None, the name is not changed.
        catalog_path_full: bool
            If True, the full path is used for the catalog. If False, a shorter version is used (e.g. table#indicator` or `dataset/table#indicator`).
        explorer: bool
            If True, the collection is created as an explorer. If False, the collection is created as a MDIM.
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

    def create_explorer_legacy(
        self,
        config: Dict[str, Any],
        df_graphers: pd.DataFrame,
        df_columns: Optional[pd.DataFrame] = None,
        reset: bool = False,
    ) -> ExplorerLegacy:
        """This function is used to create an Explorer object using the legacy configuration.

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
