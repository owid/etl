#
#  helpers.py
#  etl
#
import datetime as dt
import re
import time
from functools import cache
from pathlib import Path
from typing import Any, Dict, Iterable, List, Literal, Optional, Union

import pandas as pd
import structlog
from jsonschema import validate
from owid import catalog
from owid.catalog import CHANNEL, DatasetMeta, Table, warnings
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
from owid.walden import Catalog as WaldenCatalog
from owid.walden import Dataset as WaldenDataset

from etl import paths
from etl.config import DEFAULT_GRAPHER_SCHEMA
from etl.dag_helpers import load_dag
from etl.explorer import Explorer
from etl.files import get_schema_from_url
from etl.snapshot import Snapshot, SnapshotMeta

log = structlog.get_logger()


# TODO: Move to etl.grapher.helpers
def grapher_checks(ds: catalog.Dataset, warn_title_public: bool = True) -> None:
    """Check that the table is in the correct format for Grapher."""
    from etl.grapher import helpers as gh

    assert ds.metadata.title, "Dataset must have a title."

    for tab in ds:
        if {"year", "country"} <= set(tab.all_columns):
            if "year" in tab.columns:
                year = tab["year"]
            else:
                year = tab.index.get_level_values("year")
            assert year.dtype in gh.INT_TYPES, f"year must be of an integer type but was: {year.dtype}"
        elif {"date", "country"} <= set(tab.all_columns):
            pass
        else:
            raise AssertionError("Table must have columns country and year or date.")

        for col in tab:
            if col in ("year", "country"):
                continue
            catalog.utils.validate_underscore(col)
            assert tab[col].metadata.unit is not None, f"Column `{col}` must have a unit."
            assert tab[col].metadata.title is not None, f"Column `{col}` must have a title."
            assert (
                tab[col].m.origins or tab[col].m.sources or ds.metadata.sources
            ), f"Column `{col}` must have either sources or origins"

            _validate_description_key(tab[col].m.description_key, col)
            _validate_ordinal_variables(tab, col)
            _validate_grapher_config(tab, col)

            # Data Page title uses the following fallback
            # [title_public > grapher_config.title > display.name > title] - [attribution_short] - [title_variant]
            # the Table tab
            # [title_public > display.name > title] - [title_variant] - [attribution_short]
            # and chart heading
            # [grapher_config.title > title_public > display.name > title] - [grapher_config.subtitle > description_short]
            #
            # Warn if display.name (which is used for legend) exists and there's no title_public set. This
            # would override the indicator title in the Data Page.
            display_name = (tab[col].m.display or {}).get("name")
            title_public = getattr(tab[col].m.presentation, "title_public", None)
            if warn_title_public and display_name and not title_public:
                warnings.warn(
                    f"Column {col} uses display.name but no presentation.title_public. Ensure the latter is also defined, otherwise display.name will be used as the indicator's title.",
                    warnings.DisplayNameWarning,
                )


def _validate_grapher_config(tab: Table, col: str) -> None:
    """Validate grapher config against given schema or against the default schema."""
    grapher_config = getattr(tab[col].m.presentation, "grapher_config", None)
    if grapher_config:
        grapher_config.setdefault("$schema", DEFAULT_GRAPHER_SCHEMA)

        # Load schema and remove properties that are not relevant for the validation
        schema = get_schema_from_url(grapher_config["$schema"])
        # schema["required"] = [f for f in schema["required"] if f not in ("dimensions", "version", "title")]
        schema["required"] = []

        validate(grapher_config, schema)


def _validate_description_key(description_key: list[str], col: str) -> None:
    if description_key:
        assert not all(
            len(x) == 1 for x in description_key
        ), f"Column `{col}` uses string {description_key} as description_key, should be list of strings."


def _validate_ordinal_variables(tab: Table, col: str) -> None:
    if tab[col].m.sort:
        # Exclude NaN values, these will be dropped before inserting to the database.
        vals = tab[col].dropna()

        extra_values = set(vals) - set(vals.m.sort)
        assert (
            not extra_values
        ), f"Ordinal variable `{col}` has extra values that are not defined in field `sort`: {extra_values}"


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


def create_dataset_with_combined_metadata(
    dest_dir: Union[str, Path],
    datasets: List[catalog.Dataset],
    tables: List[catalog.Table],
    default_metadata: Optional[Union[SnapshotMeta, catalog.DatasetMeta]] = None,  # type: ignore
    underscore_table: bool = True,
    formats: List[FileFormat] = DEFAULT_FORMATS,
) -> catalog.Dataset:
    """Create a new catalog Dataset with the combination of sources and licenses of a list of datasets.

    This function will:
    * Gather all sources and licenses of a list of datasets (`datasets`).
    * Assign the combined sources and licenses to all variables in a list of tables (`tables`).
    * Create a new dataset (using the function `create_dataset`) with the combined sources and licenses.

    NOTES:
      * The sources and licenses of the default_metadata will be ignored (and the combined sources and licenses of all
        `datasets` will be used instead).
      * If a metadata yaml file exists and contains sources and licenses, the content of the metadata file will
        override the combined sources and licenses.

    Parameters
    ----------
    dest_dir : Union[str, Path]
        Destination directory for the dataset, usually argument of `run` function.
    datasets : List[catalog.Dataset]
        Datasets whose sources and licenses will be gathered and passed on to the new dataset.
    tables : List[catalog.Table]
        Tables to add to the new dataset.
    default_metadata : Optional[Union[SnapshotMeta, catalog.DatasetMeta]]
        Default metadata for the new dataset. If it contains sources and licenses, they will be ignored (and the
        combined sources of the list of datasets passed will be used).
    underscore_table : bool
        Whether to underscore the table name before adding it to the dataset.

    Returns
    -------
    catalog.Dataset
        New dataset with combined metadata.

    """
    from etl.steps.data.converters import convert_snapshot_metadata

    # Gather unique sources from the original datasets.
    sources = []
    licenses = []
    for dataset_i in datasets:
        # Get metadata from this dataset or snapshot.
        if isinstance(dataset_i.metadata, SnapshotMeta):
            metadata = convert_snapshot_metadata(dataset_i.metadata)
        else:
            metadata = dataset_i.metadata

        # Gather sources and licenses from this dataset or snapshot.
        for source in metadata.sources:
            if source.name not in [known_source.name for known_source in sources]:
                sources.append(source)
        for license in metadata.licenses:
            if license.name not in [known_license.name for known_license in licenses]:
                licenses.append(license)

    # Assign combined sources and licenses to each of the variables in each of the tables.
    for table in tables:
        index_columns = table.metadata.primary_key
        # If the table has an index, reset it, so that sources and licenses can also be assigned to index columns.
        if len(index_columns) > 0:
            table = table.reset_index()
        # Assign sources and licenses to the metadata of each variable in the table.
        for variable in table.columns:
            table[variable].metadata.sources = sources
            table[variable].metadata.licenses = licenses
        # Bring original index back.
        if len(index_columns) > 0:
            table = table.set_index(index_columns)

    if default_metadata is None:
        # If no default metadata is passed, create new empty dataset metadata.
        default_metadata = catalog.DatasetMeta()
    elif isinstance(default_metadata, SnapshotMeta):
        # If a snapshot metadata is passed as default metadata, convert it to a dataset metadata.
        default_metadata: catalog.DatasetMeta = convert_snapshot_metadata(default_metadata)

    # Assign combined sources and licenses to the new dataset metadata.
    default_metadata.sources = sources
    default_metadata.licenses = licenses

    # Create a new dataset.
    ds = create_dataset(
        dest_dir=dest_dir,
        tables=tables,
        default_metadata=default_metadata,
        underscore_table=underscore_table,
        formats=formats,
    )

    return ds


class CurrentFileMustBeAStep(ExceptionFromDocstring):
    """Current file must be an ETL step."""


class CurrentStepMustBeInDag(ExceptionFromDocstring):
    """Current step must be listed in the dag."""


class NoMatchingStepsAmongDependencies(ExceptionFromDocstringWithKwargs):
    """No steps found among dependencies of current ETL step, that match the given specifications."""


class MultipleMatchingStepsAmongDependencies(ExceptionFromDocstringWithKwargs):
    """Multiple steps found among dependencies of current ETL step, that match the given specifications."""


class UnknownChannel(ExceptionFromDocstring):
    """Unknown channel name. Valid channels are 'examples', 'walden', 'snapshot', 'meadow', 'garden', or 'grapher'."""


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
    def walden_dataset(self) -> WaldenDataset:
        return WaldenCatalog().find_one(namespace=self.namespace, version=self.version, short_name=self.short_name)

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
        elif channel == "walden":
            step_name = f"{channel}{is_private_suffix}://{namespace}/{version}/{short_name}"
        elif channel is None:
            step_name = rf"(?:snapshot{is_private_suffix}:/|walden{is_private_suffix}:/|data{is_private_suffix}://meadow|data{is_private_suffix}://garden|data{is_private_suffix}://grapher|data://explorers)/{namespace}/{version}/{short_name}$"
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
        if channel_type.startswith(("walden", "snapshot")):
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
    ) -> Union[catalog.Dataset, Snapshot, WaldenCatalog]:
        """Load a dataset dependency, given its attributes (at least its short name)."""
        dependency_step_name = self.get_dependency_step_name(
            step_type=step_type,
            short_name=short_name,
            channel=channel,
            namespace=namespace,
            version=version,
            is_private=is_private,
        )
        dependency = self._get_attributes_from_step_name(step_name=dependency_step_name)
        if dependency["channel"] == "walden":
            dataset = WaldenCatalog().find_one(
                namespace=dependency["namespace"], version=dependency["version"], short_name=dependency["short_name"]
            )
        elif dependency["channel"] == "snapshot":
            dataset = Snapshot(f"{dependency['namespace']}/{dependency['version']}/{dependency['short_name']}")
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
        config = catalog.utils.dynamic_yaml_to_dict(catalog.utils.dynamic_yaml_load(path))
        return config

    def load_mdim_config(self, filename: Optional[str] = None, path: Optional[str | Path] = None) -> Dict[str, Any]:
        """Replace code to use `self.load_config`."""
        return self.load_config(filename, path)

    def load_explorer_config(self, filename: Optional[str] = None, path: Optional[str | Path] = None) -> Dict[str, Any]:
        return self.load_config(filename, path)


def _match_dependencies(pattern: str, dependencies: set[str]) -> set[str]:
    regex = re.compile(pattern)
    return {dependency for dependency in dependencies if regex.match(dependency)}


def create_explorer(
    dest_dir: Union[str, Path],
    config: Dict[str, Any],
    df_graphers: pd.DataFrame,
    df_columns: Optional[pd.DataFrame] = None,
) -> Explorer:
    # Extract information about this step from dest_dir.
    channel, namespace, version, short_name = str(dest_dir).split("/")[-4:]

    # If the name of the explorer is specified in config, take that, otherwise use the step's short_name.
    # NOTE: This is the expected name of the explorer tsv file.
    if "name" in config:
        explorer_name = config["name"]
    else:
        explorer_name = short_name

    # Initialize explorer.
    explorer = Explorer.from_owid_content(explorer_name)
    # Add a comment to avoid manual edits.
    explorer.comments = [
        f"# DO NOT EDIT THIS FILE MANUALLY. IT WAS GENERATED BY ETL step '{channel}/{namespace}/{version}/{short_name}'."
    ]
    # Update its config.
    explorer.config.update(config)
    # Update its graphers and columns tables.
    explorer.df_graphers = df_graphers
    if df_columns is not None:
        explorer.df_columns = df_columns

    return explorer


def last_date_accessed(tb: Table) -> str:
    """Get maximum date_accessed from all origins in the table and display it in a specific format.

    Usage:
        create_dataset(..., yaml_params={"date_accessed": last_date_accessed(tb)})
    """
    date_accessed = max([origin.date_accessed for col in tb.columns for origin in tb[col].m.origins])
    return dt.datetime.strptime(date_accessed, "%Y-%m-%d").strftime("%d %B %Y")


def last_date_published(tb: Table) -> str:
    """Get maximum date_published from all origins in the table and display it in a specific format.

    Usage:
        create_dataset(..., yaml_params={"date_published": last_date_published(tb)})
    """
    date_published = max([origin.date_published for col in tb.columns for origin in tb[col].m.origins])
    return dt.datetime.strptime(date_published, "%Y-%m-%d").strftime("%d %B %Y")
