#
#  helpers.py
#  etl
#

import re
import sys
import tempfile
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Set, Union, cast
from urllib.parse import urljoin

import jsonref
import pandas as pd
import structlog
import yaml
from owid import catalog
from owid.catalog import CHANNEL, DatasetMeta, Table
from owid.catalog.datasets import DEFAULT_FORMATS, FileFormat
from owid.catalog.meta import SOURCE_EXISTS_OPTIONS
from owid.catalog.tables import (
    combine_tables_description,
    combine_tables_title,
    get_unique_licenses_from_tables,
    get_unique_sources_from_tables,
)
from owid.datautils.common import ExceptionFromDocstring
from owid.walden import Catalog as WaldenCatalog
from owid.walden import Dataset as WaldenDataset

from etl import paths
from etl.snapshot import Snapshot, SnapshotMeta
from etl.steps import extract_step_attributes, load_dag, reverse_graph

log = structlog.get_logger()


@contextmanager
def downloaded(url: str) -> Iterator[str]:
    """
    Download the url to a temporary file and yield the filename.
    """
    import requests

    with tempfile.NamedTemporaryFile() as tmp:
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            chunk_size = 2**16  # 64k
            for chunk in r.iter_content(chunk_size=chunk_size):
                tmp.write(chunk)

        yield tmp.name


def get_latest_github_sha(org: str, repo: str, branch: str) -> str:
    # Use Github's list-branches API to get the sha1 of the most recent commit
    # https://docs.github.com/en/rest/reference/repos#list-branches
    branches = _get_github_branches(org, repo)
    (match,) = [b for b in branches if b["name"] == branch]
    return cast(str, match["commit"]["sha"])


def _get_github_branches(org: str, repo: str) -> List[Any]:
    import requests

    url = f"https://api.github.com/repos/{org}/{repo}/branches?per_page=100"
    resp = requests.get(url, headers={"Accept": "application/vnd.github.v3+json"})
    if resp.status_code != 200:
        raise Exception(f"got {resp.status_code} from {url}")

    branches = cast(List[Any], resp.json())
    if len(branches) == 100:
        raise Exception("reached single page limit, should paginate request")

    return branches


def grapher_checks(ds: catalog.Dataset) -> None:
    """Check that the table is in the correct format for Grapher."""
    from etl import grapher_helpers as gh

    assert ds.metadata.title, "Dataset must have a title."

    for tab in ds:
        assert {"year", "country"} <= set(tab.reset_index().columns), "Table must have columns country and year."
        assert (
            tab.reset_index()["year"].dtype in gh.INT_TYPES
        ), f"year must be of an integer type but was: {tab['year'].dtype}"
        for col in tab:
            if col in ("year", "country"):
                continue
            catalog.utils.validate_underscore(col)
            assert tab[col].metadata.unit is not None, f"Column `{col}` must have a unit."
            assert tab[col].metadata.title is not None, f"Column `{col}` must have a title."
            assert (
                tab[col].m.origins or tab[col].m.sources or ds.metadata.sources
            ), f"Column `{col}` must have either sources or origins"

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
            if display_name and not title_public:
                log.warning(
                    f"Column {col} uses display.name but no presentation.title_public. Ensure the latter is also defined, otherwise display.name will be used as the indicator's title.",
                )


def create_dataset(
    dest_dir: Union[str, Path],
    tables: Iterable[catalog.Table],
    default_metadata: Optional[Union[SnapshotMeta, catalog.DatasetMeta]] = None,
    underscore_table: bool = True,
    camel_to_snake: bool = False,
    formats: List[FileFormat] = DEFAULT_FORMATS,
    check_variables_metadata: bool = False,
    run_grapher_checks: bool = True,
    if_origins_exist: SOURCE_EXISTS_OPTIONS = "replace",
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
    :param check_variables_metadata: Check that all variables in tables have metadata; raise a warning otherwise.
    :param run_grapher_checks: Run grapher checks on the dataset, only applies to grapher channel.
    :param if_origins_exist: What to do if origins already exist in the dataset metadata.

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
            default_metadata = DatasetMeta(licenses=licenses, title=title, description=description)
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

    # add tables to dataset
    used_short_names = set()
    for table in tables:
        if underscore_table:
            table = catalog.utils.underscore_table(table, camel_to_snake=camel_to_snake)
        if table.metadata.short_name in used_short_names:
            raise ValueError(f"Table short name `{table.metadata.short_name}` is already in use.")
        used_short_names.add(table.metadata.short_name)
        ds.add(table, formats=formats)

    # set metadata from dest_dir
    pattern = (
        r"\/"
        + r"\/".join(
            [
                f"(?P<channel>{'|'.join(CHANNEL.__args__)})",
                "(?P<namespace>.*?)",
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

    meta_path = get_metadata_path(str(dest_dir))
    if meta_path.exists():
        ds.update_metadata(meta_path, if_origins_exist=if_origins_exist)

        # check that we are not using metadata inconsistent with path
        for k, v in match.groupdict().items():
            assert str(getattr(ds.metadata, k)) == v, f"Metadata {k} is inconsistent with path {dest_dir}"

    # another override YAML file with higher priority
    meta_override_path = get_metadata_path(str(dest_dir)).with_suffix(".override.yml")
    if meta_override_path.exists():
        ds.update_metadata(meta_override_path, if_origins_exist=if_origins_exist)

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


class NoMatchingStepsAmongDependencies(ExceptionFromDocstring):
    """No steps found among dependencies of current ETL step, that match the given specifications."""


class MultipleMatchingStepsAmongDependencies(ExceptionFromDocstring):
    """Multiple steps found among dependencies of current ETL step, that match the given specifications."""


class UnknownChannel(ExceptionFromDocstring):
    """Unknown channel name. Valid channels are 'examples', 'walden', 'snapshot', 'meadow', 'garden', or 'grapher'."""


class WrongStepName(ExceptionFromDocstring):
    """Wrong step name. If this step was in the dag, it should be corrected."""


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

        # Load dag.
        if "/archive/" in __file__:
            self.dag = load_dag(paths.DAG_ARCHIVE_FILE)
        else:
            self.dag = load_dag()

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
    def channel(self) -> str:
        return self.f.parent.parent.parent.name

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

    @staticmethod
    def create_step_name(
        short_name: str,
        channel: Optional[CHANNEL] = None,
        namespace: Optional[str] = None,
        version: Optional[Union[int, str]] = None,
        is_private: Optional[bool] = False,
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

        if channel in ["meadow", "garden", "grapher", "explorers", "examples", "open_numbers"]:
            step_name = f"data{is_private_suffix}://{channel}/{namespace}/{version}/{short_name}"
        elif channel == "snapshot":
            # match also on snapshot short_names without extension
            step_name = f"{channel}{is_private_suffix}://{namespace}/{version}/{short_name}(.\\w+)?"
        elif channel == "walden":
            step_name = f"{channel}{is_private_suffix}://{namespace}/{version}/{short_name}"
        elif channel is None:
            step_name = rf"(?:snapshot{is_private_suffix}:/|walden{is_private_suffix}:/|data{is_private_suffix}://meadow|data{is_private_suffix}://garden|data://grapher|data://explorers|backport://backport)/{namespace}/{version}/{short_name}$"
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
        )

    @staticmethod
    def _get_attributes_from_step_name(step_name: str) -> Dict[str, str]:
        """Get attributes (channel, namespace, version, short name and is_private) from the step name (as it appears in the dag)."""
        channel_type, path = step_name.split("://")
        if channel_type.startswith(("walden", "snapshot")):
            channel = channel_type
            namespace, version, short_name = path.split("/")
        elif channel_type.startswith(("data", "backport")):
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
                raise CurrentStepMustBeInDag
            else:
                return _step

    @property
    def dependencies(self) -> List[str]:
        # Current step should be in the dag.
        if self.step not in self.dag:
            raise CurrentStepMustBeInDag

        return self.dag[self.step]

    def get_dependency_step_name(
        self,
        short_name: str,
        channel: Optional[str] = None,
        namespace: Optional[str] = None,
        version: Optional[Union[str, int]] = None,
        is_private: Optional[bool] = None,
    ) -> str:
        """Get dependency step name (as it appears in the dag) given its attributes (at least its short name)."""

        pattern = self.create_step_name(
            channel=channel, namespace=namespace, version=version, short_name=short_name, is_private=is_private
        )
        matches = [dependency for dependency in self.dependencies if bool(re.match(pattern, dependency))]

        # If no step was found and is_private was not specified, try again assuming step is private.
        if (len(matches) == 0) and (is_private is None):
            pattern = self.create_step_name(
                channel=channel, namespace=namespace, version=version, short_name=short_name, is_private=True
            )
            matches = [dependency for dependency in self.dependencies if bool(re.match(pattern, dependency))]

        if len(matches) == 0:
            raise NoMatchingStepsAmongDependencies
        elif len(matches) > 1:
            raise MultipleMatchingStepsAmongDependencies

        dependency = matches[0]

        return dependency

    def load_dependency(
        self,
        short_name: str,
        channel: Optional[str] = None,
        namespace: Optional[str] = None,
        version: Optional[Union[str, int]] = None,
        is_private: Optional[bool] = None,
    ) -> Union[catalog.Dataset, Snapshot, WaldenCatalog]:
        """Load a dataset dependency, given its attributes (at least its short name)."""
        dependency_step_name = self.get_dependency_step_name(
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

        return dataset

    def load_snapshot(self, short_name: Optional[str] = None) -> Snapshot:
        """Load snapshot dependency. short_name defaults to the current step's short_name."""
        snap = self.load_dependency(channel="snapshot", short_name=short_name or self.short_name)
        assert isinstance(snap, Snapshot)
        return snap

    def load_dataset(
        self,
        short_name: Optional[str] = None,
        channel: Optional[str] = None,
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


def list_all_steps_in_dag(dag: Dict[str, Any]) -> List[str]:
    """List all steps in a dag.

    Parameters
    ----------
    dag : Dict[str, Any]
        Dag.

    Returns
    -------
    all_steps : List[str]
        List of steps in dag.

    """
    all_steps = sorted(set([step for step in dag] + sum([list(dag[step]) for step in dag], [])))

    return all_steps


def get_direct_step_dependencies(dag: Dict[str, Any], step: str) -> List[str]:
    """Get direct dependencies of a given step in a dag.

    Direct dependencies of a step are those datasets that are listed in the dag as the step's dependencies.

    Parameters
    ----------
    dag : Dict[str, Any]
        Dag.
    step : str
        Step (as it appears in the dag).

    Returns
    -------
    dependencies : List[str]
        Direct dependencies of a step in a dag.

    """
    if step in dag:
        # If step is in the dag, return its dependencies.
        dependencies = sorted(dag[step])
    else:
        # If step is not in the dag, return an empty list.
        dependencies = []

    return dependencies


def get_direct_step_usages(dag: Dict[str, Any], step: str) -> List[str]:
    """Get direct usages of a given step in a dag.

    Direct usages of a step are those datasets that have the current step listed in the dag as one of the dependencies.

    Parameters
    ----------
    dag : Dict[str, Any]
        Dag.
    step : str
        Step (as it appears in the dag).

    Returns
    -------
    dependencies : List[str]
        Direct usages of a step in a dag.

    """

    used_by = sorted(set([_step for _step in dag if step in dag[_step]]))

    return used_by


def _recursive_get_all_step_dependencies(dag: Dict[str, Any], step: str, dependencies: Set[str] = set()) -> Set[str]:
    if step in dag:
        # If step is in the dag, gather all its substeps.
        substeps = dag[step]
        # Add substeps to the set of dependencies (union of sets, to avoid repetitions).
        dependencies = dependencies | set(substeps)
        for substep in substeps:
            # For each of the substeps, repeat the process.
            dependencies = dependencies | _recursive_get_all_step_dependencies(
                dag, step=substep, dependencies=dependencies
            )
    else:
        # If step is not in the dag, return the default dependencies (which is an empty set).
        pass

    return dependencies


def get_all_step_dependencies(dag: Dict[str, Any], step: str) -> List[str]:
    """Get all dependencies for a given step in a dag.

    This function returns all dependencies of a step, as well as their direct dependencies, and so on. In the end, the
    result contains all datasets that the given step depends on, directly or indirectly.

    Parameters
    ----------
    dag : Dict[str, Any]
        Dag.
    step : str
        Step (as it appears in the dag).

    Returns
    -------
    dependencies : List[str]
        All dependencies of a given step in a dag.
    """
    dependencies = sorted(_recursive_get_all_step_dependencies(dag=dag, step=step))

    return dependencies


def get_all_step_usages(dag_reverse: Dict[str, Any], step: str) -> List[str]:
    """Get all dependencies for a given step in a dag.

    This function returns all datasets for which a given step is a dependency, as well as those datasets for which they
    are also dependencies, and so on. In the end, the result contains all datasets that use, directly or indirectly, the
    given step.

    Parameters
    ----------
    dag_reverse : Dict[str, Any]
        Dag reversed (a dictionary where each item is step: set of usages).
    step : str
        Step (as it appears in the dag).

    Returns
    -------
    dependencies : List[str]
        All usages of a given step in a dag.

    """
    # A simple solution is to simply reverse the graph, and apply the already existing function that finds all
    # dependencies.
    dependencies = get_all_step_dependencies(dag=dag_reverse, step=step)

    return dependencies


class LatestVersionOfStepShouldBeActive(ExceptionFromDocstring):
    """The latest version of each data step should be in the dag as a main step (maybe it was accidentally removed)."""


def _recursive_get_all_archivable_steps(steps_df: pd.DataFrame, unused_steps: Set[str] = set()) -> Set[str]:
    # Find active meadow/garden steps for which there is a newer version.
    new_unused_steps = set(
        steps_df[
            (~steps_df["step"].isin(unused_steps) & steps_df["n_newer_versions"] > 0)
            & (steps_df["state"] == "active")
            & (steps_df["role"] == "usage")
            & (steps_df["channel"].isin(["meadow", "garden"]))
        ]["step"]
    )
    # Of those, remove the ones that are active dependencies of other steps (excluding the steps in unused_steps).
    new_unused_steps = {
        step
        for step in new_unused_steps
        if (set(steps_df[steps_df["step"] == step]["all_usages"].item()) - unused_steps) == set()
    }

    # Add them to the set of unused steps.
    unused_steps = unused_steps | new_unused_steps

    if new_unused_steps == set():
        # If no new unused step has been detected, return the set of unused steps.
        return unused_steps
    else:
        # Otherwise, repeat the process to keep finding new archivable steps.
        return _recursive_get_all_archivable_steps(steps_df=steps_df, unused_steps=unused_steps)


class VersionTracker:
    """Helper object that loads the dag, provides useful functions to check for versions and dataset dependencies, and
    checks for inconsistencies.

    """

    def __init__(self):
        # Load dag of active and archive steps (a dictionary where each item is step: set of dependencies).
        self.dag_all = load_dag(paths.DAG_ARCHIVE_FILE)
        # Create a reverse dag (a dictionary where each item is step: set of usages).
        self.dag_all_reverse = reverse_graph(graph=self.dag_all)
        # Load dag of active steps.
        self.dag_active = load_dag(paths.DAG_FILE)
        # Generate the dag of only archive steps.
        self.dag_archive = {step: self.dag_all[step] for step in self.dag_all if step not in self.dag_active}
        # List all unique steps that exist in the dag.
        self.all_steps = list_all_steps_in_dag(self.dag_all)
        # List all unique active steps.
        self.all_active_steps = list_all_steps_in_dag(self.dag_active)
        # List all active steps usages (i.e. list of steps in the dag that should be executable by ETL).
        self.all_active_usages = set(self.dag_active)
        # List all steps that are dependencies of active steps.
        self.all_active_dependencies = self.get_all_dependencies_of_active_steps()

        # Dataframe of step attributes will only be initialized once it's called.
        # This dataframe will have one row per existing step.
        self._step_attributes_df = None
        # Dataframe of steps will only be initialized once it's called.
        # This dataframe will have as many rows as entries in the dag.
        self._steps_df = None

        # TODO: Another useful method would be to find in which dag file each step is (by yaml opening each file).
        # TODO: Check that for each active usage there is a script (it has happened a few times that the code for
        # fasttrack steps was removed, but the steps were still in the dag, and we noticed it when running ETL).

    def get_direct_step_dependencies(self, step: str) -> List[str]:
        dependencies = get_direct_step_dependencies(dag=self.dag_all, step=step)

        return dependencies

    def get_direct_step_usages(self, step: str) -> List[str]:
        dependencies = get_direct_step_usages(dag=self.dag_all, step=step)

        return dependencies

    def get_all_step_dependencies(self, step: str) -> List[str]:
        dependencies = get_all_step_dependencies(dag=self.dag_all, step=step)

        return dependencies

    def get_all_step_usages(self, step: str) -> List[str]:
        dependencies = get_all_step_usages(dag_reverse=self.dag_all_reverse, step=step)

        return dependencies

    def get_all_step_versions(self, step: str) -> List[str]:
        return self.steps_df[self.steps_df["step"] == step]["same_steps_all"].item()

    def get_forward_step_versions(self, step: str) -> List[str]:
        return self.steps_df[self.steps_df["step"] == step]["same_steps_forward"].item()

    def get_backward_step_versions(self, step: str) -> List[str]:
        return self.steps_df[self.steps_df["step"] == step]["same_steps_backward"].item()

    def get_all_dependencies_of_active_steps(self) -> List[str]:
        # Gather all dependencies of active steps in the dag.
        active_dependencies = set()
        for step in self.dag_active:
            active_dependencies = active_dependencies | set(self.get_all_step_dependencies(step=step))

        return sorted(active_dependencies)

    def get_all_archivable_steps(self) -> List[str]:
        return sorted(_recursive_get_all_archivable_steps(steps_df=self.steps_df))

    def _create_step_attributes(self) -> pd.DataFrame:
        # Extract all attributes of each unique active/archive/dependency step.
        step_attributes = pd.DataFrame(
            [extract_step_attributes(step).values() for step in self.all_steps],
            columns=["step", "kind", "channel", "namespace", "version", "name", "identifier"],
        )

        # Create custom features that will let us prioritize which datasets to update.

        # Add list of all existing versions for each step.
        versions = (
            step_attributes.groupby("identifier", as_index=False)
            .agg({"version": lambda x: sorted(list(x))})
            .rename(columns={"version": "versions"})
        )
        step_attributes = pd.merge(step_attributes, versions, on="identifier", how="left")

        # Count number of versions for each step.
        step_attributes["n_versions"] = step_attributes["versions"].apply(len)

        # Find the latest version of each step.
        step_attributes["latest_version"] = step_attributes["versions"].apply(lambda x: x[-1])

        # Find how many newer versions exist for each step.
        step_attributes["n_newer_versions"] = [
            row["n_versions"] - row["versions"].index(row["version"]) - 1
            for _, row in step_attributes[["n_versions", "versions", "version"]].iterrows()
        ]

        return step_attributes

    @staticmethod
    def _add_columns_with_different_step_versions(steps_df: pd.DataFrame) -> pd.DataFrame:
        steps_df = steps_df.copy()
        # Create a dataframe with one row per unique step.
        df = steps_df.drop_duplicates(subset="step")[["step", "identifier", "version"]].reset_index(drop=True)
        # For each step, find all alternative versions.
        # One column will contain forward versions, another backward versions, and another all versions.
        other_versions_forward = []
        other_versions_backward = []
        other_versions_all = []
        for _, row in df.iterrows():
            # Create a mask that selects all steps with the same identifier.
            select_same_identifier = df["identifier"] == row["identifier"]
            # Find all forward versions of the current step.
            other_versions_forward.append(
                sorted(set(df[select_same_identifier & (df["version"] > row["version"])]["step"]))
            )
            # Find all backward versions of the current step.
            other_versions_backward.append(
                sorted(set(df[select_same_identifier & (df["version"] < row["version"])]["step"]))
            )
            # Find all versions of the current step.
            other_versions_all.append(sorted(set(df[select_same_identifier]["step"])))
        # Add columns to the dataframe.
        df["same_steps_forward"] = other_versions_forward
        df["same_steps_backward"] = other_versions_backward
        df["same_steps_all"] = other_versions_all
        # Add new columns to the original steps dataframe.
        steps_df = pd.merge(steps_df, df.drop(columns=["identifier", "version"]), on="step", how="left")

        return steps_df

    def _create_steps_df(self) -> pd.DataFrame:
        steps_df = pd.DataFrame({"step": self.all_steps.copy()})
        steps_df["direct_dependencies"] = [self.get_direct_step_dependencies(step=step) for step in self.all_steps]
        steps_df["direct_usages"] = [self.get_direct_step_usages(step=step) for step in self.all_steps]
        steps_df["all_dependencies"] = [self.get_all_step_dependencies(step=step) for step in self.all_steps]
        steps_df["all_usages"] = [self.get_all_step_usages(step=step) for step in self.all_steps]
        steps_df["state"] = ["active" if step in self.all_active_steps else "archive" for step in self.all_steps]
        steps_df["role"] = ["usage" if step in self.dag_all else "dependency" for step in self.all_steps]

        # Add attributes to steps.
        steps = pd.merge(steps_df, self.step_attributes_df, on="step", how="left")

        # Add columns with the list of forward and backwards versions for each step.
        steps = self._add_columns_with_different_step_versions(steps_df=steps)

        return steps

    @property
    def step_attributes_df(self) -> pd.DataFrame:
        if self._step_attributes_df is None:
            self._step_attributes_df = self._create_step_attributes()

        return self._step_attributes_df

    @property
    def steps_df(self) -> pd.DataFrame:
        if self._steps_df is None:
            self._steps_df = self._create_steps_df()

        return self._steps_df

    def _generate_error_for_missing_dependencies(self, missing_steps: Set[str]) -> str:
        error_message = "Missing dependencies in the dag:"
        for missing_step in missing_steps:
            error_message += f"\n* Missing step \n    {missing_step}\n  is a dependency of the following active steps:"
            direct_usages = self.get_direct_step_usages(step=missing_step)
            for usage in direct_usages:
                error_message += f"\n    {usage}"

        return error_message

    def check_that_active_dependencies_are_defined(self) -> None:
        # Gather all steps that appear in the dag only as dependencies, but not as executable steps.
        missing_steps = set(self.all_active_dependencies) - set(self.all_active_usages)

        # Remove those special steps that are expected to not appear in the dag as executable steps (e.g. snapshots).
        channels_to_ignore = ("snapshot", "backport", "etag", "github", "walden")
        missing_steps = set([step for step in missing_steps if not step.startswith(channels_to_ignore)])

        if len(missing_steps) > 0:
            error_message = self._generate_error_for_missing_dependencies(missing_steps=missing_steps)
            log.error(f"{error_message}\n\nSolution: Check if you may have accidentally deleted those missing steps.")

    def check_that_active_dependencies_are_not_archived(self) -> None:
        # Find any archive steps that are dependencies of active steps, and should therefore not be archive steps.
        missing_steps = set(self.dag_archive) & set(self.all_active_dependencies)

        if len(missing_steps) > 0:
            error_message = self._generate_error_for_missing_dependencies(missing_steps=missing_steps)
            log.error(f"{error_message}\n\nSolution: Either archive the active steps or un-archive the archive steps.")

    def check_that_latest_version_of_steps_are_active(self) -> None:
        # Check that the latest version of each main data step is in the dag.
        # If not, it could be because it has been deleted by accident.
        # We may decide to remove this test, because it will raise an error if an old step is archived, and it has no
        # newer version. This can happen for example if the name of the step was changed during the update.
        latest_data_steps = set(
            self.step_attributes_df[
                (self.step_attributes_df["n_newer_versions"] == 0)
                & (self.step_attributes_df["channel"].isin(["meadow", "garden"]))
            ]["step"]
        )
        missing_steps = latest_data_steps - set(list(self.dag_active))
        if len(missing_steps) > 0:
            for missing_step in missing_steps:
                print(f"Step {missing_step} is the latest version of a step and hence should be in the dag.")
            raise LatestVersionOfStepShouldBeActive

    def check_that_all_active_steps_are_necessary(self) -> None:
        # Find all active steps that can safely be archived.
        unused_data_steps = self.get_all_archivable_steps()
        if len(unused_data_steps) > 0:
            error_message = "Some data steps are not used and can safely be archived:"
            for unused_data_step in unused_data_steps:
                error_message += f"\n    {unused_data_step}"
            log.warning(error_message)

    def get_backported_db_dataset_ids(self) -> List[int]:
        """Get list of ids of DB datasets that are used as backported datasets in active steps of ETL.

        Returns
        -------
        backported_dataset_ids : List[int]
            Grapher DB dataset ids that are used in ETL backported datasets.
        """
        backported_dataset_names = [step for step in self.all_active_dependencies if step.startswith("backport://")]
        backported_dataset_ids = sorted(
            set([int(step.split("dataset_")[1].split("_")[0]) for step in backported_dataset_names])
        )

        return backported_dataset_ids

    def apply_sanity_checks(self) -> None:
        self.check_that_active_dependencies_are_defined()
        self.check_that_active_dependencies_are_not_archived()
        # self.check_that_latest_version_of_steps_are_active()
        self.check_that_all_active_steps_are_necessary()


def run_version_tracker_checks():
    VersionTracker().apply_sanity_checks()


def print_tables_metadata_template(tables: List[Table]):
    # This function is meant to be used when creating code in an interactive window (or a notebook).
    # It prints a template for the metadata of the tables in the list.
    # The template can be copied and pasted into the corresponding yaml file.
    # In the future, we should have an interactive tool to add or edit the content of the metadata yaml files, using
    # AI-generated texts when possible.

    # Initialize output dictionary.
    dict_tables = {}
    for tb in tables:
        dict_variables = {}
        for column in tb.columns:
            dict_values = {}
            for field in ["title", "unit", "short_unit", "description_short", "processing_level"]:
                value = getattr(tb[column].metadata, field) or ""

                # Add some simple rules to simplify some common cases.

                # If title is empty, or if title is underscore (probably because it is taken from the column name),
                # create a custom title.
                if (field == "title") and ((value == "") or ("_" in value)):
                    value = column.capitalize().replace("_", " ")

                # If unit or short_unit is empty, and the column name contains 'pct', set it to '%'.
                if (value == "") and (field in ["unit", "short_unit"]) and "pct" in column:
                    value = "%"

                if field == "processing_level":
                    # Assume a minor processing level (it will be manually overwritten, if needed).
                    value = "minor"

                dict_values[field] = value
            dict_variables[column] = dict_values
        dict_tables[tb.metadata.short_name] = {"variables": dict_variables}
    dict_output = {"tables": dict_tables}

    print(yaml.dump(dict_output, default_flow_style=False, sort_keys=False))


@contextmanager
def isolated_env(
    working_dir: Path,
    keep_modules: str = r"openpyxl|pyarrow|lxml|PIL|pydantic|sqlalchemy|sqlmodel|pandas|frictionless|numpy",
) -> Generator[None, None, None]:
    """Add given directory to pythonpath, run code in context, and
    then remove from pythonpath and unimport modules imported in context.

    Note that unimporting modules means they'll have to be imported again, but
    it has minimal impact on performance (ms).

    :param keep_modules: regex of modules to keep imported
    """
    # add module dir to pythonpath
    sys.path.append(working_dir.as_posix())

    # remember modules that were imported before
    imported_modules = set(sys.modules.keys())

    yield

    # unimport modules imported during execution unless they match `keep_modules`
    for module_name in set(sys.modules.keys()) - imported_modules:
        if not re.search(keep_modules, module_name):
            sys.modules.pop(module_name)

    # remove module dir from pythonpath
    sys.path.remove(working_dir.as_posix())


def read_json_schema(path: Union[Path, str]) -> Dict[str, Any]:
    """Read JSON schema with resolved references."""
    path = Path(path)

    # pathlib does not append trailing slashes, but jsonref needs that.
    base_dir_url = path.parent.absolute().as_uri() + "/"
    base_file_url = urljoin(base_dir_url, path.name)
    with path.open("r") as f:
        dix = jsonref.loads(f.read(), base_uri=base_file_url, lazy_load=False)
        return cast(Dict[str, Any], dix)
