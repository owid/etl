#
#  helpers.py
#  etl
#

import re
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Set, Union, cast

import pandas as pd
import requests
import structlog
from owid import catalog
from owid.catalog import CHANNEL
from owid.catalog.datasets import DEFAULT_FORMATS, FileFormat
from owid.datautils.common import ExceptionFromDocstring
from owid.walden import Catalog as WaldenCatalog
from owid.walden import Dataset as WaldenDataset

from etl import paths
from etl.snapshot import Snapshot, SnapshotMeta
from etl.steps import extract_step_attributes, load_dag, reverse_graph
from etl.steps.data.converters import convert_snapshot_metadata

log = structlog.get_logger()


@contextmanager
def downloaded(url: str) -> Iterator[str]:
    """
    Download the url to a temporary file and yield the filename.
    """
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
    for tab in ds:
        assert {"year", "country"} <= set(tab.reset_index().columns), "Table must have columns country and year."
        for col in tab:
            if col in ("year", "country"):
                continue
            catalog.utils.validate_underscore(col)
            assert tab[col].metadata.unit is not None, f"Column {col} must have a unit."
            assert tab[col].metadata.title is not None, f"Column {col} must have a title."


def create_dataset(
    dest_dir: Union[str, Path],
    tables: Iterable[catalog.Table],
    default_metadata: Optional[Union[SnapshotMeta, catalog.DatasetMeta]] = None,
    underscore_table: bool = True,
    formats: List[FileFormat] = DEFAULT_FORMATS,
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

    Usage:
        ds = create_dataset(dest_dir, [table_a, table_b], default_metadata=snap.metadata)
        ds.save()
    """
    # convert snapshot SnapshotMeta to DatasetMeta
    if isinstance(default_metadata, SnapshotMeta):
        default_metadata = convert_snapshot_metadata(default_metadata)

    # create new dataset with new metadata
    ds = catalog.Dataset.create_empty(dest_dir, metadata=default_metadata)

    # add tables to dataset
    for table in tables:
        if underscore_table:
            table = catalog.utils.underscore_table(table)
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

    # update metadata from yaml file
    N = PathFinder(str(paths.STEP_DIR / "data" / Path(dest_dir).relative_to(Path(dest_dir).parents[3])))
    if N.metadata_path.exists():
        ds.update_metadata(N.metadata_path)

        # check that we are not using metadata inconsistent with path
        for k, v in match.groupdict().items():
            assert str(getattr(ds.metadata, k)) == v, f"Metadata {k} is inconsistent with path {dest_dir}"

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

        if channel in ["meadow", "garden", "grapher", "explorers", "examples"]:
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

    def load_snapshot_dependency(self) -> Snapshot:
        """Load snapshot dependency with the same name."""
        snap = self.load_dependency(channel="snapshot", short_name=self.short_name)
        assert isinstance(snap, Snapshot)
        return snap

    def load_dataset_dependency(self) -> catalog.Dataset:
        """Load dataset dependency with the same name."""
        dataset = self.load_dependency(short_name=self.short_name)
        assert isinstance(dataset, catalog.Dataset)
        return dataset


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


def get_direct_dependencies_for_step_in_dag(dag: Dict[str, Any], step: str) -> Set[str]:
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
    dependencies : Set[str]
        Direct dependencies of a step in a dag.

    """
    dependencies = dag[step]

    return dependencies


def get_direct_usages_for_step_in_dag(dag: Dict[str, Any], step: str) -> Set[str]:
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
    dependencies : Set[str]
        Direct usages of a step in a dag.

    """

    used_by = set([_step for _step in dag if step in dag[_step]])

    return used_by


def _recursive_get_all_dependencies_for_step_in_dag(
    dag: Dict[str, Any], step: str, dependencies: Set[str] = set()
) -> Set[str]:
    if step in dag:
        # If step is in the dag, gather all its substeps.
        substeps = dag[step]
        # Add substeps to the set of dependencies (union of sets, to avoid repetitions).
        dependencies = dependencies | set(substeps)
        for substep in substeps:
            # For each of the substeps, repeat the process.
            dependencies = dependencies | _recursive_get_all_dependencies_for_step_in_dag(
                dag, step=substep, dependencies=dependencies
            )
    else:
        # If step is not in the dag, return the default dependencies (which is an empty set).
        pass

    return dependencies


def get_all_dependencies_for_step_in_dag(dag: Dict[str, Any], step: str) -> Set[str]:
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
    dependencies : Set[str]
        All dependencies of a given step in a dag.
    """
    dependencies = _recursive_get_all_dependencies_for_step_in_dag(dag=dag, step=step)

    return dependencies


def get_all_usages_for_step_in_dag(dag: Dict[str, Any], step: str) -> Set[str]:
    """Get all dependencies for a given step in a dag.

    This function returns all datasets for which a given step is a dependency, as well as those datasets for which they
    are also dependencies, and so on. In the end, the result contains all datasets that use, directly or indirectly, the
    given step.

    Parameters
    ----------
    dag : Dict[str, Any]
        Dag.
    step : str
        Step (as it appears in the dag).

    Returns
    -------
    dependencies : Set[str]
        All usages of a given step in a dag.

    """
    # A simple solution is to simply reverse the graph, and apply the already existing function that finds all
    # dependencies.
    reverse_dag = reverse_graph(graph=dag)
    dependencies = get_all_dependencies_for_step_in_dag(dag=reverse_dag, step=step)

    return dependencies


class ArchiveStepUsedByActiveStep(ExceptionFromDocstring):
    """Archived steps have been found as dependencies of active steps.

    The solution is either:
    * To archive those active steps.
    * To un-archive those archive steps.
    """


class LatestVersionOfStepShouldBeActive(ExceptionFromDocstring):
    """The latest version of each data step should be in the dag as a main step (maybe it was accidentally removed)."""


class VersionTracker:
    """Helper object that loads the dag, provides useful functions to check for versions and dataset dependencies, and
    checks for inconsistencies.

    """

    def __init__(self):
        # Load dag of active and archive steps.
        self.dag_all = load_dag(paths.DAG_ARCHIVE_FILE)
        # Load dag of active steps.
        self.dag_active = load_dag(paths.DAG_FILE)
        # Generate the dag of only archive steps.
        self.dag_archive = {step: self.dag_all[step] for step in self.dag_all if step not in self.dag_active}
        # List all unique steps that exist in the dag.
        self.all_steps = list_all_steps_in_dag(self.dag_all)
        # List all unique active steps.
        self.all_active_steps = list_all_steps_in_dag(self.dag_active)
        # List all steps that are dependencies of active steps.
        self.all_active_dependencies = self.get_all_dependencies_of_active_steps()

        # Dataframe of step attributes will only be initialized once it's called.
        # This dataframe will have one row per existing step.
        self._step_attributes_df = None
        # Dataframe of steps will only be initialized once it's called.
        # This dataframe will have as many rows as entries in the dag.
        self._steps_df = None

        # TODO: Another useful method would be to find in which dag file each step is (by yaml opening each file).

    def get_direct_dependencies_for_step(self, step: str) -> Set[str]:
        dependencies = get_direct_dependencies_for_step_in_dag(dag=self.dag_all, step=step)

        return dependencies

    def get_direct_usages_for_step(self, step: str) -> Set[str]:
        dependencies = get_direct_usages_for_step_in_dag(dag=self.dag_all, step=step)

        return dependencies

    def get_all_dependencies_for_step(self, step: str) -> Set[str]:
        dependencies = get_all_dependencies_for_step_in_dag(dag=self.dag_all, step=step)

        return dependencies

    def get_all_usages_for_step(self, step: str) -> Set[str]:
        dependencies = get_all_usages_for_step_in_dag(dag=self.dag_all, step=step)

        return dependencies

    def get_all_dependencies_of_active_steps(self) -> Set[str]:
        # Gather all dependencies of active steps in the dag.
        active_dependencies = set()
        for step in self.dag_active:
            active_dependencies = active_dependencies | self.get_all_dependencies_for_step(step=step)

        return active_dependencies

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
            for i, row in step_attributes[["n_versions", "versions", "version"]].iterrows()
        ]

        return step_attributes

    def _create_steps_df(self) -> pd.DataFrame:
        steps = []
        # Gather active steps and their dependencies.
        for step in self.dag_active:
            steps.append([step, step, "active"])
            for substep in self.dag_all[step]:
                steps.append([substep, step, "dependency"])
        # Gather archive steps and their dependencies.
        for step in self.dag_archive:
            steps.append([step, step, "archive"])
            for substep in self.dag_archive[step]:
                steps.append([substep, step, "dependency"])

        # Store all steps and their dependencies.
        # Column "used_by" includes:
        # * For a dependency step, the main step that is using it.
        # * For a main step, the main step itself.
        steps = pd.DataFrame.from_records(steps, columns=["step", "used_by", "status"])

        # Add attributes to steps.
        steps = pd.merge(steps, self.step_attributes_df, on="step", how="left")

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

    def check_that_archive_steps_are_not_dependencies_of_active_steps(self) -> None:
        # Find any archive steps that are dependencies of active steps, and should therefore not be archive steps.
        missing_steps = set(self.dag_archive) & set(self.all_active_dependencies)

        if len(missing_steps) > 0:
            for missing_step in missing_steps:
                direct_usages = self.get_direct_usages_for_step(step=missing_step)
                print(f"Archive step {missing_step} is used by active steps: {direct_usages}")
            raise ArchiveStepUsedByActiveStep

    def check_that_latest_version_of_steps_are_active(self) -> None:
        # Check that the latest version of each main data step is in the dag.
        # If not, it could be because it has been deleted by accident.
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
        # TODO: This function may need to become recurrent, because once an unused step is taken out, another step
        #  may also become unnecessary (e.g. the meadow step of an unused garden step will be detected only after the
        #  garden step has been removed).
        outdated_data_steps = set(
            self.steps_df[
                (self.steps_df["n_newer_versions"] > 0)
                & (self.steps_df["status"] == "active")
                & (self.steps_df["channel"].isin(["meadow", "garden"]))
            ]["step"]
        )

        unused_data_steps = outdated_data_steps - set(self.all_active_dependencies)

        if len(unused_data_steps) > 0:
            log.warning(f"Some data steps can be safely archived: {unused_data_steps}")

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
        self.check_that_archive_steps_are_not_dependencies_of_active_steps()
        self.check_that_latest_version_of_steps_are_active()
        self.check_that_all_active_steps_are_necessary()


def run_version_tracker_checks():
    VersionTracker().apply_sanity_checks()
