#
#  helpers.py
#  etl
#

import re
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Union, cast

import requests
import pandas as pd
from owid import catalog
from owid.catalog import CHANNEL
from owid.datautils.common import ExceptionFromDocstring
from owid.walden import Catalog as WaldenCatalog
from owid.walden import Dataset as WaldenDataset

from etl import paths
from etl.snapshot import Snapshot
from etl.steps import load_dag, reverse_graph


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


class CurrentFileMustBeAStep(ExceptionFromDocstring):
    """Current file must be an ETL step."""


class CurrentStepMustBeInDag(ExceptionFromDocstring):
    """Current step must be listed in the dag."""


class NoMatchingStepsAmongDependencies(ExceptionFromDocstring):
    """No steps found among dependencies of current ETL step, that match the given specifications."""


class MultipleMatchingStepsAmongDependencies(ExceptionFromDocstring):
    """Multiple steps found among dependencies of current ETL step, that match the given specifications."""


class UnknownChannel(ExceptionFromDocstring):
    """Unknown channel name. Valid channels are 'walden', 'snapshot', 'meadow', 'garden', or 'grapher'."""


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

    def __init__(self, __file__: str):
        self.f = Path(__file__)

        # Load dag.
        self.dag = load_dag()

        # Current file should be a data step.
        if not self.f.as_posix().startswith(paths.STEP_DIR.as_posix()):
            raise CurrentFileMustBeAStep

        # It could be either called from a module with short_name.py or __init__.py inside short_name/ dir.
        if len(self.f.relative_to(paths.STEP_DIR).parts) == 6:
            self.f = self.f.parent

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
    def _create_step_name(
        short_name: str,
        channel: Optional[CHANNEL] = None,
        namespace: Optional[str] = None,
        version: Optional[Union[int, str]] = None,
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

        if channel in ["meadow", "garden", "grapher"]:
            step_name = f"data://{channel}/{namespace}/{version}/{short_name}"
        elif channel in ["snapshot", "walden"]:
            step_name = f"{channel}://{namespace}/{version}/{short_name}"
        elif channel is None:
            step_name = rf"(?:snapshot:/|walden:/|data://meadow|data://garden|data://grapher)/{namespace}/{version}/{short_name}"
        else:
            raise UnknownChannel

        return step_name

    @staticmethod
    def _get_attributes_from_step_name(step_name: str) -> Dict[str, str]:
        """Get attributes (channel, namespace, version and short name) from the step name (as it appears in the dag)."""
        channel_type, path = step_name.split("://")
        if channel_type in ["walden", "snapshot"]:
            channel = channel_type
            namespace, version, short_name = path.split("/")
        elif channel_type in ["data"]:
            channel, namespace, version, short_name = path.split("/")
        else:
            raise WrongStepName

        attributes = {"channel": channel, "namespace": namespace, "version": version, "short_name": short_name}

        return attributes

    @property
    def step(self) -> str:
        return self._create_step_name(
            channel=self.channel, namespace=self.namespace, version=self.version, short_name=self.short_name
        )

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
    ) -> str:
        """Get dependency step name (as it appears in the dag) given its attributes (at least its short name)."""
        pattern = self._create_step_name(channel=channel, namespace=namespace, version=version, short_name=short_name)
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
    ) -> Union[catalog.Dataset, Snapshot, WaldenCatalog]:
        """Load a dataset dependency, given its attributes (at least its short name)."""
        dependency_step_name = self.get_dependency_step_name(
            short_name=short_name, channel=channel, namespace=namespace, version=version
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


# For backwards compatibility.
Names = PathFinder


def extract_step_attributes(step):
    # Extract the prefix (whatever is on the left of the '://') and the root of the step name.
    prefix, root = step.split("://")
    
    # Field 'kind' informs whether the dataset is public or private.
    if "private" in prefix:
        kind = "private"
    else:
        kind = "public"
    
    # From now on we remove the 'public' or 'private' from the prefix.
    prefix = prefix.split("-")[0]

    if prefix in ["etag", "github"]:
        # Special kinds of steps.
        channel = "etag"
        namespace = "etag"
        version = "latest"
        name = root
        identifier = root
    elif prefix in ["snapshot", "walden"]:
        # Ingestion steps.
        channel = prefix
        
        # Extract attributes from root of the step.
        namespace, version, name = root.split("/")

        # Define an identifier for this step, that is identical for all versions.
        identifier = f"{channel}/{namespace}/{name}"
    elif root == "garden/reference":
        # This is a special step that does not have a namespace or a version.
        # We should probably get rid of this special step soon. But for now, define its properties manually.
        channel = "garden"
        namespace = "owid"
        version = "latest"
        name = "reference"

        # Define an identifier for this step, that is identical for all versions.
        identifier = f"{channel}/{namespace}/{name}"
    else:
        # Regular data steps.
        
        # Extract attributes from root of the step.
        channel, namespace, version, name = root.split("/")

        # Define an identifier for this step, that is identical for all versions.
        identifier = f"{channel}/{namespace}/{name}"
    
    return step, kind, channel, namespace, version, name, identifier


def list_all_steps_in_dag(dag):
    all_steps = sorted(set([step for step in dag] + sum([list(dag[step]) for step in dag], [])))

    return all_steps


def get_direct_downstream_dependencies_for_step_in_dag(dag, step):
    dependencies = dag[step]

    return dependencies


def get_direct_upstream_dependencies_for_step_in_dag(dag, step):
    used_by = set([_step for _step in dag if step in dag[_step]])

    return used_by


def _recursive_get_all_downstream_dependencies_for_step_in_dag(dag, step, dependencies=set()):
    if step in dag:
        # If step is in the dag, gather all its substeps.
        substeps = dag[step]
        # Add substeps to the set of dependencies (union of sets, to avoid repetitions).
        dependencies = dependencies | set(substeps)
        for substep in substeps:
            # For each of the substeps, repeat the process.
            dependencies = dependencies | _recursive_get_all_downstream_dependencies_for_step_in_dag(dag, step=substep, dependencies=dependencies)
    else:
        # If step is not in the dag, return the default dependencies (which is an empty set).
        pass

    return dependencies


def get_all_downstream_dependencies_for_step_in_dag(dag, step):
    dependencies = _recursive_get_all_downstream_dependencies_for_step_in_dag(dag=dag, step=step)

    return dependencies


def get_all_upstream_dependencies_for_step_in_dag(dag, step):
    reverse_dag = reverse_graph(graph=dag)
    dependencies = get_all_downstream_dependencies_for_step_in_dag(dag=reverse_dag, step=step)

    return dependencies


class ArchiveStepUsedByActiveStep(ExceptionFromDocstring):
    """Archived steps have been found as dependencies of active steps.
    
    The solution is either:
    * To archive those active steps.
    * To un-archive those archive steps.
    """


class VersionTracker:
    def __init__(self):
        # Load dag of active and archive steps.
        self.dag_all = load_dag(include_archive=True)
        # Load dag of active steps.
        self.dag_active = load_dag(include_archive=False)
        # Generate the dag of only archive steps.
        self.dag_archive = {step: self.dag_all[step] for step in self.dag_all if step not in self.dag_active}
        # List all unique steps that exist in the dag.
        self.all_steps = list_all_steps_in_dag(self.dag_all)
        # List all steps that are dependencies of active steps.
        self.all_active_dependencies = self.get_all_dependencies_of_active_steps()

        # Create dataframe of step attributes.
        self.step_attributes_df = self._create_step_attributes_df()
        # Create dataframe of steps.
        self.steps_df = self._create_steps_df()
        
        # Apply sanity check.
        self.check_that_archive_steps_are_not_dependencies_of_active_steps()
        self.check_latest_version_of_steps_are_active()
        self.check_all_active_steps_are_necessary()

        # TODO: Another useful method would be to find in which dag file each step is (by yaml opening each file).

    def get_direct_downstream_dependencies_for_step(self, step):
        dependencies = get_direct_downstream_dependencies_for_step_in_dag(dag=self.dag_all, step=step)

        return dependencies

    def get_direct_upstream_dependencies_for_step(self, step):
        dependencies = get_direct_upstream_dependencies_for_step_in_dag(dag=self.dag_all, step=step)

        return dependencies

    def get_all_downstream_dependencies_for_step(self, step):
        dependencies = get_all_downstream_dependencies_for_step_in_dag(dag=self.dag_all, step=step)

        return dependencies

    def get_all_upstream_dependencies_for_step(self, step):
        dependencies = get_all_upstream_dependencies_for_step_in_dag(dag=self.dag_all, step=step)

        return dependencies

    def get_all_dependencies_of_active_steps(self):
        # Gather all dependencies of active steps in the dag.
        active_dependencies = set()
        for step in self.dag_active:
            active_dependencies = active_dependencies | self.get_all_downstream_dependencies_for_step(step=step)

        return active_dependencies

    def _create_step_attributes_df(self):
        # Extract all attributes of each unique active/archive/dependency step.
        step_attributes = pd.DataFrame(
            [extract_step_attributes(step) for step in self.all_steps],
            columns=["step", "kind", "channel", "namespace", "version", "name", "identifier"])

        # Create custom features that will let us prioritize which datasets to update.

        # Add list of all existing versions for each step.
        versions = step_attributes.groupby("identifier", as_index=False).agg({"version": lambda x: sorted(list(x))}).\
            rename(columns={"version": "versions"})
        step_attributes = pd.merge(step_attributes, versions, on="identifier", how="left")

        # Count number of versions for each step.
        step_attributes["n_versions"] = step_attributes["versions"].apply(len)

        # Find the latest version of each step.
        step_attributes["latest_version"] = step_attributes["versions"].apply(lambda x: x[-1])

        # Find how many newer versions exist for each step.
        step_attributes["n_newer_versions"] = [row["n_versions"] - row["versions"].index(row["version"]) - 1 for i, row in step_attributes[["n_versions", "versions", "version"]].iterrows()]

        return step_attributes

    def _create_steps_df(self):
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
        # For example, given a dag {"a": ["b", "c"]}, the steps dataframe will be:
        # step    used_by
        # ------  ---------
        # a       a
        # b       a
        # c       a
        steps = pd.DataFrame.from_records(steps, columns=["step", "used_by", "status"])
        
        # Add attributes to steps.
        steps = pd.merge(steps, self.step_attributes_df, on="step", how="left")

        return steps

    def check_that_archive_steps_are_not_dependencies_of_active_steps(self):
        # Find any archive steps that are dependencies of active steps, and should therefore not be archive steps.
        missing_steps = set(self.dag_archive) & set(self.all_active_dependencies)

        if len(missing_steps) > 0:
            for missing_step in missing_steps:
                direct_usages = self.get_direct_upstream_dependencies_for_step(step=missing_step)
                print(f"Archive step {missing_step} is used by active steps: {direct_usages}")
            raise ArchiveStepUsedByActiveStep

    def check_latest_version_of_steps_are_active(self):
        # Check that the latest version of each main data step is in the dag.
        # If not, it could be because it has been deleted by accident.
        latest_data_steps = set(self.step_attributes_df[(self.step_attributes_df["n_newer_versions"] == 0) &
                        (self.step_attributes_df["channel"].isin(["meadow", "garden"]))]["step"])
        # The only main data step that is not explicitly in the DAG is the reference dataset (which should be removed soon).
        error = "The latest version of each data step should be in the dag as a main step (if not, maybe it was removed by accident)."
        assert latest_data_steps <= set(list(self.dag_active) + ["data://garden/reference"]), error
    
    def check_all_active_steps_are_necessary(self):
        # TODO: This function may need to become recurrent, because once an unused step is taken out, another step
        #  may also become unnecessary (e.g. the meadow step of an unused garden step will be detected only after the
        #  garden step has been removed).
        outdated_data_steps = set(self.steps_df[(self.steps_df["n_newer_versions"] > 0) & (self.steps_df["status"] == "active") &
                        (self.steps_df["channel"].isin(["meadow", "garden"]))]["step"])

        unused_data_steps = outdated_data_steps - set(self.all_active_dependencies)
        
        if len(unused_data_steps) > 0:
            # TODO: Make a proper warning.
            print(f"WARNING: Some data steps can be safely archived: {unused_data_steps}")
