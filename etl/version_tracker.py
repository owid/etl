from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import click
import numpy as np
import pandas as pd
import structlog
import yaml
from rich_click.rich_command import RichCommand

from etl import paths
from etl.config import ADMIN_HOST
from etl.db import can_connect
from etl.grapher_io import get_info_for_etl_datasets
from etl.steps import extract_step_attributes, load_dag, reverse_graph

log = structlog.get_logger()

# Define the temporary step that will depend on newly created snapshots before they are used by any active steps.
DAG_TEMP_STEP = "data-private://meadow/temp/latest/step"

# Define the base URL for the grapher datasets (which will be different depending on the environment).
GRAPHER_DATASET_BASE_URL = f"{ADMIN_HOST}/admin/datasets/"

# Maximum number of days allowed for an unused new step before it's considered archivable.
# If, within that time period, no charts or external steps use it, the step will become archivable.
MAX_NUM_DAYS_BEFORE_ARCHIVABLE = 30

# Get current date (used to estimate the number of days until the next update of each step).
TODAY = pd.to_datetime(datetime.now().strftime("%Y-%m-%d"))

# List of dependencies to ignore when calculating the update state.
# This is done to avoid a certain common dependency (e.g. hyde) to make all steps appear as needing a major update.
DEPENDENCIES_TO_IGNORE = [
    "snapshot://hyde/2017/general_files.zip",
]


# Define labels for update states.
class UpdateState(Enum):
    UNKNOWN = "Unknown"
    UP_TO_DATE = "No updates known"
    OUTDATED = "Outdated"
    MINOR_UPDATE = "Minor update possible"
    MAJOR_UPDATE = "Major update possible"
    ARCHIVABLE = "Archivable"
    ARCHIVED = "Archived"
    UNUSED = "Not yet used"


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


def load_steps_for_each_dag_file() -> Dict[str, Dict[str, List[str]]]:
    """Return a dictionary of all ETL (active and archive) dag files, and the steps they contain.

    Returns
    -------
    dag_file_steps : Dict[str, Dict[str, List[str]]]
        Dictionary with items {"active": {step_1: dag_file_name_1, step_2: dag_file_name_2, ...}, "archive": {...}}.
    """
    # Create a temporary dictionary with the path to the folder of active and archive dag files.
    dag_file_paths = {"active": paths.DAG_DIR.glob("*.yml"), "archive": paths.DAG_DIR.glob("archive/*.yml")}
    # Create a dictionary that will contain the content of the active dag files and the archive dag files.
    dag_file_steps = {"active": {}, "archive": {}}
    for dag_file_path in dag_file_paths:
        for dag_file in dag_file_paths[dag_file_path]:
            # Open the current dag file and read its steps.
            with open(dag_file, "r") as f:
                content = yaml.load(f, Loader=yaml.Loader)["steps"]
                if content:
                    # Add an entry to the dictionary, with the name of the dag file, and the set of steps it contains.
                    dag_file_steps[dag_file_path][dag_file.stem] = content

    return dag_file_steps


def load_dag_file_for_each_step() -> Dict[str, str]:
    """Return a dictionary of all ETL (active and archive) steps and name of their dag file.

    Returns
    -------
    dag_file_steps_reverse : Dict[str, str]
        Dictionary with items {step_1: dag_file_name_1, step_2: dag_file_name_2, ...}.
    """
    dag_file_steps = load_steps_for_each_dag_file()
    # Reverse active dictionary of dag files.
    active_dag_files = reverse_graph(dag_file_steps["active"])  # type: ignore
    dag_file_steps_reverse = {
        step: list(active_dag_files[step])[0] for step in active_dag_files if len(active_dag_files[step]) > 0
    }
    # Add the reverse of the archive dictionary of dag files.
    archive_dag_files = reverse_graph(dag_file_steps["archive"])  # type: ignore
    dag_file_steps_reverse.update(
        {step: list(archive_dag_files[step])[0] for step in archive_dag_files if len(archive_dag_files[step]) > 0}
    )

    return dag_file_steps_reverse


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


def _recursive_get_all_step_dependencies_ndim(
    dag: Dict[str, Any], step: str, memo: Dict[str, Set[str]]
) -> Tuple[Set[str], Dict[str, Set[str]]]:
    """Optimised version of `_recursive_get_all_step_dependencies` using `memo` to store already computed dependencies."""
    if step in memo:
        # Return already computed dependencies immediately
        return memo[step], memo

    dependencies = set()
    if step in dag:
        substeps = dag[step]
        # Update dependencies by adding the substeps
        dependencies.update(substeps)
        # Recursively gather dependencies for each substep
        for substep in substeps:
            _dependencies, memo = _recursive_get_all_step_dependencies_ndim(dag, substep, memo)
            dependencies.update(_dependencies)

    # Store the computed dependencies in memo before returning
    memo[step] = dependencies
    return dependencies, memo


class VersionTracker:
    """Helper object that loads the dag, provides useful functions to check for versions and dataset dependencies, and
    checks for inconsistencies.

    """

    # List of steps known to be archivable (or unused), that we want to keep in the active dag for technical reasons.
    ARCHIVABLE_STEPS_TO_KEEP = [
        DAG_TEMP_STEP,
        "data://explorers/dummy/2020-01-01/dummy",
        "data://garden/dummy/2020-01-01/dummy",
        "data://garden/dummy/2020-01-01/dummy_full",
        "data://garden/dummy/2023-10-12/dummy_monster",
        "data://grapher/dummy/2020-01-01/dummy",
        "data://grapher/dummy/2020-01-01/dummy_full",
        "data://grapher/dummy/2023-10-12/dummy_monster",
        "data://meadow/dummy/2020-01-01/dummy",
        "data://meadow/dummy/2020-01-01/dummy_full",
        "snapshot://dummy/2020-01-01/dummy.csv",
        "snapshot://dummy/2020-01-01/dummy_full.csv",
        "data://examples/examples/latest/jupytext_example",
        "data://examples/examples/latest/notebook_example",
        "data://examples/examples/latest/script_example",
        "data://examples/examples/latest/vs_code_cells_example",
        "data-private://examples/examples/latest/private_example",
    ]

    # List of metrics to fetch related to analytics.
    ANALYTICS_COLUMNS = [
        # "views_7d",
        # "views_14d",
        "views_365d",
    ]

    def __init__(
        self,
        connect_to_db: bool = True,
        warn_on_archivable: bool = True,
        warn_on_unused: bool = True,
        ignore_archive: bool = False,
    ):
        # Load dag of active steps (a dictionary step: set of dependencies).
        self.dag_active = load_dag(paths.DAG_FILE)
        if ignore_archive:
            # Fully ignore the archive dag (so that all steps are only active steps, and there are no archive steps).
            self.dag_all = self.dag_active.copy()
        else:
            # Load dag of active and archive steps.
            self.dag_all = load_dag(paths.DAG_ARCHIVE_FILE)
        # Create a reverse dag (a dictionary where each item is step: set of usages).
        self.dag_all_reverse = reverse_graph(graph=self.dag_all)
        # Create a reverse dag (a dictionary where each item is step: set of usages) of active steps.
        self.dag_active_reverse = reverse_graph(graph=self.dag_active)
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
        # Create a dictionary of dag files for each step.
        self.dag_file_for_each_step = load_dag_file_for_each_step()

        # If connect_to_db is True, attempt to connect to DB to extract additional info about charts.
        self.connect_to_db = connect_to_db
        if self.connect_to_db and not can_connect():
            log.warning("Unable to connect to DB. Some checks will be skipped and charts info will not be available.")
            self.connect_to_db = False

        # Warn about archivable steps.
        self.warn_on_archivable = warn_on_archivable

        # Warn about unused steps.
        self.warn_on_unused = warn_on_unused

        # Initialize a dataframe of steps that have a grapher dataset (with or without charts) but does not exist in the
        # dag (active or archive) anymore.
        self.unknown_steps_with_grapher_dataset_df = None

        # Dataframe of step attributes will only be initialized once it's called.
        # This dataframe will have one row per existing step.
        self._step_attributes_df = None
        # Dataframe of steps will only be initialized once it's called.
        # This dataframe will have as many rows as entries in the dag.
        self._steps_df = None

    def get_direct_step_dependencies(self, step: str) -> List[str]:
        """Get direct dependencies of a given step in the dag."""
        dependencies = get_direct_step_dependencies(dag=self.dag_all, step=step)

        return dependencies

    def get_direct_step_usages(self, step: str) -> List[str]:
        """Get direct usages of a given step in the dag."""
        dependencies = get_direct_step_usages(dag=self.dag_all, step=step)

        return dependencies

    def get_direct_step_uses_ndim(self):
        # Initialize a dictionary to hold sets of direct usages
        # key: step in use, value: list of steps directly using key
        direct_usages_dict = {step: set() for step in self.all_steps}

        # Iterate over all the DAG
        for _step, dependencies in self.dag_all.items():
            # Detect step being used
            for step in dependencies:
                if step in direct_usages_dict:
                    direct_usages_dict[step].add(_step)

        # Convert sets back to sorted lists (if needed) and store in direct_usages
        direct_usages = [sorted(direct_usages_dict[step]) for step in self.all_steps]
        return direct_usages

    def get_all_step_dependencies(self, step: str, only_active: bool = False) -> List[str]:
        """Get all dependencies for a given step in the dag (including dependencies of dependencies)."""
        if only_active:
            dependencies = get_all_step_dependencies(dag=self.dag_active, step=step)
        else:
            dependencies = get_all_step_dependencies(dag=self.dag_all, step=step)

        return dependencies

    def get_all_step_usages(self, step: str, only_active: bool = False) -> List[str]:
        """Get all usages for a given step in the dag (including usages of usages)."""
        if only_active:
            dependencies = get_all_step_usages(dag_reverse=self.dag_active_reverse, step=step)
        else:
            dependencies = get_all_step_usages(dag_reverse=self.dag_all_reverse, step=step)

        return dependencies

    def get_all_step_usages_ndim(self, only_active: bool = False) -> List[str]:
        """Get all usages for a given step in the dag (including usages of usages)."""
        dependencies = []
        memo = {}
        for step in self.all_steps:
            # Pass the memo dictionary to store already computed dependencies
            if only_active:
                dependencies_, memo = _recursive_get_all_step_dependencies_ndim(
                    dag=self.dag_active_reverse, step=step, memo=memo
                )
            else:
                dependencies_, memo = _recursive_get_all_step_dependencies_ndim(
                    dag=self.dag_all_reverse, step=step, memo=memo
                )
            dependencies.append(sorted(dependencies_))

        return dependencies

    def get_all_step_versions(self, step: str) -> List[str]:
        """Get all versions of a given step in the dag."""
        return self.steps_df[self.steps_df["step"] == step]["same_steps_all"].item()

    def get_forward_step_versions(self, step: str) -> List[str]:
        """Get all forward versions of a given step in the dag."""
        return self.steps_df[self.steps_df["step"] == step]["same_steps_forward"].item()

    def get_backward_step_versions(self, step: str) -> List[str]:
        """Get all backward versions of a given step in the dag."""
        return self.steps_df[self.steps_df["step"] == step]["same_steps_backward"].item()

    def get_all_dependencies_of_active_steps(self) -> List[str]:
        """Get all dependencies of active steps in the dag."""
        # Gather all dependencies of active steps in the dag.
        active_dependencies = set()
        for step in self.dag_active:
            active_dependencies = active_dependencies | set(self.get_all_step_dependencies(step=step))

        return sorted(active_dependencies)

    def get_dag_file_for_step(self, step: str) -> str:
        """Get the name of the dag file for a given step."""
        if step in self.dag_file_for_each_step:
            dag_file_name = self.dag_file_for_each_step[step]
        else:
            dag_file_name = ""

        return dag_file_name

    def get_path_to_script(self, step: str, omit_base_dir: bool = False) -> Optional[Path]:
        """Get the path to the script of a given step."""
        # Get step attributes.
        _, step_type, _, channel, namespace, version, name, _ = extract_step_attributes(step=step).values()

        # Create a dictionary that contains the path to a script for a given step.
        # This dictionary has to keys, namely "active" and "archive".
        # Active steps should have a script in the active directory.
        # But steps that are in the archive dag can be either in the active or the archive directory.
        path_to_script = None
        if step_type == "export":
            path_to_script = paths.STEP_DIR / "export" / channel / namespace / version / name  # type: ignore
        elif channel == "snapshot":
            path_to_script = paths.SNAPSHOTS_DIR / namespace / version / name  # type: ignore
        elif channel in ["meadow", "garden", "grapher", "explorers", "open_numbers", "examples", "external"]:
            path_to_script = paths.STEP_DIR / "data" / channel / namespace / version / name  # type: ignore
        elif channel == "walden":
            path_to_script = paths.BASE_DIR / "lib" / "walden" / "ingests" / namespace / version / name  # type: ignore
        elif channel in ["backport", "etag"]:
            # Ignore these channels, for which there is never a script.
            return None
        else:
            log.error(f"Unknown channel {channel} for step {step}.")

        path_to_script_detected = None
        # A step script can exist either as a .py file, as a .ipynb file, or a __init__.py file inside a folder.
        # In the case of snapshots, there may or may not be a .py file, but there definitely needs to be a dvc file.
        # In that case, the corresponding script is not trivial to find, but at least we can return the dvc file.
        for path_to_script_candidate in [
            path_to_script.with_suffix(".py"),  # type: ignore
            path_to_script.with_suffix(".ipynb"),  # type: ignore
            path_to_script / "__init__.py",  # type: ignore
            path_to_script.with_name(path_to_script.name + ".dvc"),  # type: ignore
        ]:
            if path_to_script_candidate.exists():
                path_to_script_detected = path_to_script_candidate
                break
        if path_to_script_detected is None:
            log.error(f"Script for step {step} not found.")

        if omit_base_dir and path_to_script_detected is not None:
            # Return the path relative to the base directory (omitting the local path to the ETL repos).
            path_to_script_detected = path_to_script_detected.relative_to(paths.BASE_DIR)

        return path_to_script_detected

    def _create_step_attributes(self) -> pd.DataFrame:
        # Extract all attributes of each unique active/archive/dependency step.
        step_attributes = pd.DataFrame(
            [extract_step_attributes(step).values() for step in self.all_steps],
            columns=["step", "step_type", "kind", "channel", "namespace", "version", "name", "identifier"],
        )

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

    def _add_steps_update_state(self, steps_df: pd.DataFrame) -> pd.DataFrame:
        # Separate active and inactive steps.
        steps_active_df = steps_df[steps_df["state"] == "active"].reset_index()
        steps_inactive_df = steps_df[steps_df["state"] == "archive"].reset_index()

        # To speed up calculations, create a dictionary with all info in steps_df.
        steps_dict = steps_active_df.set_index("step").to_dict(orient="index")

        # Add a column with the dependencies that are not their latest version.
        steps_active_df["updateable_dependencies"] = [
            [
                dependency
                for dependency in dependencies
                if (dependency not in DEPENDENCIES_TO_IGNORE) and (not steps_dict[dependency]["is_latest"])
            ]
            for dependencies in steps_active_df["all_active_dependencies"]
        ]

        # Add a column with the total number of dependencies that are not their latest version.
        steps_active_df["n_updateable_dependencies"] = [
            len(dependencies) for dependencies in steps_active_df["updateable_dependencies"]
        ]
        # Number of snapshot dependencies that are not their latest version.
        steps_active_df["n_updateable_snapshot_dependencies"] = [
            sum(
                [
                    not steps_dict[dependency]["is_latest"]
                    if steps_dict[dependency]["channel"] == "snapshot"
                    else False
                    for dependency in dependencies
                    if dependency not in DEPENDENCIES_TO_IGNORE
                ]
            )
            for dependencies in steps_active_df["all_active_dependencies"]
        ]
        # Add a column with the number of dependencies from the explorers and external channels.
        steps_active_df["external_usages"] = [
            [usage for usage in usages if steps_dict[usage]["channel"] in ["explorers", "external"]]
            for usages in steps_active_df["all_active_usages"]
        ]
        # Add a column with the total number of external usages.
        steps_active_df["n_external_usages"] = [len(usage) for usage in steps_active_df["external_usages"]]
        # Add a column with the update state.
        # By default, the state is unknown.
        steps_active_df["update_state"] = UpdateState.UNKNOWN.value
        # If there is a newer version of the step, it is outdated.
        steps_active_df.loc[~steps_active_df["is_latest"], "update_state"] = UpdateState.OUTDATED.value
        # If there are any dependencies that are not their latest version, it needs a minor update.
        # NOTE: If any of those dependencies is a snapshot, it needs a major update (defined in the following line).
        steps_active_df.loc[
            (steps_active_df["is_latest"]) & (steps_active_df["n_updateable_dependencies"] > 0), "update_state"
        ] = UpdateState.MINOR_UPDATE.value
        # If there are any snapshot dependencies that are not their latest version, it needs a major update.
        steps_active_df.loc[
            (steps_active_df["is_latest"]) & (steps_active_df["n_updateable_snapshot_dependencies"] > 0), "update_state"
        ] = UpdateState.MAJOR_UPDATE.value
        # If the step does not need to be updated (i.e. update_period_days = 0) or if all dependencies are up to date,
        # then the step is up to date (in other words, we are not aware of any possible update).
        steps_active_df.loc[
            (steps_active_df["update_period_days"] == 0)
            | (
                (steps_active_df["is_latest"])
                & (steps_active_df["n_updateable_snapshot_dependencies"] == 0)
                & (steps_active_df["n_updateable_dependencies"] == 0)
            ),
            "update_state",
        ] = UpdateState.UP_TO_DATE.value
        # If a step is not the latest version, has no charts, and no external usages, it is archivable.
        # NOTE: See that below we also make archivable all steps that have been unused for too long.
        steps_active_df.loc[
            (steps_active_df["n_charts"] == 0)
            & (steps_active_df["n_external_usages"] == 0)
            & (~steps_active_df["is_latest"]),
            "update_state",
        ] = UpdateState.ARCHIVABLE.value
        # If a step is the latest version but has no charts and no external usages, it is unused.
        steps_active_df.loc[
            (steps_active_df["n_charts"] == 0)
            & (steps_active_df["n_external_usages"] == 0)
            & (steps_active_df["is_latest"]),
            "update_state",
        ] = UpdateState.UNUSED.value

        def _days_since_step_creation(version):
            # Calculate the number of days since the creation of the step.
            if version == "latest":
                # If the version is 'latest', assume the step was created today.
                return 0
            try:
                # If the version is a full date, use it to calculate the number of days since then.
                version_date = pd.to_datetime(version).date()
            except ValueError:
                # If the version is a year, assume the step was created on the first day of that year.
                version_date = pd.to_datetime(f"{version}-01-01").date()
            return (TODAY.date() - version_date).days

        # Make archivable all steps that have been unused for too long.
        steps_active_df.loc[
            (steps_active_df["update_state"] == UpdateState.UNUSED.value)
            & (steps_active_df["version"].apply(_days_since_step_creation) > MAX_NUM_DAYS_BEFORE_ARCHIVABLE),
            "update_state",
        ] = UpdateState.ARCHIVABLE.value

        # There are special steps that, even though they are archivable or unused, we want to keep in the active dag.
        steps_active_df.loc[
            steps_active_df["step"].isin(self.ARCHIVABLE_STEPS_TO_KEEP), "update_state"
        ] = UpdateState.UP_TO_DATE.value

        # All explorers and external steps should be considered up to date.
        steps_active_df.loc[
            steps_active_df["channel"].isin(["explorers", "external"]), "update_state"
        ] = UpdateState.UP_TO_DATE.value

        # Add update state to archived steps.
        steps_inactive_df["update_state"] = UpdateState.ARCHIVED.value

        # Concatenate active and inactive steps.
        steps_df = pd.concat([steps_active_df, steps_inactive_df], ignore_index=True)

        return steps_df

    @staticmethod
    def _add_columns_with_different_step_versions(steps_df: pd.DataFrame) -> pd.DataFrame:
        steps_df = steps_df.copy()
        # Create a dataframe with one row per unique step.
        df = steps_df.drop_duplicates(subset="step")[["step", "identifier", "version"]].reset_index(drop=True)

        # Only run for steps that have more than one version.
        more_than_one_version = df["identifier"].value_counts() > 1
        ids_more_than_one_version = list(more_than_one_version[more_than_one_version].index)
        df_n = df[df["identifier"].isin(ids_more_than_one_version)].copy()

        # For each step, find all alternative versions.
        # New columns will contain forward versions, backward versions, all versions, and latest version.
        other_versions_forward = []
        other_versions_backward = []
        other_versions_all = []
        latest_version = []
        for _, row in df_n.iterrows():
            # Create a mask that selects all steps with the same identifier.
            select_same_identifier = df_n["identifier"] == row["identifier"]
            # Find all forward versions of the current step.
            versions_forward = sorted(set(df_n[select_same_identifier & (df_n["version"] > row["version"])]["step"]))
            other_versions_forward.append(versions_forward)
            # Find all backward versions of the current step.
            other_versions_backward.append(
                sorted(set(df_n[select_same_identifier & (df_n["version"] < row["version"])]["step"]))
            )
            # Find all versions of the current step.
            other_versions_all.append(sorted(set(df_n[select_same_identifier]["step"])))
            # Find latest version of the current step.
            latest_version.append(versions_forward[-1] if len(versions_forward) > 0 else row["step"])
        # Add columns to the dataframe.
        df_n["same_steps_forward"] = other_versions_forward
        df_n["same_steps_backward"] = other_versions_backward
        df_n["same_steps_all"] = other_versions_all
        df_n["same_steps_latest"] = latest_version

        # Only one version
        ids_one_version = list(more_than_one_version[~more_than_one_version].index)
        df_1 = df[df["identifier"].isin(ids_one_version)].copy()
        empty_lists = [[] for n in range(len(df_1))]
        df_1["same_steps_forward"] = empty_lists
        df_1["same_steps_backward"] = empty_lists
        df_1["same_steps_all"] = df_1["step"].str.split()
        df_1["same_steps_latest"] = df_1["step"]

        # Concatenate the two dataframes.
        df = pd.concat([df_n, df_1], ignore_index=True)

        # Add new columns to the original steps dataframe.
        steps_df = pd.merge(steps_df, df.drop(columns=["identifier", "version"]), on="step", how="left")

        return steps_df

    def _add_info_from_db(self, steps_df: pd.DataFrame) -> pd.DataFrame:
        steps_df = steps_df.copy()
        # Fetch all info about datasets from the DB.
        info_df = get_info_for_etl_datasets().rename(
            columns={
                "dataset_id": "db_dataset_id",
                "dataset_name": "db_dataset_name",
                "is_private": "db_private",
                "is_archived": "db_archived",
                "update_period_days": "update_period_days",
                "views_7d": "chart_views_7d",
                "views_14d": "chart_views_14d",
                "views_365d": "chart_views_365d",
            },
            errors="raise",
        )
        # Combine steps_df with info_df.
        # To do that, note that info_df contains an "etl_path", e.g.
        # 'grapher/ggdc/2020-10-01/ggdc_maddison/maddison_gdp#gdp'
        # while steps_df contains a dataset "step", e.g. 'data://grapher/ggdc/2020-10-01/ggdc_maddison'.
        # So, first, create a temporary merge key to both dataframes.
        # Note that info_df only contains data steps (that should start with either "data://" or "data-private://").
        steps_df["merge_key"] = steps_df["step"].str.replace("data://", "").str.replace("data-private://", "")
        info_df["merge_key"] = ["/".join(etl_path.split("#")[0].split("/")[:-1]) for etl_path in info_df["etl_path"]]

        # NOTE: This dataframe may have more steps than steps_df, for different reasons:
        #  * A grapher dataset was created by ETL, but the ETL step has been removed from the dag, while the
        #    grapher dataset still exists. This shouldn't happen, but it does happen (mostly for fasttrack drafts).
        #  * A grapher dataset (in production) has been created by ETL in a branch which is not yet merged. This,
        #    again, should not happen, but it does happen every now and then, exceptionally.
        # Identify these steps (and ignore archive DB datasets).
        self.unknown_steps_with_grapher_dataset_df = info_df[
            (~info_df["merge_key"].isin(steps_df["merge_key"])) & (~info_df["db_archived"])
        ].reset_index(drop=True)

        # Add info from db to steps dataframe.
        steps_df = pd.merge(
            steps_df,
            info_df[
                [
                    "merge_key",
                    "chart_ids",
                    "chart_slugs",
                    "db_dataset_id",
                    "db_dataset_name",
                    "db_private",
                    "db_archived",
                    "update_period_days",
                ]
                + [f"chart_{metric}" for metric in self.ANALYTICS_COLUMNS]
            ],
            on="merge_key",
            how="left",
        ).drop(columns=["merge_key"])
        # Fill missing values (e.g. "chart_ids" for steps that are not grapher steps).
        for column in ["chart_ids", "chart_slugs", "all_usages"] + [
            f"chart_{metric}" for metric in self.ANALYTICS_COLUMNS
        ]:
            steps_df[column] = [row if isinstance(row, list) else [] for row in steps_df[column]]
        for column in ["db_dataset_id", "db_dataset_name"]:
            steps_df.loc[steps_df[column].isnull(), column] = None
        for column in ["db_private", "db_archived"]:
            steps_df[column] = steps_df[column].fillna(False)
        # Create a dictionary mapping steps to chart ids,
        # e.g. {"step_1": [123, 456], "step_2": [789, 1011], ...}.
        step_to_chart_ids = steps_df[["step", "chart_ids"]].set_index("step").to_dict()["chart_ids"]
        # Create a dictionary mapping steps to chart slugs,
        # e.g. {"step_1": [(123, "slug_1"), (456, "slug_2")], "step_2": [], "step_3": ...}.
        step_to_chart_slugs = steps_df[["step", "chart_slugs"]].set_index("step").to_dict()["chart_slugs"]
        # Create a dictionary mapping steps to chart views,
        # e.g. {"step_1": [(123, 1000), (456, 2000)], "step_2": [], "step_3": ...}.
        step_to_chart_views = {}
        for metric in self.ANALYTICS_COLUMNS:
            step_to_chart_views[metric] = (
                steps_df[["step", f"chart_{metric}"]].set_index("step").to_dict()[f"chart_{metric}"]
            )
        # NOTE: Instead of this approach, an alternative would be to add grapher db datasets as steps of a different
        #   channel (e.g. "db").
        # Create a column with all chart ids of all dependencies of each step.
        # To achieve that, for each step, sum the list of chart ids from all its usages to the list of chart ids of
        # the step itself. Then create a sorted list of the set of all those chart ids.
        steps_df["all_chart_ids"] = [
            sorted(set(sum([step_to_chart_ids[usage] for usage in row["all_usages"]], step_to_chart_ids[row["step"]])))  # type: ignore
            for _, row in steps_df.iterrows()
        ]
        # Create a column with charts slugs, i.e. for each step, [(123, "some_chart"), (456, "some_other_chart"), ...].
        steps_df["all_chart_slugs"] = [
            sorted(
                set(sum([step_to_chart_slugs[usage] for usage in row["all_usages"]], step_to_chart_slugs[row["step"]]))  # type: ignore
            )
            for _, row in steps_df.iterrows()
        ]
        # Create a column with the number of charts affected (in any way possible) by each step.
        steps_df["n_charts"] = [len(charts_ids) for charts_ids in steps_df["all_chart_ids"]]
        # Add analytics for all charts.
        for metric in self.ANALYTICS_COLUMNS:
            # Create a column with the number of chart views, i.e. for each step, [(123, 1000), (456, 2000), ...].
            steps_df[f"all_chart_{metric}"] = [
                sorted(
                    set(
                        sum(
                            [step_to_chart_views[metric][usage] for usage in row["all_usages"]],  # type: ignore
                            step_to_chart_views[metric][row["step"]],
                        )
                    )
                )
                for _, row in steps_df.iterrows()
            ]
            # Create a column with the total number of views of all charts affected by each step.
            steps_df[f"n_chart_{metric}"] = [
                sum([chart_views[1] for chart_views in charts_views])
                for charts_views in steps_df[f"all_chart_{metric}"]
            ]

        # Remove all those rows that correspond to DB datasets which:
        # * Are archived.
        # * Have no charts.
        # * Have no ETL steps (they may have already been deleted).
        steps_df = steps_df.drop(
            steps_df[(steps_df["db_archived"]) & (steps_df["n_charts"] == 0) & (steps_df["kind"].isnull())].index
        ).reset_index(drop=True)

        return steps_df

    def _create_steps_df(self) -> pd.DataFrame:
        # Initialise steps_df with core columns.
        steps_df = self._init_steps_df_ndim()

        if self.connect_to_db:
            # Add info from DB.
            steps_df = self._add_info_from_db(steps_df=steps_df)

            # Add columns with the date and the number of days until the next update.
            steps_df = _add_days_to_update_columns(steps_df=steps_df)
        else:
            # Add empty columns.
            for column in (
                [
                    "chart_ids",
                    "chart_slugs",
                    "db_dataset_id",
                    "db_dataset_name",
                    "db_private",
                    "db_archived",
                    "update_period_days",
                    "date_of_next_update",
                    "days_to_update",
                    "all_chart_ids",
                    "all_chart_slugs",
                    "n_charts",
                ]
                + [f"chart_{metric}" for metric in self.ANALYTICS_COLUMNS]
                + [f"n_chart_{metric}" for metric in self.ANALYTICS_COLUMNS]
            ):
                steps_df[column] = None

        # Add columns with the list of forward and backwards versions for each step.
        steps_df = self._add_columns_with_different_step_versions(steps_df=steps_df)

        # For convenience, add full local path to dag files to steps dataframe.
        steps_df["dag_file_path"] = [
            (paths.DAG_DIR / dag_file_name).with_suffix(".yml") if dag_file_name else None
            for dag_file_name in steps_df["dag_file_name"].fillna("")
        ]
        # For convenience, add full local path to script files.
        steps_df["full_path_to_script"] = [
            paths.BASE_DIR / script_file_name if script_file_name else None
            for script_file_name in steps_df["path_to_script"].fillna("")
        ]

        # Add column that is true if the step is the latest version.
        steps_df["is_latest"] = False
        steps_df.loc[steps_df["version"] == steps_df["latest_version"], "is_latest"] = True

        # Add update state to steps_df.
        steps_df = self._add_steps_update_state(steps_df=steps_df)

        return steps_df

    def _init_steps_df(self) -> pd.DataFrame:
        # Create a dataframe where each row correspond to one step.
        steps_df = pd.DataFrame({"step": self.all_steps.copy()})
        # Add relevant information about each step.
        steps_df["direct_dependencies"] = [self.get_direct_step_dependencies(step=step) for step in self.all_steps]
        steps_df["direct_usages"] = [self.get_direct_step_usages(step=step) for step in self.all_steps]
        steps_df["all_active_dependencies"] = [
            self.get_all_step_dependencies(step=step, only_active=True) for step in self.all_steps
        ]
        steps_df["all_dependencies"] = [self.get_all_step_dependencies(step=step) for step in self.all_steps]
        steps_df["all_active_usages"] = [
            self.get_all_step_usages(step=step, only_active=True) for step in self.all_steps
        ]
        steps_df["all_usages"] = [self.get_all_step_usages(step=step) for step in self.all_steps]
        steps_df["state"] = ["active" if step in self.all_active_steps else "archive" for step in self.all_steps]
        steps_df["role"] = ["usage" if step in self.dag_all else "dependency" for step in self.all_steps]
        steps_df["dag_file_name"] = [self.get_dag_file_for_step(step=step) for step in self.all_steps]
        steps_df["path_to_script"] = [self.get_path_to_script(step=step, omit_base_dir=True) for step in self.all_steps]

        # Add column for the total number of all dependencies and usges.
        steps_df["n_all_dependencies"] = [len(dependencies) for dependencies in steps_df["all_dependencies"]]
        steps_df["n_all_usages"] = [len(usages) for usages in steps_df["all_usages"]]

        # Add attributes to steps.
        steps_df = pd.merge(steps_df, self.step_attributes_df, on="step", how="left")

        return steps_df

    def _init_steps_df_ndim(self) -> pd.DataFrame:
        """Optimised version of the _init_steps_df method (loop-opimisation)."""
        # Create a dataframe where each row correspond to one step.
        steps_df = pd.DataFrame({"step": self.all_steps.copy()})
        # Add relevant information about each step.
        steps_df["direct_dependencies"] = [self.get_direct_step_dependencies(step=step) for step in self.all_steps]
        steps_df["direct_usages"] = self.get_direct_step_uses_ndim()
        steps_df["all_active_dependencies"] = [
            self.get_all_step_dependencies(step=step, only_active=True) for step in self.all_steps
        ]
        steps_df["all_dependencies"] = [self.get_all_step_dependencies(step=step) for step in self.all_steps]
        steps_df["all_active_usages"] = self.get_all_step_usages_ndim(only_active=True)
        steps_df["all_usages"] = self.get_all_step_usages_ndim()
        steps_df["state"] = ["active" if step in self.all_active_steps else "archive" for step in self.all_steps]
        steps_df["role"] = ["usage" if step in self.dag_all else "dependency" for step in self.all_steps]
        steps_df["dag_file_name"] = [self.get_dag_file_for_step(step=step) for step in self.all_steps]
        steps_df["path_to_script"] = [self.get_path_to_script(step=step, omit_base_dir=True) for step in self.all_steps]

        # Add column for the total number of all dependencies and usges.
        steps_df["n_all_dependencies"] = [len(dependencies) for dependencies in steps_df["all_dependencies"]]
        steps_df["n_all_usages"] = [len(usages) for usages in steps_df["all_usages"]]

        # Add attributes to steps.
        steps_df = pd.merge(steps_df, self.step_attributes_df, on="step", how="left")

        return steps_df

    @property
    def step_attributes_df(self) -> pd.DataFrame:
        if self._step_attributes_df is None:
            self._step_attributes_df = self._create_step_attributes()

        return self._step_attributes_df

    @property
    def steps_df(self) -> pd.DataFrame:
        """Dataframe where each row corresponds to a unique step that appears in the (active or archive) dag.

        Returns
        -------
        steps_df : pd.DataFrame
            Steps dataframe.
        """
        if self._steps_df is None:
            self._steps_df = self._create_steps_df()

        return self._steps_df

    @staticmethod
    def _log_warnings_and_errors(message, list_affected, warning_or_error, additional_info=None):
        error_message = message
        if len(list_affected) > 0:
            for i, affected in enumerate(list_affected):
                error_message += f"\n    {affected}"
                if additional_info:
                    error_message += f" - {additional_info[i]}"
            if warning_or_error == "error":
                log.error(error_message)
            elif warning_or_error == "warning":
                log.warning(error_message)

    def _generate_error_for_missing_dependencies(self, missing_steps: Set[str]) -> str:
        error_message = "Missing dependencies in the dag:"
        for missing_step in missing_steps:
            error_message += f"\n* Missing step \n    {missing_step}\n  is a dependency of the following active steps:"
            direct_usages = self.get_direct_step_usages(step=missing_step)
            for usage in direct_usages:
                error_message += f"\n    {usage}"

        return error_message

    def check_that_active_dependencies_are_defined(self) -> None:
        """Check that all active dependencies are defined in the dag; if not, raise an informative error."""
        # Gather all steps that appear in the dag only as dependencies, but not as executable steps.
        missing_steps = set(self.all_active_dependencies) - set(self.all_active_usages)

        # Remove those special steps that are expected to not appear in the dag as executable steps (e.g. snapshots).
        channels_to_ignore = ("snapshot", "etag", "github", "walden")
        missing_steps = set([step for step in missing_steps if not step.startswith(channels_to_ignore)])

        if len(missing_steps) > 0:
            error_message = self._generate_error_for_missing_dependencies(missing_steps=missing_steps)
            log.error(f"{error_message}\n\nSolution: Check if you may have accidentally deleted those missing steps.")

    def check_that_active_dependencies_are_not_archived(self) -> None:
        """Check that all active dependencies are not archived; if they are, raise an informative error."""
        # Find any archive steps that are dependencies of active steps, and should therefore not be archive steps.
        missing_steps = set(self.dag_archive) & set(self.all_active_dependencies)

        if len(missing_steps) > 0:
            error_message = self._generate_error_for_missing_dependencies(missing_steps=missing_steps)
            log.error(f"{error_message}\n\nSolution: Either archive the active steps or un-archive the archive steps.")

    def check_that_all_active_steps_are_necessary(self) -> None:
        """Check that all active steps are needed in the dag; if not, raise an informative warning."""
        if self.warn_on_archivable:
            # Find all active steps that can safely be archived.
            archivable_steps = self.steps_df[self.steps_df["update_state"] == UpdateState.ARCHIVABLE.value][
                "step"
            ].tolist()
            self._log_warnings_and_errors(
                message="Some active steps can safely be archived:",
                list_affected=archivable_steps,
                warning_or_error="warning",
            )
        if self.warn_on_unused:
            # Find all active steps that are not yet used (and should either be used or archived).
            unused_steps = self.steps_df[self.steps_df["update_state"] == UpdateState.UNUSED.value]["step"].tolist()
            self._log_warnings_and_errors(
                message="Some active steps are not yet used, and could potentially be archived:",
                list_affected=unused_steps,
                warning_or_error="warning",
            )

    def check_that_all_steps_have_a_script(self) -> None:
        """Check that all steps have code.

        If the step is active and there is no code, raise an error.
        If the step is archived and there is no code, raise a warning.
        For snapshots, the code can either be the script to generate the snapshot, or a metadata dvc file.
        """
        [self.get_path_to_script(step) for step in self.all_steps]

    def check_that_db_datasets_with_charts_are_not_archived(self) -> None:
        """Check that DB datasets with public charts are not archived (though they may be private).

        This check can only be performed if we have access to the DB.
        """
        archived_db_datasets = sorted(
            set(self.steps_df[(self.steps_df["n_charts"] > 0) & (self.steps_df["db_archived"])]["step"])
        )
        self._log_warnings_and_errors(
            message="Some DB datasets are archived even though they have public charts. They should be public:",
            list_affected=archived_db_datasets,
            warning_or_error="error",
        )

    def check_that_db_datasets_with_charts_have_active_etl_steps(self) -> None:
        """Check that DB datasets with public charts have active ETL steps.

        This check can only be performed if we have access to the DB.
        """
        missing_etl_steps = sorted(
            set(
                self.steps_df[
                    (self.steps_df["db_dataset_id"].notnull())
                    & (self.steps_df["n_charts"] > 0)
                    & (self.steps_df["state"].isin([np.nan, "archive"]))
                ]["step"]
            )
        )
        self._log_warnings_and_errors(
            message="Some DB datasets with public charts have no active ETL steps:",
            list_affected=missing_etl_steps,
            warning_or_error="error",
        )

    def check_that_db_datasets_exist_in_etl(self) -> None:
        """Check that all active DB datasets (with or without charts) exist in the ETL dag (active or archive).

        This check can only be performed if we have access to the DB.
        """
        if self.connect_to_db:
            # Ensure steps_df is calculated.
            _ = self.steps_df

        if self.unknown_steps_with_grapher_dataset_df is not None:
            missing_df = self.unknown_steps_with_grapher_dataset_df.sort_values("db_dataset_name").reset_index(
                drop=True
            )
            dataset_names = missing_df["db_dataset_name"].tolist()
            dataset_urls = [
                f"{GRAPHER_DATASET_BASE_URL}{int(dataset_id)}" for dataset_id in missing_df["db_dataset_id"]
            ]
            self._log_warnings_and_errors(
                message="Some DB datasets do not exist in the ETL:",
                list_affected=dataset_names,
                warning_or_error="warning",
                additional_info=dataset_urls,
            )

    def check_that_all_steps_have_update_state(self) -> None:
        """Check that all steps have an update state."""
        missing_update_state = self.steps_df[
            (self.steps_df["update_state"].isnull()) | (self.steps_df["update_state"] == UpdateState.UNKNOWN)
        ]["step"].tolist()
        self._log_warnings_and_errors(
            message="Some steps have no update state:",
            list_affected=missing_update_state,
            warning_or_error="error",
        )

    def apply_sanity_checks(self) -> None:
        """Apply all sanity checks."""
        self.check_that_active_dependencies_are_defined()
        self.check_that_active_dependencies_are_not_archived()
        self.check_that_all_steps_have_a_script()
        self.check_that_all_steps_have_update_state()
        if self.connect_to_db:
            self.check_that_db_datasets_with_charts_are_not_archived()
            self.check_that_db_datasets_with_charts_have_active_etl_steps()
            self.check_that_db_datasets_exist_in_etl()
        # The following check will warn of archivable steps (if warn_on_archivable is True).
        # Depending on whether connect_to_db is True or False, the criterion will be different.
        # When False, the criterion is rather a proxy; True uses a more meaningful criterion.
        self.check_that_all_active_steps_are_necessary()


@click.command(name="version-tracker", cls=RichCommand)
@click.option(
    "--skip-db",
    is_flag=True,
    default=False,
    help="True to skip connecting to the database of the current environment. False to try to connect to DB, to get a better informed picture of what steps may be missing or archivable. If not connected, all checks will be based purely on the content of the ETL dag.",
)
@click.option(
    "--warn-on-archivable",
    is_flag=True,
    default=False,
    help="True to warn about archivable steps. By default this is False, because we currently have many archivable steps.",
)
@click.option(
    "--warn-on-unused",
    is_flag=True,
    default=False,
    help="True to warn about unused steps (i.e. steps that may be up-to-date, but not yet used anywhere, and hence can potentially be archived). By default this is False, because we currently have many unused steps.",
)
def run_version_tracker_checks(
    skip_db: bool = False, warn_on_archivable: bool = False, warn_on_unused: bool = False
) -> None:
    """Check that all DAG dependencies (e.g. make sure no step is missing).

    Run all version tracker sanity checks.
    """
    VersionTracker(
        connect_to_db=not skip_db, warn_on_archivable=warn_on_archivable, warn_on_unused=warn_on_unused
    ).apply_sanity_checks()


def _add_days_to_update_columns(steps_df):
    """Add columns to steps dataframe with the date of next update and the number of days until the next update.

    We currently don't have a clear way to calculate the expected date of update of a dataset.
    For now, we simply add update_period_days to the step's version.
    But a dataset may have had a minor update since the main release (while the origins are still the same).

    Alternatively, we could add update_period_days to the date_published of its snapshots.
    However, if there are multiple snapshots, it is not clear which one to use as a reference.
    For example, if a dataset uses the income groups dataset, that would have a date_published that is irrelevant.
    We would need some way to decide which snapshot(s) is (are) the main one(s).

    For now, one option is that, on a minor update, we manually edit update_period_days.
    For example, if a dataset is expected to be updated every 365 days, but had a minor update after 1 month, we could
    modify update_period_days to be 335.

    Parameters
    ----------
    steps_df : pd.DataFrame
        Steps dataframe.

    Returns
    -------
    df : pd.DataFrame
        Steps dataframe with the new columns "date_of_next_update" and "days_to_update".
    """
    df = steps_df.copy()

    # Extract version from steps data frame, and make it a string.
    version = df["version"].copy().astype(str)
    # Assume first of January to those versions that are only given as years.
    filter_years = (version.str.len() == 4) & (version.str.isdigit())
    version[filter_years] = version[filter_years] + "-01-01"
    # Convert version to datetime where possible, setting "latest" to NaT.
    version_date = pd.to_datetime(version, errors="coerce", format="%Y-%m-%d")

    # Extract update_period_days from steps dataframe, ensuring it is numeric, or NaT where it was None.
    update_period_days = pd.to_numeric(df["update_period_days"], errors="coerce")

    # Create a column with the date of next update where possible.
    df["date_of_next_update"] = None
    filter_dates = (version_date.notnull()) & (update_period_days > 0)
    df.loc[filter_dates, "date_of_next_update"] = (
        version_date[filter_dates] + pd.to_timedelta(update_period_days[filter_dates], unit="D")
    ).dt.strftime("%Y-%m-%d")

    # Create a column with the number of days until the next update.
    df["days_to_update"] = None
    df.loc[filter_dates, "days_to_update"] = (
        pd.to_datetime(df.loc[filter_dates, "date_of_next_update"]) - TODAY
    ).dt.days

    return df
