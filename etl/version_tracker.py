import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import click
import numpy as np
import pandas as pd
import structlog
import yaml
from rich_click.rich_command import RichCommand

from etl import paths
from etl.db import can_connect, get_info_for_etl_datasets
from etl.steps import extract_step_attributes, load_dag, reverse_graph

log = structlog.get_logger()

# Define the temporary step that will depend on newly created snapshots before they are used by any active steps.
DAG_TEMP_STEP = "data-private://meadow/temp/latest/step"


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
        if (set(steps_df[steps_df["step"] == step]["all_active_usages"].item()) - unused_steps) == set()
    }

    # Add them to the set of unused steps.
    unused_steps = unused_steps | new_unused_steps

    if new_unused_steps == set():
        # If no new unused step has been detected, return the set of unused steps.
        return unused_steps
    else:
        # Otherwise, repeat the process to keep finding new archivable steps.
        return _recursive_get_all_archivable_steps(steps_df=steps_df, unused_steps=unused_steps)


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


class VersionTracker:
    """Helper object that loads the dag, provides useful functions to check for versions and dataset dependencies, and
    checks for inconsistencies.

    """

    # List of steps known to be archivable, for which we don't want to see warnings.
    # We keep them in the active dag for technical reasons.
    KNOWN_ARCHIVABLE_STEPS = [
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
    ]

    def __init__(self, connect_to_db: bool = True, warn_on_archivable: bool = True):
        # Load dag of active and archive steps (a dictionary where each item is step: set of dependencies).
        self.dag_all = load_dag(paths.DAG_ARCHIVE_FILE)
        # Create a reverse dag (a dictionary where each item is step: set of usages).
        self.dag_all_reverse = reverse_graph(graph=self.dag_all)
        # Load dag of active steps.
        self.dag_active = load_dag(paths.DAG_FILE)
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

    def get_all_archivable_steps(self) -> List[str]:
        """Get all steps in the dag that can safely be moved to the archive.

        NOTE: This function currently doesn't take into account whether a dataset is used in an explorer, or an external
        repos (like energy-data, co2-data, poverty-data, or covid-19-data). Currently, there is no easy way to check for
        those usages.

        If we have access to DB, a step is archivable is:
        * It is active.
        * It has no charts (including all indirect charts).
        * It has a date version and it is older than a certain number of days (n_days_before_archiving).
        * It is not listed in the KNOWN_ARCHIVABLE_STEPS.

        Otherwise, without access to DB, a step is archivable if:
        * It is active.
        * There is a newer version of the same step in the active dag.
        * It is either in channel meadow or garden.
        * It is not an active dependency.
        * It is not listed in the KNOWN_ARCHIVABLE_STEPS.

        """
        if self.connect_to_db:
            # Number of days that an ETL active step is allowed to exist without charts, before we warn about archiving it.
            n_days_before_archiving = 7
            # Calculate the earliest date that a step can have without having charts, before we warn about archiving it.
            earliest_date = (datetime.now() - timedelta(days=n_days_before_archiving)).strftime("%Y-%m-%d")
            archivable_steps = sorted(
                set(self.steps_df[(self.steps_df["state"] == "active") & (self.steps_df["n_charts"] == 0)]["step"])
            )
            # Remove steps that are very recent (since maybe charts have not yet been created).
            archivable_steps = [
                step
                for step in archivable_steps
                if ((re.findall(r"\d{4}-\d{2}-\d{2}", step) or ["9999"])[0] < earliest_date)
            ]
        else:
            archivable_steps = sorted(_recursive_get_all_archivable_steps(steps_df=self.steps_df))

        # Remove steps that are already known to be archivable.
        archivable_steps = [step for step in archivable_steps if step not in self.KNOWN_ARCHIVABLE_STEPS]

        return archivable_steps

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
        _, _, channel, namespace, version, name, _ = extract_step_attributes(step=step).values()
        state = "active" if step in self.all_active_steps else "archive"

        # Create a dictionary that contains the path to a script for a given step.
        # This dictionary has to keys, namely "active" and "archive".
        # Active steps should have a script in the active directory.
        # But steps that are in the archive dag can be either in the active or the archive directory.
        path_to_script = {"active": None, "archive": None}
        if channel == "snapshot":
            path_to_script["active"] = paths.SNAPSHOTS_DIR / namespace / version / name  # type: ignore
            path_to_script["archive"] = paths.SNAPSHOTS_DIR_ARCHIVE / namespace / version / name  # type: ignore
        elif channel in ["meadow", "garden", "grapher", "explorers", "open_numbers", "examples"]:
            path_to_script["active"] = paths.STEP_DIR / "data" / channel / namespace / version / name  # type: ignore
            path_to_script["archive"] = paths.STEP_DIR_ARCHIVE / channel / namespace / version / name  # type: ignore
        elif channel == "walden":
            path_to_script["active"] = paths.BASE_DIR / "lib" / "walden" / "ingests" / namespace / version / name  # type: ignore
            path_to_script["archive"] = paths.BASE_DIR / "lib" / "walden" / "ingests" / namespace / version / name  # type: ignore
        elif channel in ["backport", "etag"]:
            # Ignore these channels, for which there is never a script.
            return None
        else:
            log.error(f"Unknown channel {channel} for step {step}.")

        if state == "active":
            # Steps in the active dag should only have a script in the active directory.
            del path_to_script["archive"]

        path_to_script_detected = None
        for state in path_to_script:
            # A step script can exist either as a .py file, as a .ipynb file, or a __init__.py file inside a folder.
            # In the case of snapshots, there may or may not be a .py file, but there definitely needs to be a dvc file.
            # In that case, the corresponding script is not trivial to find, but at least we can return the dvc file.
            for path_to_script_candidate in [
                path_to_script[state].with_suffix(".py"),  # type: ignore
                path_to_script[state].with_suffix(".ipynb"),  # type: ignore
                path_to_script[state] / "__init__.py",  # type: ignore
                path_to_script[state].with_name(path_to_script[state].name + ".dvc"),  # type: ignore
            ]:
                if path_to_script_candidate.exists():
                    path_to_script_detected = path_to_script_candidate
                    break
        if path_to_script_detected is None:
            if state == "active":
                log.error(f"Script for step {step} not found.")
            else:
                log.warning(f"Script for archive step {step} not found.")

        if omit_base_dir and path_to_script_detected is not None:
            # Return the path relative to the base directory (omitting the local path to the ETL repos).
            path_to_script_detected = path_to_script_detected.relative_to(paths.BASE_DIR)

        return path_to_script_detected

    def _create_step_attributes(self) -> pd.DataFrame:
        # Extract all attributes of each unique active/archive/dependency step.
        step_attributes = pd.DataFrame(
            [extract_step_attributes(step).values() for step in self.all_steps],
            columns=["step", "kind", "channel", "namespace", "version", "name", "identifier"],
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

    @staticmethod
    def _add_columns_with_different_step_versions(steps_df: pd.DataFrame) -> pd.DataFrame:
        steps_df = steps_df.copy()
        # Create a dataframe with one row per unique step.
        df = steps_df.drop_duplicates(subset="step")[["step", "identifier", "version"]].reset_index(drop=True)
        # For each step, find all alternative versions.
        # New columns will contain forward versions, backward versions, all versions, and latest version.
        other_versions_forward = []
        other_versions_backward = []
        other_versions_all = []
        latest_version = []
        for _, row in df.iterrows():
            # Create a mask that selects all steps with the same identifier.
            select_same_identifier = df["identifier"] == row["identifier"]
            # Find all forward versions of the current step.
            versions_forward = sorted(set(df[select_same_identifier & (df["version"] > row["version"])]["step"]))
            other_versions_forward.append(versions_forward)
            # Find all backward versions of the current step.
            other_versions_backward.append(
                sorted(set(df[select_same_identifier & (df["version"] < row["version"])]["step"]))
            )
            # Find all versions of the current step.
            other_versions_all.append(sorted(set(df[select_same_identifier]["step"])))
            # Find latest version of the current step.
            latest_version.append(versions_forward[-1] if len(versions_forward) > 0 else row["step"])
        # Add columns to the dataframe.
        df["same_steps_forward"] = other_versions_forward
        df["same_steps_backward"] = other_versions_backward
        df["same_steps_all"] = other_versions_all
        df["same_steps_latest"] = latest_version
        # Add new columns to the original steps dataframe.
        steps_df = pd.merge(steps_df, df.drop(columns=["identifier", "version"]), on="step", how="left")

        return steps_df

    @staticmethod
    def _add_info_from_db(steps_df: pd.DataFrame) -> pd.DataFrame:
        steps_df = steps_df.copy()
        # Fetch all info about datasets from the DB.
        info_df = get_info_for_etl_datasets().rename(
            columns={
                "dataset_id": "db_dataset_id",
                "dataset_name": "db_dataset_name",
                "is_private": "db_private",
                "is_archived": "db_archived",
            },
            errors="raise",
        )
        steps_df = pd.merge(
            steps_df,
            info_df[["step", "chart_ids", "db_dataset_id", "db_dataset_name", "db_private", "db_archived"]],
            on="step",
            how="outer",
        )
        # Fill missing values.
        for column in ["chart_ids", "all_usages"]:
            steps_df[column] = [row if isinstance(row, list) else [] for row in steps_df[column]]
        for column in ["db_dataset_id", "db_dataset_name"]:
            steps_df.loc[steps_df[column].isnull(), column] = None
        for column in ["db_private", "db_archived"]:
            steps_df[column] = steps_df[column].fillna(False)
        # Create a dictionary step: chart_ids.
        step_chart_ids = steps_df[["step", "chart_ids"]].set_index("step").to_dict()["chart_ids"]
        # NOTE: Instead of this approach, an alternative would be to add grapher db datasets as steps of a different
        #   channel (e.g. "db").
        # Create a column with all chart ids of all dependencies of each step.
        # To achieve that, for each step, sum the list of chart ids from all its usages to the list of chart ids of
        # the step itself. Then create a sorted list of the set of all those chart ids.
        steps_df["all_chart_ids"] = [
            sorted(set(sum([step_chart_ids[usage] for usage in row["all_usages"]], step_chart_ids[row["step"]])))  # type: ignore
            for _, row in steps_df.iterrows()
        ]
        # Create a column with the number of charts affected (in any way possible) by each step.
        steps_df["n_charts"] = [len(chart_ids) for chart_ids in steps_df["all_chart_ids"]]

        # Remove all those rows that correspond to DB datasets which:
        # * Are archived.
        # * Have no charts.
        # * Have no ETL steps (they may have already been deleted).
        steps_df = steps_df.drop(
            steps_df[(steps_df["db_archived"]) & (steps_df["n_charts"] == 0) & (steps_df["kind"].isnull())].index
        ).reset_index(drop=True)

        return steps_df

    def _create_steps_df(self) -> pd.DataFrame:
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

        # Add attributes to steps.
        steps_df = pd.merge(steps_df, self.step_attributes_df, on="step", how="left")

        if self.connect_to_db:
            # Add info from DB.
            steps_df = self._add_info_from_db(steps_df=steps_df)

        # Add columns with the list of forward and backwards versions for each step.
        steps_df = self._add_columns_with_different_step_versions(steps_df=steps_df)

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
    def _log_warnings_and_errors(message, list_affected, warning_or_error):
        error_message = message
        if len(list_affected) > 0:
            for affected in list_affected:
                error_message += f"\n    {affected}"
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
        channels_to_ignore = ("snapshot", "backport", "etag", "github", "walden")
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
            unused_data_steps = self.get_all_archivable_steps()
            self._log_warnings_and_errors(
                message="Some active steps are not used and can safely be archived:",
                list_affected=unused_data_steps,
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
        """Check that DB datasets with public charts are not archived (though they may be private)."""
        archived_db_datasets = sorted(
            set(self.steps_df[(self.steps_df["n_charts"] > 0) & (self.steps_df["db_archived"])]["step"])
        )
        self._log_warnings_and_errors(
            message="Some DB datasets are archived even though they have public charts. They should be public:",
            list_affected=archived_db_datasets,
            warning_or_error="error",
        )

    def check_that_db_datasets_with_charts_have_active_etl_steps(self) -> None:
        """Check that DB datasets with public charts have active ETL steps."""
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

    def apply_sanity_checks(self) -> None:
        """Apply all sanity checks."""
        self.check_that_active_dependencies_are_defined()
        self.check_that_active_dependencies_are_not_archived()
        self.check_that_all_steps_have_a_script()
        if self.connect_to_db:
            self.check_that_db_datasets_with_charts_are_not_archived()
            self.check_that_db_datasets_with_charts_have_active_etl_steps()
        # The following check will warn of archivable steps (if warn_on_archivable is True).
        # Depending on whether connect_to_db is True or False, the criterion will be different.
        # When False, the criterion is rather a proxy; True uses a more meaningful criterion.
        self.check_that_all_active_steps_are_necessary()

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
def run_version_tracker_checks(skip_db: bool = False, warn_on_archivable: bool = False) -> None:
    """Check that all DAG dependencies (e.g. make sure no step is missing).

    Run all version tracker sanity checks.
    """
    VersionTracker(connect_to_db=not skip_db, warn_on_archivable=warn_on_archivable).apply_sanity_checks()
