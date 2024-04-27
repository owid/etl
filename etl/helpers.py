#
#  helpers.py
#  etl
#

import re
import sys
import tempfile
from collections.abc import Generator
from contextlib import contextmanager
from functools import cache
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Literal, Optional, Union, cast
from urllib.parse import urljoin

import jsonref
import structlog
import yaml
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
from owid.datautils.common import ExceptionFromDocstring
from owid.walden import Catalog as WaldenCatalog
from owid.walden import Dataset as WaldenDataset

from etl import paths
from etl.snapshot import Snapshot, SnapshotMeta
from etl.steps import load_dag

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


def grapher_checks(ds: catalog.Dataset, warn_title_public: bool = True) -> None:
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

            _validate_description_key(tab[col].m.description_key, col)
            _validate_ordinal_variables(tab, col)

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


def _validate_description_key(description_key: list[str], col: str) -> None:
    if description_key:
        assert not all(
            len(x) == 1 for x in description_key
        ), f"Column `{col}` uses string {description_key} as description_key, should be list of strings."


def _validate_ordinal_variables(tab: Table, col: str) -> None:
    if tab[col].m.sort:
        extra_values = set(tab[col]) - set(tab[col].m.sort)
        assert (
            not extra_values
        ), f"Ordinal variable `{col}` has extra values that are not defined in field `sort`: {extra_values}"


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
    errors: Literal["ignore", "warn", "raise"] = "raise",
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
        ds.update_metadata(meta_path, if_origins_exist=if_origins_exist, errors=errors)

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


def get_comments_above_step_in_dag(step: str, dag_file: Path) -> str:
    """Get the comment lines right above a step in the dag file."""

    # Read the content of the dag file.
    with open(dag_file, "r") as _dag_file:
        lines = _dag_file.readlines()

    # Initialize a list to store the header lines.
    header_lines = []
    for line in lines:
        if line.strip().startswith("-") or (
            line.strip().endswith(":") and (not line.strip().startswith("#")) and (step not in line)
        ):
            # Restart the header if the current line:
            # * Is a dependency.
            # * Is a step that is not the current step.
            # * Is a special line like "steps:" or "include:".
            header_lines = []
        elif step in line and line.strip().endswith(":"):
            # If the current line is the step, stop reading the rest of the file.
            return "\n".join([line.strip() for line in header_lines]) + "\n" if len(header_lines) > 0 else ""
        elif line.strip() == "":
            # If the current line is empty, ignore it.
            continue
        else:
            # Any line that is not a dependency,
            header_lines.append(line)

    # If the step has not been found, raise an error and return nothing.
    log.error(f"Step {step} not found in dag file {dag_file}.")

    return ""


def write_to_dag_file(
    dag_file: Path,
    dag_part: Dict[str, Any],
    comments: Optional[Dict[str, str]] = None,
    indent_step=2,
    indent_dependency=4,
):
    """Update the content of a dag file, respecting the comments above the steps.

    NOTE: A simpler implementation of function may be possible using ruamel. However, I couldn't find out how to respect
    comments that are above steps.

    Parameters
    ----------
    dag_file : Path
        Path to dag file.
    dag_part : Dict[str, Any]
        Partial dag, containing the steps that need to be updated.
        This partial dag is a dictionary with steps as keys and the set of dependencies as values.
    comments : Optional[Dict[str, str]], optional
        Comments to add above the steps in the partial dag. The keys are the steps, and the values are the comments.
    indent_step : int, optional
        Number of spaces to use as indentation for steps in the dag.
    indent_dependency : int, optional
        Number of spaces to use as indentation for dependencies in the dag.

    """

    # If comments is not defined, assume an empty dictionary.
    if comments is None:
        comments = {}

    for step in comments:
        if len(comments[step]) > 0 and comments[step][-1] != "\n":
            # Ensure all comments end in a line break, otherwise add it.
            comments[step] = comments[step] + "\n"

    # Read the lines in the original dag file.
    with open(dag_file, "r") as file:
        lines = file.readlines()

    # Separate that content into the "steps" section (always given) and the "include" section (sometimes given).
    section_steps = []
    section_include = []
    inside_section_steps = True
    for line in lines:
        if line.strip().startswith("include"):
            inside_section_steps = False
        if inside_section_steps:
            section_steps.append(line)
        else:
            section_include.append(line)

    # Now the "steps" section will be updated, and at the end the "include" section will be appended.

    # Initialize a list with the new lines that will be written to the dag file.
    updated_lines = []
    # Initialize a list of comments preceding the next step after a given step.
    comments_next_step = []
    # Initialize a flag to skip lines until the next step.
    skip_until_next_step = False
    # Initialize a set to keep track of the steps that were found in the original dag file.
    steps_found = set()
    for line in section_steps:
        # Remove leading and trailing whitespace from the line.
        stripped_line = line.strip()

        # Identify the start of a step, e.g. "  data://meadow/temp/latest/step:".
        if stripped_line.endswith(":") and not stripped_line.startswith("-") and not stripped_line.startswith("steps:"):
            if comments_next_step:
                updated_lines.extend(comments_next_step)
                comments_next_step = []
            # Extract the name of the step (without the ":" at the end).
            current_step = ":".join(stripped_line.split(":")[:-1])
            if current_step in dag_part:
                # This step was in dag_part, which means it needs to be updated.
                # First add the step itself.
                updated_lines.append(line)
                # Now add each of its dependencies.
                for dep in dag_part[current_step]:
                    updated_lines.append(" " * indent_dependency + f"- {dep}\n")
                # Skip the following lines until the next step is found.
                skip_until_next_step = True
                # Start tracking possible comments of the next step.
                comments_next_step = []
                # Add the current step to the set of steps found in the dag file.
                steps_found.add(current_step)
                continue
            else:
                # This step was not in dag_part, so it will be copied as is.
                skip_until_next_step = False

        # Skip dependencies and comments among dependencies of the step being updated.
        if skip_until_next_step:
            if stripped_line.startswith("-"):
                # Remove comments among dependencies.
                comments_next_step = []
                continue
            elif stripped_line.startswith("#"):
                # Add comments that may potentially be related to the next step.
                comments_next_step.append(line)
                continue

        # Add lines that should not be skipped.
        updated_lines.append(line)

    # Append new steps that weren't found in the original content.
    for step, dependencies in dag_part.items():
        if step not in steps_found:
            # Add the comment for this step, if any was given.
            if step in comments:
                updated_lines.append(
                    " " * indent_step + ("\n" + " " * indent_step).join(comments[step].split("\n")[:-1]) + "\n"
                    if len(comments[step]) > 0
                    else ""
                )
            # Add the step itself.
            updated_lines.append(" " * indent_step + f"{step}:\n")
            # Add each of its dependencies.
            for dep in dependencies:
                updated_lines.append(" " * indent_dependency + f"- {dep}\n")

    if len(section_include) > 0:
        # Append the include section, ensuring there is only one line break in between.
        for i in range(len(updated_lines) - 1, -1, -1):
            if updated_lines[i] != "\n":
                # Slice the list to remove trailing line breaks
                updated_lines = updated_lines[: i + 1]
                break
        # Add a single line break before the include section, and then add the include section.
        updated_lines.extend(["\n"] + section_include)

    # Write the updated content back to the dag file.
    with open(dag_file, "w") as file:
        file.writelines(updated_lines)


def _remove_step_from_dag_file(dag_file: Path, step: str) -> None:
    with open(dag_file, "r") as file:
        lines = file.readlines()

    new_lines = []
    _number_of_comment_lines = 0
    _step_detected = False
    _continue_until_the_end = False
    num_spaces_indent = 0
    for line in lines:
        if line.startswith(("steps:", "include:")):
            # These are special lines, store them and move on.
            new_lines.append(line)
            continue

        if _continue_until_the_end:
            new_lines.append(line)
            continue

        if not _step_detected:
            if line.strip().startswith("#") or line.strip() == "":
                _number_of_comment_lines += 1
                new_lines.append(line)
                continue
            elif line.strip().startswith(step):
                # Remove the previous comment lines and ignore the current line.
                new_lines = new_lines[:-_number_of_comment_lines]
                # Find the number of spaces on the left of the step name.
                # We need this to know if the next comments are indented (as comments within dependencies).
                num_spaces_indent = len(line) - len(line.lstrip())
                _step_detected = True
                continue
            else:
                # This line corresponds to any other step or step dependency.
                new_lines.append(line)
                _number_of_comment_lines = 0
                continue
        else:
            if line.strip().startswith("- "):
                # Ignore the dependencies of the step.
                continue
            elif (line.strip().startswith("#")) and (len(line) - len(line.lstrip()) > num_spaces_indent):
                # Ignore comments that are indented (as comments within dependencies).
                continue
            elif line.strip() == "":
                # Ignore empty lines.
                continue
            else:
                # The step dependencies have ended. Append current line and continue until the end of the dag file.
                new_lines.append(line)
                _continue_until_the_end = True
                continue

    # Write the new content to the active dag file.
    with open(dag_file, "w") as file:
        file.writelines(new_lines)


def remove_steps_from_dag_file(dag_file: Path, steps_to_remove: List[str]) -> None:
    """Remove specific steps from a dag file, including their comments.

    Parameters
    ----------
    dag_file : Path
        Path to dag file.
    steps_to_remove : List[str]
        List of steps to be removed from the DAG file.
        Their dependencies do not need to be specified (they will also be removed).

    """
    for step in steps_to_remove:
        _remove_step_from_dag_file(dag_file=dag_file, step=step)
