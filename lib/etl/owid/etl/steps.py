#
#  steps.py
#  owid.etl
#
#  Step definitions and execution for ETL pipelines.
#  This module contains all step types used in OWID's ETL system.
#
import hashlib
import importlib.util
import inspect
import json
import os
import re
import subprocess
import sys
import tempfile
import warnings
from collections import defaultdict
from collections.abc import Generator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass
from functools import partial
from glob import glob
from importlib import import_module
from pathlib import Path
from types import ModuleType
from typing import Any, Dict, List, Optional, Protocol, cast
from urllib.parse import urlparse

import fasteners
import pandas as pd
import requests
import structlog
from owid import catalog
from owid.catalog import s3_utils
from owid.catalog.catalogs import OWID_CATALOG_URI
from owid.catalog.datasets import DEFAULT_FORMATS
from etl import config, files, paths
from etl.config import TLS_VERIFY
from etl.helpers import get_metadata_path
from etl.snapshot import Snapshot

from .dag import Graph, to_dependency_order

log = structlog.get_logger()


# Registry for step classes - allows external code to register custom step types
STEP_REGISTRY: Dict[str, type] = {}


def register_step(step_type: str):
    """Decorator to register a step class for a given step type.

    Example:
        @register_step("data")
        class DataStep:
            ...
    """

    def decorator(cls: type) -> type:
        STEP_REGISTRY[step_type] = cls
        return cls

    return decorator


# Default modules to keep when using isolated_env
DEFAULT_KEEP_MODULES = (
    r"openpyxl|pyarrow|lxml|PIL|pydantic|sqlalchemy|sqlmodel|pandas|"
    r"frictionless|numpy|pyproj|geopandas|google|plotly|shapely"
)

ipynb_lock = fasteners.InterProcessLock(paths.BASE_DIR / ".ipynb_lock")

# Dictionary to store metadata changes for each dataset if INSTANT flag is set
INSTANT_METADATA_DIFF: Dict[str, Dict[str, List[str]]] = {}


@contextmanager
def isolated_env(
    working_dir: Path,
    keep_modules: str = DEFAULT_KEEP_MODULES,
) -> Generator[None, None, None]:
    """Add given directory to pythonpath, run code in context, and
    then remove from pythonpath and unimport modules imported in context.

    This is useful for running ETL steps in isolation, ensuring that
    modules imported by one step don't bleed into another.

    Note that unimporting modules means they'll have to be imported again, but
    it has minimal impact on performance (ms).

    Args:
        working_dir: Directory to add to sys.path.
        keep_modules: Regex of modules to keep imported after context exits.

    Example:
        with isolated_env(Path("./steps/data/garden/example")):
            import my_step_module
            my_step_module.run()
    """
    # Remember original sys.path and modules
    original_path = sys.path.copy()
    imported_modules = set(sys.modules.keys())

    # Create a temporary module registry for this context
    context_modules: dict[str, ModuleType] = {}

    try:
        # Insert working_dir at the beginning to give it highest priority
        # This ensures this process's modules take precedence
        sys.path.insert(0, working_dir.as_posix())

        # Monkey-patch __import__ to handle relative imports more safely
        original_import = __builtins__["__import__"]  # type: ignore[index]

        def safe_import(name: str, globals: Any = None, locals: Any = None, fromlist: Any = (), level: int = 0) -> Any:
            # For relative imports or modules that might exist in working_dir,
            # check working_dir first
            if level == 0 and globals and hasattr(globals.get("__spec__"), "submodule_search_locations"):
                # Try to load from working_dir first for potential conflicts
                module_path = working_dir / f"{name}.py"
                if module_path.exists() and name not in context_modules:
                    spec = importlib.util.spec_from_file_location(name, module_path)
                    if spec and spec.loader:
                        module = importlib.util.module_from_spec(spec)
                        context_modules[name] = module
                        sys.modules[name] = module
                        spec.loader.exec_module(module)
                        return module

            return original_import(name, globals, locals, fromlist, level)

        __builtins__["__import__"] = safe_import  # type: ignore[index]

        yield

    finally:
        # Restore original import function
        __builtins__["__import__"] = original_import  # type: ignore[index]

        # Restore original sys.path
        sys.path[:] = original_path

        # Unimport modules imported during execution unless they match `keep_modules`
        for module_name in set(sys.modules.keys()) - imported_modules:
            if not re.search(keep_modules, module_name):
                sys.modules.pop(module_name, None)

        # Clean up context modules
        for module_name in context_modules:
            sys.modules.pop(module_name, None)


def run_module_run(module: Any, dest_dir: str) -> None:
    """Ensure module has run() and execute it with dest_dir if applicable.

    Args:
        module: The module to run (must have a run() function).
        dest_dir: Destination directory to pass to run() if it accepts it.

    Raises:
        ValueError: If the module doesn't have a run() function.
    """
    if not hasattr(module, "run"):
        raise ValueError(f'No run() method defined for module "{module}"')
    sig = inspect.signature(module.run)
    if "dest_dir" in sig.parameters:
        module.run(dest_dir)
    else:
        module.run()


def checksum_file(path: str) -> str:
    """Calculate MD5 checksum of a file."""
    with open(path, "rb") as f:
        return hashlib.md5(f.read()).hexdigest()


def walk_files(directory: Path) -> List[Path]:
    """Walk a directory and return all files."""
    files_list = []
    for item in directory.rglob("*"):
        if item.is_file():
            files_list.append(item)
    return files_list


class Step(Protocol):
    path: str
    is_public: bool
    version: str
    dependencies: List["Step"]

    def run(self) -> None: ...

    def is_dirty(self) -> bool: ...

    def checksum_output(self) -> str: ...

    def can_execute(self, archive_ok: bool = True) -> bool: ...

    def __str__(self) -> str:
        raise NotImplementedError()

    @property
    def channel(self) -> str:
        """Channel name (meadow, garden, grapher, etc.). Available for DataStep."""
        ...


def compile_steps(
    dag: Graph,
    subdag: Graph,
) -> List[Step]:
    """
    Return the list of steps which, if executed in order, mean that every
    step has its dependencies ready for it.

    Parameters
    ----------
    dag : Graph
        The full DAG containing all steps and their complete dependency information.
        Required to get the full dependencies of each step during parsing.
    subdag : Graph
        The filtered subset of steps to execute. Used to determine execution order
        while maintaining access to complete dependency information from the full DAG.

    Returns
    -------
    List[Step]
        Steps in dependency order, ready for execution.
    """
    # make sure each step runs after its dependencies
    steps = to_dependency_order(subdag)

    # parse the steps into Python objects
    # NOTE: We need the full DAG here to get complete dependencies of each step
    return [parse_step(name, dag) for name in steps]


def parse_step(step_name: str, dag: Dict[str, Any]) -> Step:
    "Convert each step's name into a step object that we can run."
    parts = urlparse(step_name)
    step_type = parts.scheme
    path = parts.netloc + parts.path
    # dependencies are new objects
    dependencies = [parse_step(s, dag) for s in dag.get(step_name, [])]

    if step_type not in STEP_REGISTRY:
        raise Exception(f"no recipe for executing step: {step_name}")

    step_class = STEP_REGISTRY[step_type]

    # Different step types have different constructor signatures
    # Steps with dependencies: DataStep, DataStepPrivate, ExportStep
    # Steps without dependencies: SnapshotStep, SnapshotStepPrivate, ETagStep, GithubStep
    import inspect

    sig = inspect.signature(step_class.__init__)
    if "dependencies" in sig.parameters:
        step = step_class(path, dependencies)
    else:
        step = step_class(path)

    return step


def extract_step_attributes(step: str) -> Dict[str, str]:
    """Extract attributes of a step from its name in the dag.

    Parameters
    ----------
    step : str
        Step (as it appears in the dag).

    Returns
    -------
    step : str
        Step (as it appears in the dag).
    step_type : str
        Type of step (e.g. data or export).
    kind : str
        Kind of step (namely, 'public' or 'private').
    channel: str
        Channel (e.g. 'meadow').
    namespace: str
        Namespace (e.g. 'energy').
    version: str
        Version (e.g. '2023-01-26').
    name: str
        Short name of the dataset (e.g. 'primary_energy').
    identifier : str
        Identifier of the step that is independent of the kind and of the version of the step.

    """
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
    elif prefix in ["snapshot"]:
        # Ingestion steps.
        channel = prefix

        # Extract attributes from root of the step.
        namespace, version, name = root.split("/")

        # Define an identifier for this step, that is identical for all versions.
        identifier = f"{channel}/{namespace}/{name}"
    else:
        # Regular data steps.

        # Extract attributes from root of the step.
        channel, namespace, version, name = root.split("/")

        # Define an identifier for this step, that is identical for all versions.
        identifier = f"{channel}/{namespace}/{name}"

    attributes = {
        "step": step,
        "step_type": prefix,
        "kind": kind,
        "channel": channel,
        "namespace": namespace,
        "version": version,
        "name": name,
        "identifier": identifier,
    }

    return attributes


def load_from_uri(uri: str) -> catalog.Dataset | Snapshot:
    """Load an ETL dataset from a URI."""
    attributes = extract_step_attributes(cast(str, uri))
    # Snapshot
    if attributes["channel"] == "snapshot":
        path = f"{attributes['namespace']} / {attributes['version']} / {attributes['name']}"
        try:
            dataset = Snapshot(path)
        except FileNotFoundError:
            raise FileNotFoundError(f"Snapshot not found for URI '{uri}'. Please run `python snapshot {path}` first")
    # Data
    else:
        path = f"{attributes['channel']}/{attributes['namespace']}/{attributes['version']}/{attributes['name']}"
        try:
            dataset = catalog.Dataset(paths.DATA_DIR / path)
        except FileNotFoundError:
            raise FileNotFoundError(f"Dataset not found for URI '{uri}'. Please run `etlr {uri}` first")
    return dataset


def get_etag(url: str, verify_ssl: bool = True) -> str:
    """Get the ETag header from a URL.

    Args:
        url: The URL to check.
        verify_ssl: Whether to verify SSL certificates.

    Returns:
        The ETag header value.
    """
    resp = requests.head(url, verify=verify_ssl)
    resp.raise_for_status()
    return cast(str, resp.headers["ETag"])


@register_step("data")
class DataStep:
    """
    A step which creates a local Dataset on disk in the data/ folder. You specify it
    by making a Python module or a Jupyter notebook with a matching path in the
    etl/steps/data folder.

    Features:
    - PREFER_DOWNLOAD: Download from R2 catalog if checksums match
    - INSTANT: Fast-track metadata-only updates for garden steps
    - DEBUG: Run in-process instead of subprocess
    - Jupyter notebook support
    - Subprocess execution with memory limits (prlimit)
    """

    path: str
    dependencies: List[Step]
    version: str = ""
    is_public: bool = True

    def __init__(self, path: str, dependencies: List[Step]) -> None:
        self.path = path
        self.dependencies = dependencies
        # Extract version from path: channel/namespace/version/dataset
        parts = self.path.split("/")
        self.version = parts[2] if len(parts) > 2 else ""

    def __str__(self) -> str:
        return f"data://{self.path}"

    @property
    def channel(self) -> str:
        return self.path.split("/")[0]

    @property
    def namespace(self) -> str:
        parts = self.path.split("/")
        return parts[1] if len(parts) > 1 else ""

    @property
    def dataset(self) -> str:
        parts = self.path.split("/")
        return parts[3] if len(parts) > 3 else ""

    def _dataset_index_mtime(self) -> Optional[float]:
        try:
            return os.stat(self._output_dataset._index_file).st_mtime
        except KeyError as e:
            if _uses_old_schema(e):
                return None
            else:
                raise e
        except (ValueError, Exception) as e:
            # ValueError from base class, Exception from legacy code
            if "dataset has not been created yet" in str(e).lower() or "has not been created yet" in str(e).lower():
                return None
            else:
                raise e

    def run(self) -> None:
        # make sure the enclosing folder is there
        self._dest_dir.parent.mkdir(parents=True, exist_ok=True)

        if config.PREFER_DOWNLOAD:
            # if checksums match, download the dataset from the catalog
            success = self._download_dataset_from_catalog()
            if success:
                return

        ds_idex_mtime = self._dataset_index_mtime()

        # if INSTANT flag is set, just update the metadata
        if config.INSTANT and self.channel == "garden":
            self._run_instant_metadata()
            return

        sp = self._search_path
        if sp.with_suffix(".py").exists() or (sp / "__init__.py").exists():
            if config.DEBUG:
                self._run_py_isolated()
            else:
                self._run_py()

        # We lock this to prevent the following error
        # ImportError: PyO3 modules may only be initialized once per interpreter process
        elif sp.with_suffix(".ipynb").exists():
            with ipynb_lock:
                self._run_notebook()

        else:
            raise Exception(f"have no idea how to run step: {self.path}")

        # was the index file modified? if not then `save` was not called
        # NOTE: we se warnings.warn instead of log.warning because we want this in stderr
        new_ds_index_mtime = self._dataset_index_mtime()
        if new_ds_index_mtime is None or ds_idex_mtime == new_ds_index_mtime:
            warnings.warn(f"Step {self.path} did not call .save() on its output dataset")

        # modify the dataset to remember what inputs were used to build it
        dataset = self._output_dataset
        dataset.metadata.source_checksum = self.checksum_input()
        dataset.save()

        self.after_run()

    def after_run(self) -> None:
        """Hook called after step execution. Override in subclasses."""
        pass

    def is_dirty(self) -> bool:
        if not self.has_existing_data() or any(d.is_dirty() for d in self.dependencies):
            return True

        try:
            found_source_checksum = catalog.Dataset(self._dest_dir.as_posix()).metadata.source_checksum
        except KeyError as e:
            if _uses_old_schema(e):
                return True
            else:
                raise e
        exp_source_checksum = self.checksum_input()

        # if INSTANT is on, we use _instant suffix in a checksum
        if config.INSTANT and found_source_checksum:
            found_source_checksum = found_source_checksum.replace("_instant", "")

        if found_source_checksum != exp_source_checksum:
            return True

        return False

    def has_existing_data(self) -> bool:
        return self._dest_dir.is_dir()

    def can_execute(self, archive_ok: bool = True) -> bool:
        sp = self._search_path
        if not archive_ok and "/archive/" in sp.as_posix():
            return False

        return (
            # python script
            sp.with_suffix(".py").exists()
            # folder of scripts with __init__.py
            or (sp / "__init__.py").exists()
            # jupyter notebook
            or sp.with_suffix(".ipynb").exists()
        )

    def checksum_input(self) -> str:
        """Calculate checksum of all inputs to this step."""
        checksums = {
            # The pandas library is so important to the output that we include it in the checksum
            "__pandas__": pd.__version__,
            # If the epoch changes, rebuild everything
            "__etl_epoch__": str(config.ETL_EPOCH),
        }

        # Add any extra checksum items from subclasses
        checksums.update(self._get_checksum_extras())

        for d in self.dependencies:
            checksums[d.path] = d.checksum_output()

        for f in self._step_files():
            checksums[f] = checksum_file(f)

        in_order = [v for _, v in sorted(checksums.items())]
        return hashlib.md5(",".join(in_order).encode("utf8")).hexdigest()

    def _get_checksum_extras(self) -> Dict[str, str]:
        """Add OWID-specific items to checksum calculation."""
        extras: Dict[str, str] = {}

        # If using SUBSET, add it to checksums for files that reference it
        if config.SUBSET:
            for f in self._step_files():
                with open(f) as istream:
                    if "SUBSET" in istream.read():
                        extras[f"__subset_{f}__"] = config.SUBSET

        return extras

    def checksum_output(self) -> str:
        """Output checksum is the same as input checksum."""
        return self.checksum_input()

    @property
    def _output_dataset(self) -> catalog.Dataset:
        """Get the output dataset."""
        if not self._dest_dir.is_dir():
            raise ValueError("Dataset has not been created yet")
        return catalog.Dataset(self._dest_dir.as_posix())

    def _step_files(self) -> List[str]:
        "Return a list of code files defining this step."
        # if dataset is a folder, use all its files
        if self._search_path.is_dir():
            return [p.as_posix() for p in files.walk(self._search_path)]

        # if a dataset is a single file, use [dataset].py and shared* files
        return glob(self._search_path.as_posix() + "*") + glob((self._search_path.parent / "shared*").as_posix())

    @property
    def _search_path(self) -> Path:
        """Path where step code is located.

        Checks archive directory first before falling back to steps_dir.
        """
        # Check archive directory first
        archive_path = paths.STEP_DIR / "archive" / self.path
        if list(archive_path.parent.glob(archive_path.name + "*")):
            return archive_path

        return paths.STEP_DIR / "data" / self.path

    @property
    def _dest_dir(self) -> Path:
        """Destination directory for output data."""
        return paths.DATA_DIR / self.path.lstrip("/")

    def _run_instant_metadata(self) -> None:
        """If INSTANT flag is set, instead of running the whole garden step, just load
        the existing dataset and update its metadata. This is useful for fast-tracking
        metadata changes without having to rerun the whole step.
        """

        meta_path = get_metadata_path(self._dest_dir.as_posix())
        if not meta_path.exists():
            return

        ds = catalog.Dataset(self._dest_dir.as_posix())

        # Read metadata before
        table_meta_before = _load_tables_metadata(ds)

        # Save dataset, but use _instant suffix in the checksum to make sure we
        # trigger fresh run when not using INSTANT
        ds.update_metadata(meta_path)

        # Also load the override metadata file if it exists (similar to create_dataset in helpers.py)
        meta_override_path = meta_path.with_suffix(".override.yml")
        if meta_override_path.exists():
            ds.update_metadata(meta_override_path)

        ds.metadata.source_checksum = self.checksum_input() + "_instant"
        ds.save()

        # Read metadata after
        table_meta_after = _load_tables_metadata(ds)

        INSTANT_METADATA_DIFF[ds.m.short_name] = defaultdict(list)

        for table_name in ds.table_names:
            # Find variables with metadata changes
            for var_name in table_meta_before[table_name]["fields"]:
                if (
                    table_meta_before[table_name]["fields"][var_name]
                    != table_meta_after[table_name]["fields"][var_name]
                ):
                    log.info("instant.update", table_name=table_name, var_name=var_name)
                    # Add the variable to a global variable so that we can only rerun changed
                    # variables in the grapher step
                    INSTANT_METADATA_DIFF[ds.m.short_name][table_name].append(var_name)

    def _run_py_isolated(self) -> None:
        """
        Import the Python module for this step and call run() on it. This method
        does not have overhead from forking an extra process like _run_py and
        should be used with caution.
        """
        # path can be either in a module with __init__.py or a single .py file
        module_dir = self._search_path if self._search_path.is_dir() else self._search_path.parent

        with isolated_env(module_dir):
            step_module = import_module(self._search_path.relative_to(paths.BASE_DIR).as_posix().replace("/", "."))
            run_module_run(step_module, self._dest_dir.as_posix())

    def _run_py(self) -> None:
        """
        Import the Python module for this step and call run() on it.
        """
        # use a subprocess to isolate each step from the others, and avoid state bleeding
        # between them
        args = []

        if sys.platform == "linux":
            args.extend(["prlimit", f"--as={config.MAX_VIRTUAL_MEMORY_LINUX}"])

        args.extend(["uv", "run", "etl", "d", "run-python-step"])

        if config.IPDB_ENABLED:
            args.append("--ipdb")

        args.extend(
            [
                str(self),
                self._dest_dir.as_posix(),
            ]
        )

        # Add uv to the path, it causes problems in Buildkite
        env = os.environ.copy()
        env["PATH"] = os.path.expanduser("~/.cargo/bin") + ":" + env["PATH"]

        try:
            subprocess.check_call(args, env=env)
        except subprocess.CalledProcessError as e:
            sys.exit(e.returncode)

    def _run_notebook(self) -> None:
        "Run a parameterised Jupyter notebook."
        # don't import it again if it's already imported to avoid
        # ImportError: PyO3 modules may only be initialized once per interpreter process
        if "papermill" not in sys.modules:
            # smother deprecation warnings by papermill
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                import papermill as pm
        else:
            pm = sys.modules["papermill"]

        notebook_path = self._search_path.with_suffix(".ipynb")
        with tempfile.TemporaryDirectory() as tmp_dir:
            notebook_out = Path(tmp_dir) / "notebook.ipynb"
            log_file = Path(tmp_dir) / "output.log"
            with open(log_file.as_posix(), "w") as ostream:
                pm.execute_notebook(
                    notebook_path.as_posix(),
                    notebook_out.as_posix(),
                    parameters={"dest_dir": self._dest_dir.as_posix()},
                    progress_bar=False,
                    stdout_file=ostream,
                    stderr_file=ostream,
                    cwd=notebook_path.parent.as_posix(),
                )

    def _download_dataset_from_catalog(self) -> bool:
        """Download the dataset from the catalog if the checksums match. Return True if successful."""
        url = f"{OWID_CATALOG_URI}{self.path}/index.json"
        resp = requests.get(url, verify=TLS_VERIFY)
        if not resp.ok:
            return False

        ds_meta = resp.json()

        # checksums don't match, return False
        if self.checksum_output() != ds_meta["source_checksum"]:
            return False

        r2 = s3_utils.connect_r2_cached()

        # Get available formats of a dataset
        s3_files = s3_utils.list_s3_objects(f"s3://owid-catalog/{self.path}/", client=s3_utils.connect_r2_cached())
        available_formats = {f.split(".")[-1] for f in s3_files} - {"json"}

        # if one of the format is in DEFAULT_FORMATS, download it
        if set(available_formats) & set(DEFAULT_FORMATS):
            download_formats = DEFAULT_FORMATS
        # otherwise download all available formats
        # NOTE: explorers only publish .csv format which might not be in DEFAULT_FORMATS
        else:
            download_formats = available_formats

        include = [".meta.json"] + [f".{format}" for format in download_formats]

        s3_utils.download_s3_folder(
            f"s3://owid-catalog/{self.path}/",
            self._dest_dir,
            client=r2,
            include=include,
            exclude=["index.json"],
            delete=True,
        )

        """download files over HTTPS, the problem is that we don't have a list of tables to download
        in index.json

        from owid.datautils.web import download_file_from_url

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            futures = []

            for fname in ("cherry_blossom.feather", "cherry_blossom.meta.json"):
                futures.append(
                    executor.submit(
                        download_file_from_url,
                        f"{OWID_CATALOG_URI}/{self.path}/{fname}",
                        f"{self._dest_dir}/{fname}",
                    )
                )
        """

        # save index.json file after successful download
        with open(self._dest_dir / "index.json", "w") as ostream:
            json.dump(ds_meta, ostream)

        log.info(f"Downloaded {self.path} from catalog")

        return True


@register_step("snapshot")
@dataclass
class SnapshotStep:
    path: str
    dependencies: List[Step] = None  # type: ignore
    is_public: bool = True

    def __post_init__(self):
        if self.dependencies is None:
            self.dependencies = []

    def __str__(self) -> str:
        return f"snapshot://{self.path}"

    def can_execute(self, archive_ok: bool = True) -> bool:
        try:
            Snapshot(self.path)
            return True

        except Exception:
            return False

    def run(self) -> None:
        snap = Snapshot(self.path)
        snap.pull(force=True, retries=3)

    def is_dirty(self) -> bool:
        snap = Snapshot(self.path)
        return snap.is_dirty()

    def has_existing_data(self) -> bool:
        return True

    def checksum_output(self) -> str:
        return files.checksum_file(self._dvc_path)

    @property
    def _dvc_path(self) -> str:
        return f"{paths.SNAPSHOTS_DIR}/{self.path}.dvc"

    @property
    def _path(self) -> str:
        return f"{paths.DATA_DIR}/snapshots/{self.path}"

    @property
    def version(self) -> str:
        # namspace / version / filename
        return self.path.split("/")[1]


@register_step("snapshot-private")
class SnapshotStepPrivate(SnapshotStep):
    is_public: bool = False

    def __str__(self) -> str:
        return f"snapshot-private://{self.path}"

    def run(self) -> None:
        snap = Snapshot(self.path)
        assert snap.metadata.is_public is False
        snap.pull(force=True)


@register_step("export")
class ExportStep(DataStep):
    """
    A step which exports something once. For instance committing to a Github repository
    or upserting an Explorer to DB.
    """

    path: str
    dependencies: List[Step]

    def __init__(self, path: str, dependencies: List[Step]) -> None:
        self.dependencies = dependencies
        self.path = path

    def __str__(self) -> str:
        return f"export://{self.path}"

    def run(self) -> None:
        # make sure the enclosing folder is there
        self._dest_dir.parent.mkdir(parents=True, exist_ok=True)

        from etl.helpers import create_dataset

        # Create folder for the dataset, export step can save files there
        ds = create_dataset(self._dest_dir, tables=[])

        sp = self._search_path
        if sp.with_suffix(".py").exists() or (sp / "__init__.py").exists():
            if config.DEBUG:
                DataStep._run_py_isolated(self)  # type: ignore
            else:
                DataStep._run_py(self)  # type: ignore

        # save checksum
        ds.metadata.source_checksum = self.checksum_input()
        ds.save()

    def checksum_output(self) -> str:
        # output checksum is checksum of all ingredients
        return self.checksum_input()

    @property
    def _search_path(self) -> Path:
        return paths.STEP_DIR / "export" / self.path

    @property
    def _dest_dir(self) -> Path:
        return paths.EXPORT_DIR / self.path.lstrip("/")


@register_step("etag")
class ETagStep:
    """
    An empty step that represents a dependency on the ETag of a URL.

    This step doesn't produce any output, but its checksum changes when the
    ETag of the URL changes, triggering rebuilds of dependent steps.
    """

    path: str
    version: str = "latest"
    is_public: bool = True
    dependencies: List[Step] = []

    def __init__(self, path: str) -> None:
        self.path = path
        self.dependencies = []

    def __str__(self) -> str:
        return f"etag://{self.path}"

    def is_dirty(self) -> bool:
        return False

    def run(self) -> None:
        # Nothing is done for this step
        pass

    def checksum_output(self) -> str:
        return get_etag(f"https://{self.path}", verify_ssl=TLS_VERIFY)


@dataclass
class GithubRepo:
    """A helper class for working with GitHub repositories.

    Provides methods to clone, update, and check the status of a GitHub repository.
    Repositories are cached locally in ~/.owid/git/.
    """

    org: str
    repo: str

    @property
    def github_url(self) -> str:
        return f"https://github.com/{self.org}/{self.repo}"

    @property
    def cache_dir(self) -> Path:
        return Path(f"~/.owid/git/{self.org}/{self.repo}").expanduser()

    def ensure_cloned(self, shallow: bool = True) -> None:
        """Ensure that a copy of this repo has been cloned and is up to date."""
        dest_dir = self.cache_dir
        if not dest_dir.is_dir():
            dest_dir.parent.mkdir(parents=True, exist_ok=True)
            cmd = ["git", "clone"]
            if shallow:
                cmd.extend(["--depth=1"])
            cmd.extend([self.github_url, dest_dir.as_posix()])
            subprocess.run(cmd, check=True)
        else:
            self.update_and_reset()

    def update_and_reset(self) -> None:
        """Fetch new changes from origin and do a hard reset."""
        if not self.cache_dir.is_dir():
            raise ValueError("Cannot update repo until repo is cloned")

        subprocess.run(
            ["git", "fetch"],
            cwd=self.cache_dir,
            check=True,
            capture_output=True,
        )
        subprocess.run(
            ["git", "reset", "--hard", f"origin/{self.branch_name}"],
            cwd=self.cache_dir,
            check=True,
            capture_output=True,
        )

    @property
    def branch_name(self) -> str:
        """Return the current branch name of the checked out repo."""
        result = subprocess.run(
            ["git", "symbolic-ref", "--short", "HEAD"],
            cwd=self.cache_dir,
            check=True,
            capture_output=True,
            text=True,
        )
        return result.stdout.strip()

    @property
    def latest_sha(self) -> str:
        """Return the latest commit SHA of the local repo."""
        master_file = self.cache_dir / ".git/refs/heads/master"
        with open(master_file, "r") as f:
            sha = f.read().strip()
        return sha

    def is_up_to_date(self) -> bool:
        """Returns true if remote has no new changes, false otherwise."""
        if not self.cache_dir.is_dir():
            return False
        return self.latest_remote_sha() == self.latest_sha

    def latest_remote_sha(self) -> str:
        """Return the latest commit SHA of the remote branch."""
        # We rely on the smart HTTPS protocol for Git served by GitHub
        uri = self.github_url + "/info/refs?service=git-upload-pack"
        resp = requests.get(uri, verify=TLS_VERIFY)
        resp.raise_for_status()
        lines = resp.content.decode("latin-1").splitlines()

        for line in lines:
            # Some repos use "main" instead of "master"
            if line.count(" ") == 1 and line.endswith("refs/heads/master"):
                # The first four bytes are the line length, ignore them
                sha = line.split(" ")[0][4:]
                return cast(str, sha)

        raise ValueError("Could not find latest remote SHA in response")


@register_step("github")
class GithubStep:
    """A step that tracks a GitHub repository.

    Shallow-clone a git repo and ensure that the most recent version is available.
    This has the effect of triggering a rebuild each time the repo gets new commits.
    """

    path: str
    gh_repo: GithubRepo
    version: str = "latest"
    is_public: bool = True
    dependencies: List[Step] = []

    def __init__(self, path: str) -> None:
        self.path = path
        self.dependencies = []
        try:
            org, repo = path.split("/")
        except ValueError:
            raise ValueError("github step is not in the form github://<org>/<repo>")

        self.gh_repo = GithubRepo(org, repo)

    def __str__(self) -> str:
        return f"github://{self.path}"

    def is_dirty(self) -> bool:
        # Always poll the git repo
        return not self.gh_repo.is_up_to_date()

    def run(self) -> None:
        # Either clone the repo, or update it
        self.gh_repo.ensure_cloned()

    def checksum_output(self) -> str:
        return self.gh_repo.latest_sha


class PrivateMixin:
    def after_run(self) -> None:
        """Make dataset private"""
        ds = catalog.Dataset(self._dest_dir.as_posix())  # type: ignore
        ds.metadata.is_public = False
        ds.save()


@register_step("data-private")
class DataStepPrivate(PrivateMixin, DataStep):
    is_public = False

    def __str__(self) -> str:
        return f"data-private://{self.path}"


def select_dirty_steps(steps: List[Step], workers: int = 1) -> List[Step]:
    """Select dirty steps using threadpool."""
    # dynamically add cached version of `is_dirty` to all steps to avoid re-computing
    # this is a bit hacky, but it's the easiest way to only cache it here without
    # affecting the rest
    cache_is_dirty = files.RuntimeCache()
    for s in steps:
        _add_is_dirty_cached(s, cache_is_dirty)

    with ThreadPoolExecutor(max_workers=workers) as executor:
        steps_dirty = executor.map(_step_is_dirty, steps)  # type: ignore
        steps = [s for s, is_dirty in zip(steps, steps_dirty) if is_dirty]

    cache_is_dirty.clear()

    return steps


def _step_is_dirty(s: Step) -> bool:
    return s.is_dirty()


def _cached_is_dirty(step: Step, cache: files.RuntimeCache) -> bool:
    key = str(step)
    if key not in cache:
        cache.add(key, step._is_dirty())
    return cache[key]


def _add_is_dirty_cached(s: Step, cache: files.RuntimeCache) -> None:
    """Save copy of a method to _is_dirty and replace it with a cached version."""
    s._is_dirty = s.is_dirty
    s._cache = cache
    s.is_dirty = partial(_cached_is_dirty, s, cache)
    for dep in getattr(s, "dependencies", []):
        _add_is_dirty_cached(dep, cache)


def _uses_old_schema(e: KeyError) -> bool:
    """Origins without `title` use old schema before rename. This can be removed once
    we recompute all datasets."""
    return e.args[0] == "title"


def _load_tables_metadata(ds: catalog.Dataset) -> Dict[str, Dict[str, Any]]:
    """Load metadata for all tables in a dataset."""
    table_meta = {}
    for table_name in ds.table_names:
        with open(Path(ds.path) / f"{table_name}.meta.json") as f:
            table_meta[table_name] = json.load(f)
    return table_meta
