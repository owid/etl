from __future__ import annotations

import pathlib
import re
from dataclasses import dataclass

import tomli


@dataclass(frozen=True)
class OwlProject:
    root: pathlib.Path
    steps_dir: str = "owl_steps"
    snapshots_dir: str = "data/snapshots"
    data_dir: str = "data"
    default_channel: str = "garden"

    @property
    def steps_root(self) -> pathlib.Path:
        return self.root / self.steps_dir

    @property
    def snapshots_root(self) -> pathlib.Path:
        return self.root / self.snapshots_dir

    @property
    def data_root(self) -> pathlib.Path:
        return self.root / self.data_dir


@dataclass(frozen=True)
class OwlStepInfo:
    namespace: str
    dataset: str
    version_slug: str
    version: str
    module: str


def find_project_root(start: pathlib.Path | None = None) -> pathlib.Path:
    """Find the nearest parent containing pyproject.toml, .git, or owl_steps."""
    current = (start or pathlib.Path.cwd()).resolve()
    for path in [current, *current.parents]:
        if (path / "pyproject.toml").exists() or (path / ".git").exists() or (path / "owl_steps").exists():
            return path
    return current


def load_project(start: pathlib.Path | None = None) -> OwlProject:
    root = find_project_root(start)
    config = {}
    pyproject = root / "pyproject.toml"
    if pyproject.exists():
        with pyproject.open("rb") as f:
            config = tomli.load(f).get("tool", {}).get("owl", {})

    return OwlProject(
        root=root,
        steps_dir=config.get("steps-dir", "owl_steps"),
        snapshots_dir=config.get("snapshots-dir", "data/snapshots"),
        data_dir=config.get("data-dir", "data"),
        default_channel=config.get("default-channel", "garden"),
    )


def find_steps_root(source_file: str) -> pathlib.Path:
    """Walk up from a step source file to the configured Owl steps directory."""
    src = pathlib.Path(source_file).resolve()
    project = load_project(src.parent)
    steps_root = project.steps_root.resolve()
    try:
        src.relative_to(steps_root)
    except ValueError as err:
        raise RuntimeError(f"Could not find Owl steps root '{steps_root}' for path: {src}") from err
    return steps_root


def _parse_version_slug(version_slug: str) -> str:
    match = re.fullmatch(r"v(\d{4})(\d{2})(\d{2})", version_slug)
    if match:
        year, month, day = match.groups()
        return f"{year}-{month}-{day}"
    if version_slug == "latest":
        return version_slug
    raise ValueError(f"Owl version folders must be named vYYYYMMDD or latest, got: {version_slug}")


def parse_step_file(source_file: str) -> OwlStepInfo:
    """Parse owl_steps/<namespace>/<dataset>/<version>/step.py into ETL path parts."""
    src = pathlib.Path(source_file).resolve()
    steps_root = find_steps_root(source_file)
    rel = src.relative_to(steps_root)
    parts = rel.parts
    if len(parts) != 4 or parts[-1] != "step.py":
        raise RuntimeError(f"Owl step files must live at owl_steps/<namespace>/<dataset>/vYYYYMMDD/step.py; got {rel}")
    namespace, dataset, version_slug, module = parts
    return OwlStepInfo(
        namespace=namespace,
        dataset=dataset,
        version_slug=version_slug,
        version=_parse_version_slug(version_slug),
        module=module,
    )


def dataset_output_dir(source_file: str, dataset_name: str, channel: str | None = None) -> pathlib.Path:
    """Return data/<channel>/<namespace>/<version>/<dataset_name> for an Owl dataset."""
    project = load_project(pathlib.Path(source_file).parent)
    info = parse_step_file(source_file)
    return project.data_root / (channel or project.default_channel) / info.namespace / info.version / dataset_name
