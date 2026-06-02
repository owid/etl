import importlib
import inspect
import pathlib
import re
import sys
from datetime import date
from types import ModuleType

import click

from owl.dataset import Action, Dataset
from owl.project import load_project, parse_step_file
from owl.snapshot import Snapshot


def _get_step_modules(steps_root: pathlib.Path, pattern: str | None) -> list[tuple[str, ModuleType]]:
    """Load step modules, optionally filtered by regex pattern."""
    project_root = load_project().root
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

    all_paths = sorted(steps_root.rglob("step.py"))

    if pattern:
        regex = re.compile(pattern)

        def matches(path: pathlib.Path) -> bool:
            info = parse_step_file(str(path))
            candidates = [
                f"{info.namespace}/{info.dataset}",
                f"{info.namespace}/{info.dataset}/{info.version_slug}",
                f"{info.namespace}/{info.dataset}/{info.version}",
                str(path.relative_to(steps_root).with_suffix("")),
            ]
            return any(regex.search(candidate) for candidate in candidates)

        paths = [p for p in all_paths if matches(p)]
        if not paths:
            raise click.ClickException(f"No steps matched pattern: {pattern}")
    else:
        paths = all_paths

    modules = []
    for path in paths:
        rel = path.relative_to(project_root)
        mod_name = ".".join(rel.with_suffix("").parts)
        module = importlib.import_module(mod_name)
        modules.append((mod_name, module))
    return modules


def _find_datasets(module) -> list[tuple[str, Dataset]]:
    """Find Dataset objects defined in this module (not imported)."""
    return [
        (name, obj)
        for name, obj in inspect.getmembers(module)
        if isinstance(obj, Dataset) and obj._source_file == module.__file__
    ]


def _find_actions(module) -> list[tuple[str, Action]]:
    """Find Action objects defined in this module (not imported)."""
    return [
        (name, obj)
        for name, obj in inspect.getmembers(module)
        if isinstance(obj, Action) and obj._source_file == module.__file__
    ]


def _find_snapshots(module) -> list[tuple[str, Snapshot]]:
    """Find Snapshot objects defined in this module (not imported)."""
    return [
        (name, obj)
        for name, obj in inspect.getmembers(module)
        if isinstance(obj, Snapshot) and obj._source_file == module.__file__
    ]


@click.group()
def cli():
    pass


@cli.command("new")
@click.argument("path")
@click.option("--date", "version_date", default=None, help="Version date as YYYY-MM-DD or YYYYMMDD. Defaults to today.")
@click.option("--snapshot/--no-snapshot", default=True, help="Include a starter snapshot.")
def new_step(path, version_date, snapshot):
    """Create a new Owl step scaffold (e.g. biodiversity/cherry_blossom)."""
    project = load_project()
    parts = pathlib.PurePosixPath(path).parts
    if len(parts) != 2:
        raise click.ClickException("Path must be <namespace>/<dataset>, e.g. biodiversity/cherry_blossom")
    namespace, dataset = parts
    if not re.fullmatch(r"[a-z0-9_]+", namespace) or not re.fullmatch(r"[a-z0-9_]+", dataset):
        raise click.ClickException("Namespace and dataset must use lowercase letters, numbers, and underscores only.")

    if version_date is None:
        version = date.today().strftime("%Y%m%d")
        iso_version = date.today().isoformat()
    else:
        cleaned = version_date.replace("-", "")
        if not re.fullmatch(r"\d{8}", cleaned):
            raise click.ClickException("--date must be YYYY-MM-DD or YYYYMMDD")
        version = cleaned
        iso_version = f"{cleaned[:4]}-{cleaned[4:6]}-{cleaned[6:8]}"

    step_dir = project.steps_root / namespace / dataset / f"v{version}"
    step_path = step_dir / "step.py"
    meta_path = step_dir / "meta.yml"
    if step_path.exists() or meta_path.exists():
        raise click.ClickException(f"Step already exists: {step_dir}")

    step_dir.mkdir(parents=True, exist_ok=True)
    if snapshot:
        step_source = f"""import pandas as pd\n\nfrom owl import ColumnMeta, Dataset, DatasetMeta, Snapshot\n\n\n@Snapshot\ndef raw_data(snap):\n    # Replace with snap.download("https://example.com/data.csv") or return a DataFrame.\n    return pd.DataFrame({{"country": ["World"], "year": [{iso_version[:4]}], "value": [1]}})\n\n\n@Dataset\ndef {dataset}(raw_data: Snapshot):\n    df = raw_data.load()\n    return df, DatasetMeta(\n        title="TODO: dataset title",\n        description="TODO: dataset description",\n        columns={{\n            "country": ColumnMeta(title="Country", role="entity"),\n            "year": ColumnMeta(title="Year", role="time"),\n            "value": ColumnMeta(title="Value", unit="TODO"),\n        }},\n    )\n"""
        meta_source = f'''snapshots:\n  raw_data:\n    origin:\n      title: "TODO: source title"\n      producer: "TODO: producer"\n      date_accessed: "{iso_version}"\n      url_main: "TODO: source URL"\n\ndatasets:\n  {dataset}: {{}}\n'''
    else:
        step_source = f"""import pandas as pd\n\nfrom owl import ColumnMeta, Dataset, DatasetMeta\n\n\n@Dataset\ndef {dataset}():\n    df = pd.DataFrame({{"country": ["World"], "year": [{iso_version[:4]}], "value": [1]}})\n    return df, DatasetMeta(\n        title="TODO: dataset title",\n        description="TODO: dataset description",\n        columns={{\n            "country": ColumnMeta(title="Country", role="entity"),\n            "year": ColumnMeta(title="Year", role="time"),\n            "value": ColumnMeta(title="Value", unit="TODO"),\n        }},\n    )\n"""
        meta_source = f"""datasets:\n  {dataset}: {{}}\n"""

    step_path.write_text(step_source, encoding="utf-8")
    meta_path.write_text(meta_source, encoding="utf-8")
    click.echo(f"Created {step_dir}")
    click.echo(f"Next: owl snapshot {namespace}/{dataset} && owl run {namespace}/{dataset}")


@cli.command()
@click.argument("pattern", required=False)
@click.option(
    "--force",
    is_flag=True,
    default=False,
    help="Force re-run, ignore staleness checks.",
)
@click.option("--action", "action_kinds", multiple=True, help="Run actions of this kind. Can be repeated.")
@click.option("--grapher", is_flag=True, default=False, help="Run Grapher upsert actions.")
def run(pattern, force, action_kinds, grapher):
    """Run steps matching a regex pattern (e.g. "worldbank/.*", "who/life_expectancy").

    If no pattern is given, all steps are run.
    Stale datasets (and their upstream dependencies) are rebuilt automatically.
    """
    steps_root = load_project().steps_root

    from owl.log import step as log_step

    requested_action_kinds = set(action_kinds)
    if grapher:
        requested_action_kinds.add("grapher")

    for mod_name, module in _get_step_modules(steps_root, pattern):
        datasets = _find_datasets(module)
        actions = _find_actions(module)

        if not datasets and not actions:
            continue

        info = parse_step_file(module.__file__)
        log_step(f"{info.namespace}/{info.dataset}/{info.version_slug}")

        for ds_name, ds in datasets:
            ds.run(force=force)

        for act_name, act in actions:
            if act.default or act.kind in requested_action_kinds:
                act.run(force=force)


@cli.command()
@click.argument("pattern", required=False)
def snapshot(pattern):
    """Fetch and save snapshots for steps matching a regex pattern.

    This is the only way to update a snapshot. The pipeline never fetches data itself.
    """
    steps_root = load_project().steps_root

    from owl.log import step as log_step

    for mod_name, module in _get_step_modules(steps_root, pattern):
        snapshots = _find_snapshots(module)
        if not snapshots:
            continue

        info = parse_step_file(module.__file__)
        log_step(f"{info.namespace}/{info.dataset}/{info.version_slug}")

        for name, snap in snapshots:
            snap.fetch_and_save()


@cli.command()
@click.argument("pattern", required=False)
@click.option("--output", "-o", default="dag", help="Output filename (without extension).")
def viz(pattern, output):
    """Visualize the dependency DAG for matching steps.

    Opens a PNG showing Snapshot → Dataset dependencies.
    """
    import graphviz

    steps_root = load_project().steps_root

    dot = graphviz.Digraph("Owl", format="png")
    dot.attr(rankdir="LR")

    seen_nodes = set()

    for mod_name, module in _get_step_modules(steps_root, pattern):
        for ds_name, ds in _find_datasets(module):
            node_id = f"{ds.path}/{ds.name}"
            if node_id not in seen_nodes:
                dot.node(node_id, label=node_id, shape="box")
                seen_nodes.add(node_id)

            for dep in ds._dependencies():
                if isinstance(dep, Snapshot):
                    dep_id = f"{dep.name}\n(snapshot)"
                    if dep_id not in seen_nodes:
                        dot.node(dep_id, label=dep_id, shape="ellipse")
                        seen_nodes.add(dep_id)
                    dot.edge(dep_id, node_id)
                elif isinstance(dep, Dataset):
                    dep_node_id = f"{dep.path}/{dep.name}"
                    if dep_node_id not in seen_nodes:
                        dot.node(dep_node_id, label=dep_node_id, shape="box")
                        seen_nodes.add(dep_node_id)
                    dot.edge(dep_node_id, node_id)

    filepath = dot.render(output, view=True, cleanup=True)
    click.echo(f"DAG written to {filepath}")
