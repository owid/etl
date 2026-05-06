import datetime
import importlib
import inspect
import os
import pathlib
import re
import shutil
import subprocess
import sys
from types import ModuleType

import click
from dotenv import load_dotenv

from owl.dataset import Action, Dataset
from owl.project import load_project, parse_step_file
from owl.snapshot import Snapshot


def _project_root() -> pathlib.Path:
    return load_project().root


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


@cli.command()
@click.argument("pattern", required=False)
@click.option(
    "--force",
    is_flag=True,
    default=False,
    help="Force re-run, ignore staleness checks.",
)
@click.option("--prefect", is_flag=True, default=False, help="Track execution in Prefect UI.")
def run(pattern, force, prefect):
    """Run steps matching a regex pattern (e.g. "worldbank/.*", "who/life_expectancy").

    If no pattern is given, all steps are run.
    Stale datasets (and their upstream dependencies) are rebuilt automatically.
    """
    steps_root = load_project().steps_root

    from owl.log import step as log_step

    for mod_name, module in _get_step_modules(steps_root, pattern):
        datasets = _find_datasets(module)
        actions = _find_actions(module)

        if not datasets and not actions:
            continue

        info = parse_step_file(module.__file__)
        log_step(f"{info.namespace}/{info.dataset}/{info.version_slug}")

        for ds_name, ds in datasets:
            if prefect:
                ds.run_with_prefect(force=force)
            else:
                ds.run(force=force)

        for act_name, act in actions:
            act.run(force=force)


@cli.command()
@click.argument("pattern", required=False)
@click.option(
    "--version",
    default=None,
    help="Version identifier (e.g. 2025-06-30). Defaults to the version pinned in code.",
)
def snapshot(pattern, version):
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
            snap.fetch_and_save(version=version or snap.version)


@cli.command()
@click.argument("step", required=True)
@click.option(
    "--version",
    default=None,
    help="Version label (default: today's date, e.g. 2025-04-07).",
)
@click.option("--message", "-m", default="", help="Optional note for the lock file.")
def pin(step, version, message):
    """Pin the current dataset output so it's preserved across future rebuilds.

    Copies the parquet to a versioned file and writes a .lock with the
    git commit needed to rebuild.

    Example:
        owl pin biodiversity/cherry_blossom
        owl pin biodiversity/cherry_blossom --version 2024-01-25
    """
    import json
    import shutil
    import subprocess
    from datetime import date

    project_root = _project_root()
    project = load_project()

    modules = _get_step_modules(project.steps_root, step)
    datasets = [ds for _, module in modules for _, ds in _find_datasets(module)]
    if len(datasets) != 1:
        raise click.ClickException(f"Expected exactly one dataset matching {step!r}, found {len(datasets)}.")
    dataset = datasets[0]

    source_dir = dataset._data_path
    if not (source_dir / "index.json").exists():
        raise click.ClickException(f"Dataset not found: {source_dir}\nRun: owl run {step}")

    version = version or date.today().isoformat()
    pinned = source_dir.with_name(f"{source_dir.name}@{version}")
    lock = source_dir.with_name(f"{source_dir.name}@{version}.lock")

    if pinned.exists() or lock.exists():
        raise click.ClickException(f"Already pinned: {pinned.name}\nUse a different --version or delete it first.")

    try:
        commit = subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=project_root,
            text=True,
        ).strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        commit = "unknown"

    shutil.copytree(source_dir, pinned)

    step_file = pathlib.Path(dataset._source_file).relative_to(project_root)
    lock_data = {
        "version": version,
        "commit": commit,
        "source": str(source_dir.relative_to(project_root)),
        "pinned_at": date.today().isoformat(),
        "rebuild": f"git checkout {commit[:8]} -- {step_file} {step_file.parent / 'meta.yml'} && owl run {step}",
    }
    if message:
        lock_data["message"] = message

    lock.write_text(json.dumps(lock_data, indent=2) + "\n")

    from owl.log import dataset as _log_dataset

    _log_dataset(f"pinned {pinned.name}")
    click.echo(f"  lock: {lock.name}")
    click.echo(f"  commit: {commit[:8]}")
    if message:
        click.echo(f"  note: {message}")


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
                    dep_id = f"{dep.name}\n({dep.version})"
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


def build_catalog():
    """Build an Owl catalog from Parquet file metadata.

    Only indexes datasets whose source directory has a matching owl_steps module.
    This prevents stale data from other branches from appearing in the catalog.
    """
    import json

    import pyarrow.parquet as pq

    project_root = _project_root()
    data_dir = load_project().data_root
    steps_dir = load_project().steps_root

    # Only include sources that have ETL steps tracked in git.
    # Untracked step dirs from other branches are ignored.
    import subprocess

    tracked = subprocess.run(
        ["git", "ls-tree", "-d", "--name-only", "HEAD", f"{load_project().steps_dir}/"],
        capture_output=True,
        text=True,
        cwd=project_root,
    )
    if tracked.returncode == 0 and tracked.stdout.strip():
        active_sources = {line.split("/")[-1] for line in tracked.stdout.strip().splitlines() if line}
    else:
        # Fallback: not a git repo or no tracked steps — use all directories
        active_sources = (
            {p.name for p in steps_dir.iterdir() if p.is_dir() and not p.name.startswith("_")}
            if steps_dir.is_dir()
            else set()
        )

    entries = []

    for parquet_path in sorted(data_dir.rglob("*.parquet")):
        rel = parquet_path.relative_to(data_dir)
        parts = rel.with_suffix("").parts  # e.g. ("worldbank", "population")
        if len(parts) != 2:
            continue
        source, name = parts

        if active_sources and source not in active_sources:
            continue
        slug = f"{source}/{name}"

        table = pq.read_table(parquet_path)
        meta = table.schema.metadata or {}

        # Read embedded metadata from DatasetMeta
        description = meta.get(b"description", b"").decode()
        source_str = meta.get(b"source", b"").decode()
        tags_raw = meta.get(b"tags", b"[]").decode()
        columns_raw = meta.get(b"columns", b"{}").decode()

        default_entities_raw = meta.get(b"default_entities", b"[]").decode()

        try:
            tags = json.loads(tags_raw)
        except json.JSONDecodeError:
            tags = []

        try:
            col_meta = json.loads(columns_raw)
        except json.JSONDecodeError:
            col_meta = {}

        try:
            default_entities = json.loads(default_entities_raw)
        except json.JSONDecodeError:
            default_entities = []

        # Build column info from schema + embedded metadata
        columns = []
        for field in table.schema:
            if field.name in ("__index_level_0__",):
                continue
            cm = col_meta.get(field.name, {})
            columns.append(
                {
                    "name": field.name,
                    "type": str(field.type),
                    "title": cm.get("title", ""),
                    "description": cm.get("description", ""),
                    "unit": cm.get("unit", ""),
                    "role": cm.get("role", ""),
                }
            )

        # Title: humanize the slug
        title = name.replace("_", " ").title()

        file_size = parquet_path.stat().st_size

        # Derive temporal coverage from time column
        temporal_coverage = None
        time_cols = [c for c, m in col_meta.items() if m.get("role") == "time"]
        if time_cols:
            time_col = time_cols[0]
            if time_col in table.column_names:
                arr = table.column(time_col)
                try:
                    vals = arr.drop_null().to_pylist()
                    if vals:
                        temporal_coverage = {
                            "start": str(min(vals)),
                            "end": str(max(vals)),
                        }
                except (TypeError, ValueError):
                    pass  # non-comparable values in time column

        # Use file modification time as last_updated
        mtime = parquet_path.stat().st_mtime
        last_updated = datetime.datetime.fromtimestamp(mtime).strftime("%Y-%m-%d")

        entries.append(
            {
                "slug": slug,
                "source": source_str or source.upper(),
                "name": name,
                "title": title,
                "description": description,
                "tags": tags,
                "columns": columns,
                "num_rows": table.num_rows,
                "file_size_bytes": file_size,
                "download_url": f"/catalog/{slug}.parquet",
                **({"temporal_coverage": temporal_coverage} if temporal_coverage else {}),
                **({"last_updated": last_updated} if last_updated else {}),
                **({"default_entities": default_entities} if default_entities else {}),
            }
        )

    print(json.dumps(entries, indent=2, ensure_ascii=False))


SYNC_DIRS = ["data"]


def _require_rclone():
    if not shutil.which("rclone"):
        raise click.ClickException("rclone is not installed. Install it from https://rclone.org/install/")


def _get_r2_prefix() -> str:
    """Read r2-prefix from pyproject.toml [tool.owl]. Returns empty string if not set."""
    import tomli

    pyproject = _project_root() / "pyproject.toml"
    if not pyproject.exists():
        return ""
    with open(pyproject, "rb") as f:
        data = tomli.load(f)
    return data.get("tool", {}).get("owl", {}).get("r2-prefix", "").strip("/")


def _load_r2_config() -> dict[str, str]:
    load_dotenv(_project_root() / ".env")

    keys = [
        "R2_BUCKET_NAME",
        "R2_ENDPOINT_URL",
        "R2_ACCESS_KEY_ID",
        "R2_SECRET_ACCESS_KEY",
    ]
    config = {}
    missing = []
    for key in keys:
        val = os.environ.get(key)
        if not val:
            missing.append(key)
        else:
            config[key] = val

    if missing:
        raise click.ClickException(
            f"Missing R2 config in .env: {', '.join(missing)}\nCopy .env.example to .env and fill in your credentials."
        )
    return config


def _rclone_sync(local_dir: pathlib.Path, bucket: str, remote_path: str, config: dict, dry_run: bool):
    cmd = [
        "rclone",
        "copy",
        str(local_dir),
        f":s3:{bucket}/{remote_path}",
        "--s3-provider",
        "Cloudflare",
        "--s3-access-key-id",
        config["R2_ACCESS_KEY_ID"],
        "--s3-secret-access-key",
        config["R2_SECRET_ACCESS_KEY"],
        "--s3-endpoint",
        config["R2_ENDPOINT_URL"],
        "--s3-no-check-bucket",
        "--progress",
    ]
    if dry_run:
        cmd.append("--dry-run")

    click.echo(f"{'[DRY RUN] ' if dry_run else ''}Syncing {local_dir} → s3://{bucket}/{remote_path}")
    subprocess.run(cmd, check=True)


def _rclone_delete(bucket: str, remote_path: str, config: dict, dry_run: bool):
    """Delete a remote path from R2."""
    cmd = [
        "rclone",
        "purge",
        f":s3:{bucket}/{remote_path}",
        "--s3-provider",
        "Cloudflare",
        "--s3-access-key-id",
        config["R2_ACCESS_KEY_ID"],
        "--s3-secret-access-key",
        config["R2_SECRET_ACCESS_KEY"],
        "--s3-endpoint",
        config["R2_ENDPOINT_URL"],
        "--s3-no-check-bucket",
    ]
    if dry_run:
        cmd.append("--dry-run")

    click.echo(f"{'[DRY RUN] ' if dry_run else ''}Deleting s3://{bucket}/{remote_path}")
    subprocess.run(cmd, check=True)


@cli.command()
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Preview what would be synced without uploading.",
)
@click.option(
    "--dir",
    "dirs",
    multiple=True,
    type=click.Choice(SYNC_DIRS),
    help="Sync only specific directories. Can be repeated. Default: all.",
)
@click.option(
    "--prefix",
    default="",
    help="R2 path prefix (e.g. 'preview/pr-7'). Data is uploaded to {prefix}/{dir}/.",
)
@click.option(
    "--delete-prefix",
    default="",
    help="Delete all data under this R2 prefix (e.g. 'preview/pr-7').",
)
def sync(dry_run, dirs, prefix, delete_prefix):
    """Sync Owl outputs in data/ to Cloudflare R2."""
    _require_rclone()
    config = _load_r2_config()

    if delete_prefix:
        _rclone_delete(config["R2_BUCKET_NAME"], delete_prefix, config, dry_run)
        return

    # Read r2-prefix from pyproject.toml [tool.owl] for multi-project bucket isolation.
    # When set, data goes to s3://bucket/<project>/<prefix>/data/
    # When empty (default), data goes to s3://bucket/<prefix>/data/
    project_prefix = _get_r2_prefix()

    targets = list(dirs) if dirs else SYNC_DIRS
    project_root = _project_root()

    for dir_name in targets:
        local_dir = project_root / dir_name
        if not local_dir.exists():
            click.echo(f"Skipping {dir_name}/ — directory does not exist.")
            continue
        parts = [p for p in [project_prefix, prefix, dir_name] if p]
        remote_path = "/".join(parts)
        _rclone_sync(local_dir, config["R2_BUCKET_NAME"], remote_path, config, dry_run)
