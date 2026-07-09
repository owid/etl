"""CLI for the content inspector.

The deterministic parts of the inspector live here (gathering content bundles, the lint pass,
and the findings store). The LLM detection pass runs in Claude Code via the ``inspector`` skill
(``.claude/skills/inspector``), which drives these commands and adds the semantic findings.

Typical flow:

    etl inspector gather -s energy -o ai/inspector/run1/bundles
    etl inspector lint ai/inspector/run1/bundles -o ai/inspector/run1/lint_findings.json
    etl inspector store ai/inspector/run1/*.json --bundles ai/inspector/run1/bundles
    etl inspector list
"""

import json
from pathlib import Path

import rich_click as click
from rich import print as rprint

from apps.inspector.schema import KINDS, ContentBundle
from etl import config

# Bundle filenames are `{kind}__{slug}.json`.
BUNDLE_FILENAME_SEPARATOR = "__"


def _load_bundles(bundle_dir: Path) -> list[ContentBundle]:
    bundles = [ContentBundle.load(path) for path in sorted(bundle_dir.glob("*.json"))]
    if not bundles:
        raise click.ClickException(f"No bundle files found in {bundle_dir}")
    return bundles


@click.group(name="inspector")
def cli() -> None:
    """Inspect public-facing content (charts, MDims, explorers, posts) for typos and semantic
    issues.

    These commands cover the deterministic parts (gather, lint, store, triage). For the full
    inspection including LLM semantic checks, use the `inspector` skill in Claude Code, which
    orchestrates them.
    """


@cli.command()
@click.option("--slug", "-s", multiple=True, help="Slug(s) to gather. Omit to gather everything.")
@click.option(
    "--type",
    "-t",
    "content_types",
    multiple=True,
    type=click.Choice(KINDS, case_sensitive=False),
    help="Content type(s) to gather. Omit for all types.",
)
@click.option("--limit", "-l", type=int, default=None, help="Max objects per content type.")
@click.option(
    "--output-dir",
    "-o",
    type=click.Path(path_type=Path),
    required=True,
    help="Directory to write one bundle JSON per content object.",
)
def gather(slug: tuple[str, ...], content_types: tuple[str, ...], limit: int | None, output_dir: Path) -> None:
    """Gather content bundles from the DB (charts, MDims, explorers, posts)."""
    from apps.inspector.gather import gather as gather_bundles

    bundles = gather_bundles(
        slugs=list(slug) or None,
        kinds=[t.lower() for t in content_types] or None,
        limit=limit,
    )
    if not bundles:
        raise click.ClickException("No content found for the given filters.")
    output_dir.mkdir(parents=True, exist_ok=True)
    for bundle in bundles:
        bundle.save(output_dir / f"{bundle.kind}{BUNDLE_FILENAME_SEPARATOR}{bundle.slug}.json")
    rprint(f"[green]✓ Wrote {len(bundles)} bundle(s) to {output_dir}[/green]")
    for bundle in bundles:
        rprint(
            f"  {bundle.kind}: [bold]{bundle.slug}[/bold] "
            f"({len(bundle.views)} view(s), {len(bundle.indicators)} indicator(s))"
        )


@cli.command()
@click.argument("bundle_dir", type=click.Path(exists=True, file_okay=False, path_type=Path))
@click.option(
    "--output",
    "-o",
    type=click.Path(path_type=Path),
    default=None,
    help="Findings JSON output path (default: <bundle_dir>/../lint_findings.json).",
)
def lint(bundle_dir: Path, output: Path | None) -> None:
    """Run the deterministic checks (codespell + rules) over gathered bundles. No LLM, no cost."""
    from apps.inspector.lint import lint as lint_bundles

    bundles = _load_bundles(bundle_dir)
    findings = lint_bundles(bundles)
    output = output or bundle_dir.parent / "lint_findings.json"
    output.write_text(json.dumps(findings, indent=1, ensure_ascii=False))
    rprint(f"[green]✓ {len(findings)} lint finding(s) written to {output}[/green]")


@cli.command()
@click.argument("findings_files", type=click.Path(exists=True, dir_okay=False, path_type=Path), nargs=-1, required=True)
@click.option(
    "--bundles",
    "-b",
    "bundle_dir",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    default=None,
    help="Bundle dir of this run; open findings on these objects that were not found again are marked fixed.",
)
def store(findings_files: tuple[Path, ...], bundle_dir: Path | None) -> None:
    """Store findings in the `inspections` DB table (created on demand; staging-only)."""
    from apps.inspector.store import load_findings_file, store_findings

    findings = []
    for path in findings_files:
        findings.extend(load_findings_file(path))

    inspected = None
    if bundle_dir:
        inspected = [(b.kind, b.slug) for b in _load_bundles(bundle_dir)]

    counts = store_findings(findings, inspected=inspected)
    rprint(
        f"[green]✓ Stored findings on {config.OWID_ENV.name} DB:[/green] "
        f"{counts['new']} new, {counts['seen_again']} seen again, {counts['reopened']} reopened, "
        f"{counts['still_dismissed']} still dismissed, {counts['marked_fixed']} marked fixed."
    )


@cli.command(name="list")
@click.option("--status", default="open", help="Filter by status (open/dismissed/fixed). Empty for all.")
@click.option("--type", "-t", "content_type", default=None, help="Filter by content type.")
@click.option("--slug", "-s", default=None, help="Filter by slug.")
def list_command(status: str, content_type: str | None, slug: str | None) -> None:
    """List stored findings."""
    from apps.inspector.store import list_findings

    df = list_findings(status=status or None, content_type=content_type, slug=slug)
    if df.empty:
        rprint("[green]No findings.[/green]")
        return
    severity_colors = {"high": "red", "medium": "yellow", "low": "dim"}
    for row in df.to_dict("records"):
        color = severity_colors.get(row["severity"], "white")
        rprint(
            f"[bold]{row['fingerprint'][:8]}[/bold] [{color}]{row['severity']}[/{color}] {row['category']} "
            f"({row['contentType']}:{row['slug']} · {row['field']})\n"
            f"    {row['explanation']}\n"
            f"    [dim]{row['url'] or ''}[/dim]"
        )
    rprint(f"\n[cyan]{len(df)} finding(s).[/cyan]")


@cli.command()
@click.argument("fingerprints", nargs=-1, required=True)
@click.option("--reason", "-r", required=True, help="Why this finding is a false positive or acceptable.")
@click.option("--by", default=None, help="Who dismisses (defaults to the system user).")
def dismiss(fingerprints: tuple[str, ...], reason: str, by: str | None) -> None:
    """Dismiss findings by fingerprint (prefixes accepted). Suppressed until the text changes."""
    import getpass

    from apps.inspector.store import dismiss_findings

    dismissed = dismiss_findings(list(fingerprints), reason=reason, dismissed_by=by or getpass.getuser())
    rprint(f"[green]✓ Dismissed {dismissed} finding(s).[/green]")
