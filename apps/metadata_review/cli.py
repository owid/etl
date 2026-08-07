"""CLI for the Metadata Review tool.

    etl metadata-review export <target> [-o out.yml] [--status open]
    etl metadata-review resolve <id> --status implemented

`<target>` is an MDim catalogPath ('ns/version/short#short'), an MDim slug, or a
grapher dataset catalogPath ('ns/version/dataset', 'grapher/' prefix optional).

The export runs in the repo so every suggestion is traced to a concrete edit
location (garden .meta.yml key, MDim config .yml key, or the step .py when the
value is generated programmatically). Point it at the environment where the
suggestions were filed: `STAGING=<branch> etl metadata-review export ...` for a
branch's staging server, or the default env from your `.env`.
"""

from pathlib import Path

import rich_click as click
from rich.console import Console
from sqlalchemy.orm import Session

import etl.grapher.model as gm
from apps.metadata_review.export import build_export, export_to_yaml
from etl import config
from etl.paths import BASE_DIR

console = Console()

STATUSES = ["open", "implemented", "rejected"]


@click.group(name="metadata-review", help=__doc__)
def cli() -> None:
    pass


@cli.command(name="export")
@click.argument("target")
@click.option(
    "--output",
    "-o",
    type=click.Path(dir_okay=False, path_type=Path),
    default=None,
    help="Output YAML path. Defaults to ai/metadata_review_<slug>.yml.",
)
@click.option(
    "--status",
    "statuses",
    type=click.Choice(STATUSES),
    multiple=True,
    help="Only export suggestions with these statuses (default: all).",
)
def export_cli(target: str, output: Path | None, statuses: tuple[str, ...]) -> None:
    """Export suggestions for TARGET to the YAML handoff consumed by Claude."""
    with Session(config.OWID_ENV.engine) as session:
        document = build_export(session, target, statuses=list(statuses) or None)

    if output is None:
        slug = (document["metadata_review_export"]["target"].get("slug") or target).replace("/", "_").replace("#", "_")
        output = BASE_DIR / "ai" / f"metadata_review_{slug}.yml"
    path = export_to_yaml(document, output)
    n = document["metadata_review_export"]["n_suggestions"]
    console.print(f"[green]Exported {n} suggestion(s) to[/green] {path}")
    if n == 0:
        console.print(
            f"[yellow]No suggestions matched — is the environment right? (env: {config.OWID_ENV.name})[/yellow]"
        )


@cli.command(name="resolve")
@click.argument("suggestion_id", type=int)
@click.option("--status", type=click.Choice(STATUSES), required=True, help="New status.")
def resolve_cli(suggestion_id: int, status: str) -> None:
    """Set the resolution status of one suggestion (terminal-side alternative to the wizard app)."""
    if not config.GRAPHER_USER_ID:
        raise click.ClickException("GRAPHER_USER_ID must be set in your .env to resolve suggestions from the CLI.")
    with Session(config.OWID_ENV.engine) as session:
        suggestion = session.get(gm.MetadataReviewSuggestion, suggestion_id)
        if suggestion is None:
            raise click.ClickException(f"No suggestion with id {suggestion_id}.")
        try:
            suggestion.set_status(session, status, user_id=int(config.GRAPHER_USER_ID))  # type: ignore[arg-type]
        except ValueError as e:
            raise click.ClickException(str(e))
    console.print(f"[green]Suggestion {suggestion_id} set to {status}.[/green]")
