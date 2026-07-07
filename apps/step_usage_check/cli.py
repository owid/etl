"""CLI to catch *silent* breakage in downstream steps after an update.

When you bump a foundational dataset (income groups, regions, population, WDI, ...) and repoint
its consumers, most steps still build without error — but some quietly lose data: a region whose
aggregate can no longer be computed becomes NaN, a country that was reclassified disappears, a
join that used to match now drops rows. Nothing raises, the feather is written, and the gap only
surfaces weeks later on a chart.

This command rebuilds every downstream consumer of an updated step and coverage-diffs each one
against the build that was on disk *before* the rebuild, flagging:

- **build failures** — the consumer no longer runs against the new upstream;
- **dropped tables / columns / entities** — present before the rebuild, gone after;
- **all-NaN columns** and **entities with no data** — present but silently emptied (no baseline
  needed, so this fires even on a first local build).

It works for any dataset, not just regional aggregations: the checks are generic coverage
invariants over the catalog output of each `data://` consumer. `grapher://` and `export://`
consumers are listed but skipped (they upsert to MySQL / have no catalog output).

    etl usage-check wb/2026-07-01/income_groups          # check direct consumers of the update
    etl usage-check wb/2026-07-01/income_groups --all    # check the full transitive downstream
    etl usage-check data://garden/wb/2026-07-01/income_groups --dry-run   # just list consumers
"""

import traceback
from dataclasses import dataclass, field
from pathlib import Path

import rich_click as click
import structlog
from owid import catalog
from rich.console import Console

from etl import config, paths
from etl.command import main as etl_main
from etl.version_tracker import VersionTracker

log = structlog.get_logger()
console = Console()

# Index level names we treat as "entities" (countries / regions). Coverage of these is the most
# common silent-null casualty, so we diff them explicitly when the level is present.
ENTITY_LEVELS = ("country", "countries", "entity", "location")

# Per-entity emptiness scan groups by entity, which is O(rows). Skip it above this size to keep the
# check tractable on very large tables — column-level and set-diff checks still run.
MAX_ROWS_FOR_ENTITY_SCAN = 5_000_000

# Only these schemes produce a catalog dataset on disk that we can coverage-diff.
CATALOG_SCHEMES = ("data://", "data-private://")


@dataclass
class TableCoverage:
    """Coverage summary of a single table, cheap enough to hold in memory across a rebuild."""

    columns: dict[str, int]  # value column -> non-null count
    n_rows: int
    entities: set[str] | None = None  # entity values, or None if the table has no entity level
    empty_entities: set[str] = field(default_factory=set)  # entities present but all-NaN


@dataclass
class Finding:
    consumer: str
    severity: str  # "problem" | "warning"
    message: str


def _dest_dir(step: str) -> Path:
    """Map a `data(-private)://channel/namespace/version/short` step to its on-disk dataset dir."""
    path = step.split("://", 1)[1]
    return paths.DATA_DIR / path


def _is_catalog_step(step: str) -> bool:
    return step.startswith(CATALOG_SCHEMES)


def _load_coverage(step: str) -> dict[str, TableCoverage] | None:
    """Summarise the on-disk build of `step`, or return None if it isn't built yet."""
    dest = _dest_dir(step)
    if not (dest / "index.json").exists():
        return None
    ds = catalog.Dataset(dest.as_posix())
    coverage: dict[str, TableCoverage] = {}
    for name in ds.table_names:
        # reset_index=False keeps dims in the index so `tb.columns` is exactly the value columns;
        # safe_types=False avoids expensive categorical->object conversions on large tables.
        tb = ds.read(name, reset_index=False, safe_types=False)
        columns = {col: int(tb[col].notna().sum()) for col in tb.columns}

        entities: set[str] | None = None
        empty_entities: set[str] = set()
        entity_level = next((lvl for lvl in tb.index.names if lvl in ENTITY_LEVELS), None)
        if entity_level is not None:
            entities = set(tb.index.get_level_values(entity_level).astype(str))
            if len(tb) <= MAX_ROWS_FOR_ENTITY_SCAN and len(tb.columns) > 0:
                # An entity "has data" if any value column is non-null in any of its rows.
                has_data = tb.notna().any(axis=1).groupby(level=entity_level, observed=True).any()
                empty_entities = {str(e) for e in has_data.index[~has_data.to_numpy()]}

        coverage[name] = TableCoverage(
            columns=columns,
            n_rows=len(tb),
            entities=entities,
            empty_entities=empty_entities,
        )
    return coverage


def _rebuild(step: str) -> tuple[bool, str]:
    """Rebuild a single consumer in-process. Returns (ok, error_traceback).

    Rebuilds genuinely against the local upstream — never downloads the pre-update build from the
    catalog (that would defeat the whole check), so `PREFER_DOWNLOAD` is forced off here.
    """
    config.PREFER_DOWNLOAD = False
    # Fast path: force just this step, reusing deps already on disk. If a dep is missing, fall back
    # to letting etl resolve/build the chain (no force, so change-detection keeps it minimal).
    try:
        etl_main(includes=[step], exact_match=True, private=True, force=True, only=True)
        return True, ""
    except FileNotFoundError:
        log.info("usage_check.deps_missing_on_disk.resolving_chain", step=step)
        try:
            etl_main(includes=[step], exact_match=True, private=True, force=False, only=False)
            return True, ""
        except Exception:
            return False, traceback.format_exc()
    except Exception:
        return False, traceback.format_exc()


def _diff_coverage(
    consumer: str,
    before: dict[str, TableCoverage] | None,
    after: dict[str, TableCoverage],
) -> list[Finding]:
    """Compare before/after coverage of one consumer and emit findings."""
    findings: list[Finding] = []

    for table_name, cov in after.items():
        # --- absolute checks (no baseline needed) ---
        all_nan = sorted(col for col, n in cov.columns.items() if n == 0)
        if all_nan:
            findings.append(Finding(consumer, "problem", f"table {table_name!r}: all-NaN column(s) {all_nan}"))
        if cov.empty_entities:
            sample = sorted(cov.empty_entities)
            shown = sample[:10]
            more = f" (+{len(sample) - len(shown)} more)" if len(sample) > len(shown) else ""
            findings.append(
                Finding(
                    consumer,
                    "problem",
                    f"table {table_name!r}: {len(sample)} entity(ies) present but all-NaN: {shown}{more}",
                )
            )

        # --- comparative checks (need a pre-rebuild baseline) ---
        if before is None or table_name not in before:
            continue
        prev = before[table_name]

        dropped_cols = sorted(set(prev.columns) - set(cov.columns))
        if dropped_cols:
            findings.append(Finding(consumer, "problem", f"table {table_name!r}: column(s) disappeared {dropped_cols}"))

        if prev.entities is not None and cov.entities is not None:
            dropped_entities = sorted(prev.entities - cov.entities)
            if dropped_entities:
                shown = dropped_entities[:10]
                more = f" (+{len(dropped_entities) - len(shown)} more)" if len(dropped_entities) > len(shown) else ""
                findings.append(
                    Finding(
                        consumer,
                        "problem",
                        f"table {table_name!r}: {len(dropped_entities)} entity(ies) disappeared: {shown}{more}",
                    )
                )

        # Soft signal: a column that survived but lost a big chunk of its non-null values.
        for col, n_after in cov.columns.items():
            n_before = prev.columns.get(col)
            if n_before and n_after > 0 and n_after < n_before * 0.9:
                pct = 100 * (n_before - n_after) / n_before
                findings.append(
                    Finding(
                        consumer,
                        "warning",
                        f"table {table_name!r}: column {col!r} lost {pct:.0f}% of its values "
                        f"({n_before} -> {n_after} non-null)",
                    )
                )

    # Whole tables that vanished.
    if before is not None:
        for table_name in sorted(set(before) - set(after)):
            findings.append(Finding(consumer, "problem", f"table {table_name!r} disappeared entirely"))

    return findings


def _resolve_consumers(step_arg: str, all_usages: bool) -> tuple[list[str], list[str]]:
    """Resolve the updated step(s) and their downstream consumers.

    `step_arg` may be a full step URI or a path fragment (e.g. `wb/2026-07-01/income_groups`), in
    which case it matches every active step containing it (snapshot/meadow/garden/grapher of the
    dataset). Returns (matched_steps, consumers) with the matched steps themselves excluded from
    the consumer list.
    """
    vt = VersionTracker(connect_to_db=False, warn_on_unused=False)
    if "://" in step_arg:
        matched = [step_arg] if step_arg in vt.all_steps else []
    else:
        matched = sorted(s for s in vt.all_steps if step_arg in s)
    if not matched:
        raise click.ClickException(f"No active step in the DAG matches {step_arg!r}.")

    consumers: set[str] = set()
    for step in matched:
        usages = vt.get_all_step_usages(step) if all_usages else vt.get_direct_step_usages(step)
        consumers.update(usages)
    # A consumer that is itself one of the matched steps is not "downstream" — drop it.
    consumers.difference_update(matched)
    return matched, sorted(consumers)


@click.command(name="usage-check", cls=click.RichCommand)
@click.argument("step", type=str)
@click.option(
    "--all", "all_usages", is_flag=True, help="Check the full transitive downstream, not just direct consumers."
)
@click.option("--dry-run", is_flag=True, help="List the consumers that would be rebuilt, then stop.")
def cli(step: str, all_usages: bool, dry_run: bool) -> None:
    """Rebuild downstream consumers of an updated STEP and coverage-diff them to catch silent breakage.

    STEP is a step URI or a path fragment, e.g. `wb/2026-07-01/income_groups`.
    """
    matched, consumers = _resolve_consumers(step, all_usages)
    console.print(f"[bold]Updated step(s):[/bold] {', '.join(matched)}")

    catalog_consumers = [c for c in consumers if _is_catalog_step(c)]
    skipped = [c for c in consumers if not _is_catalog_step(c)]
    scope = "transitive" if all_usages else "direct"
    console.print(
        f"[bold]{len(catalog_consumers)}[/bold] {scope} catalog consumer(s) to rebuild"
        + (f", [dim]{len(skipped)} non-catalog consumer(s) skipped (grapher/export)[/dim]" if skipped else "")
    )

    if dry_run:
        for c in catalog_consumers:
            console.print(f"  • {c}")
        for c in skipped:
            console.print(f"  • [dim]{c} (skipped)[/dim]")
        return

    all_findings: list[Finding] = []
    build_failures: list[str] = []

    for i, consumer in enumerate(catalog_consumers, 1):
        console.print(f"\n[bold cyan]\\[{i}/{len(catalog_consumers)}][/bold cyan] {consumer}")
        before = _load_coverage(consumer)
        if before is None:
            console.print("  [dim]no local build to diff against — only absolute checks will apply[/dim]")

        ok, err = _rebuild(consumer)
        if not ok:
            build_failures.append(consumer)
            console.print("  [red]BUILD FAILED[/red]")
            # Show the last line of the traceback inline; full trace is in the logs above.
            last = err.strip().splitlines()[-1] if err.strip() else ""
            console.print(f"    [red]{last}[/red]")
            continue

        after = _load_coverage(consumer)
        if after is None:
            console.print("  [yellow]built but produced no catalog output to check[/yellow]")
            continue

        findings = _diff_coverage(consumer, before, after)
        all_findings.extend(findings)
        problems = [f for f in findings if f.severity == "problem"]
        warnings = [f for f in findings if f.severity == "warning"]
        if not problems and not warnings:
            console.print("  [green]OK[/green]")
        for f in problems:
            console.print(f"  [red]✗ {f.message}[/red]")
        for f in warnings:
            console.print(f"  [yellow]⚠ {f.message}[/yellow]")

    # ---- summary ----
    problems = [f for f in all_findings if f.severity == "problem"]
    warnings = [f for f in all_findings if f.severity == "warning"]
    console.print("\n[bold]Summary[/bold]")
    console.print(f"  consumers rebuilt: {len(catalog_consumers) - len(build_failures)}/{len(catalog_consumers)}")
    console.print(f"  build failures:    {len(build_failures)}")
    console.print(f"  coverage problems: {len(problems)}")
    console.print(f"  warnings:          {len(warnings)}")

    if build_failures or problems:
        console.print(
            "\n[red bold]Silent-breakage check FAILED.[/red bold] "
            "Investigate the steps above before merging — the update dropped data downstream."
        )
        raise SystemExit(1)
    console.print("\n[green bold]All downstream consumers build and keep their coverage.[/green bold]")
