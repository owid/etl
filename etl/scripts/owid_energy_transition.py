"""Runbook for the energy Total-Energy-Supply transition: redirects, banners and archives.

The Energy Institute's Statistical Review switched from the substitution method to total energy
supply. Our energy charts move to three multidims (energy mix, electricity mix, fossil fuels), the
old charts are redirected or retired, and the charts whose *values* change get a deprecation banner
first. This script is the executable half of that: it performs the steps that can be automated,
verifies the ones that cannot, and refuses to let a step run before its prerequisites hold.

The steps are ordered. Run `check` before and after every step; it is cheap and read-only.

    .venv/bin/python etl/scripts/owid_energy_transition.py check
    .venv/bin/python etl/scripts/owid_energy_transition.py <step>            # dry run
    .venv/bin/python etl/scripts/owid_energy_transition.py <step> --apply    # writes

Everything targets **production** via ``ENV_FILE_PROD``. Nothing here reads or prints credentials;
the env file supplies them. Two capabilities are needed and are checked by ``check``:

  * read/write on the prod grapher DB (``etl_grapher`` has ``ALL PRIVILEGES`` on ``live_grapher``);
  * ``ADMIN_API_KEY`` in the prod env file, because chart edits must go through the admin API
    rather than raw SQL. Writing ``chart_configs`` directly skips the R2 config sync and the
    revision history, which silently desynchronises what readers are served from what the DB says.

## Why some steps are manual

Four things have no scriptable path and are listed as manual steps below:

  1. publishing the three multidims with their slugs (admin UI);
  2. featured metrics (the multidim views have to be picked by hand);
  3. re-creating narrative charts that were built on a retired chart;
  4. replacing chart and explorer links inside gdoc articles.

## The one guardrail that will bite you

Grapher's admin rejects a multidim redirect whose source is already the *target* of an old slug
("would form a redirect chain"), and a good number of our charts are in that position. The admin's
bulk endpoint therefore cannot be used for them; ``createMultiDimRedirectsFromCsv`` exists for
exactly this case. ``check`` prints the affected charts so the count is never guessed.

## Order of operations, and why

  0. check                     prerequisites, and the chain-conflict list
  1. publish-multidims         MANUAL. Nothing else works first: every redirect target is
                               /grapher/<mdim-slug>, and both mapping skills read *published* views.
  2. banners                   set the deprecation notice on the methodology-affected charts. Must
                               happen while they are still published, and before the archival bake,
                               because only published charts are archived.
  3. archival-bake             MANUAL trigger, so the frozen copies carry the banner.
  4. explorer-redirects        the 38 fossil-fuel explorer views, via the admin bulk endpoint.
  5. archive-redirects         the two charts with no successor, to their frozen copies.
  6. chart-redirects           the settled chart -> multidim redirects, via the grapher CLI.
  7. references                MANUAL. gdoc links, narrative charts, featured metrics.
  8. retire-banners            after the cutover date: clear the banners and redirect those charts.

Steps 5 and 6 are independent of the banner timeline; steps 2 and 8 bracket it.
"""

import subprocess
from dataclasses import dataclass
from pathlib import Path

import click
import pandas as pd
import sqlalchemy as sa
from dotenv import dotenv_values
from rich_click.rich_command import RichCommand
from structlog import get_logger

from etl import config
from etl.config import OWIDEnv

log = get_logger()

DATA_DIR = Path(__file__).parent / "owid_energy_transition"
# The settled redirects: source path -> target path, one row per redirect. Produced from the
# migration review tool; kept here so this script is reproducible without it.
REDIRECTS_CSV = DATA_DIR / "redirects.csv"
# The charts whose values change because of the methodology switch, so they get a banner first.
AFFECTED_CSV = DATA_DIR / "methodology_affected_charts.csv"

# Slugs the three multidims must be published under. Every redirect target depends on these.
MDIM_SLUGS = {
    "energy/latest/energy_mix#energy_mix": "energy-mix",
    "energy/latest/electricity_mix#electricity_mix": "electricity-mix",
    "energy/latest/fossil_fuels#fossil_fuels": "fossil-fuels",
}

# Charts with no successor indicator: neither quantity exists under total energy supply, so both
# retire to the frozen archive snapshot taken before the 2026 release.
ARCHIVE_REDIRECTS = {
    "/grapher/thermal-efficiency-factor": "https://archive.ourworldindata.org/20260518-083815/grapher/thermal-efficiency-factor.html",
    "/grapher/renewable-nuclear-direct-substitution": "https://archive.ourworldindata.org/20260518-090244/grapher/renewable-nuclear-direct-substitution.html",
}

EXPLAINER_URL = "https://ourworldindata.org/how-primary-energy-is-measured-has-changed-across-our-charts"
# The date the bannered charts are redirected to their successors. Spelled out with the year,
# because an archived page keeps this text indefinitely and "October 1st" alone would age badly.
CUTOVER_DATE = "October 1, 2026"
# Markdown, rendered inside the chart frame above the title, so it is read before the chart. Kept to
# two sentences carrying three links, because the banner competes with the title for space.
#
# "The old version" rather than "the current chart": after the first sentence introduces an updated
# version, "current" reads as though it were the new one. And the archive is not framed as temporary,
# because after the cutover it is the only place these values exist, the slug having redirected away.
BANNER_TEMPLATE = (
    f"On {CUTOVER_DATE} this chart will be replaced by "
    "[an updated version]({successor_url}) that uses "
    f"[a different methodology]({EXPLAINER_URL}). "
    "The old version will remain available in [the archive]({archive_url})."
)


@dataclass
class Step:
    name: str
    manual: bool
    instructions: str


STEPS: list[Step] = [
    Step(
        name="publish-multidims",
        manual=True,
        instructions="""
        In the admin, set `slug` and `published` on each multidim:
            energy/latest/energy_mix#energy_mix            -> energy-mix
            energy/latest/electricity_mix#electricity_mix  -> electricity-mix
            energy/latest/fossil_fuels#fossil_fuels        -> fossil-fuels
        Nothing else in this runbook works until all three are published: every redirect target is a
        /grapher/<mdim-slug> path, and both mapping skills read the published views out of
        multi_dim_data_pages.config.
        """,
    ),
    Step(
        name="banners",
        manual=False,
        instructions="""
        Sets the deprecation notice on the methodology-affected charts (see
        methodology_affected_charts.csv), through the admin API so the R2 config and the revision
        history stay in sync.

        Requires the owid-grapher deprecation-notice feature to be deployed to production.

        Do this BEFORE retiring anything: only published charts are archived, so a banner set after
        a chart is unpublished can never reach its frozen copy.
        """,
    ),
    Step(
        name="archival-bake",
        manual=True,
        instructions="""
        Trigger an archival bake so the frozen copies carry the banner.

        Needed because the baker decides what to re-snapshot from a hash over the chart config plus
        the indicator checksums. Depending on how the notice is stored it may not change that hash,
        in which case no new snapshot is made and the archive keeps its pre-banner version. Verify
        afterwards that archived_chart_versions has a row newer than the banner for each affected
        chart; `check` reports this.
        """,
    ),
    Step(
        name="explorer-redirects",
        manual=True,
        instructions="""
        The 38 fossil-fuel explorer views (the energy explorer's views are handled chart by chart).

            1. Run `/map-explorer-to-mdim` to get one redirect-payload JSON per explorer.
            2. Paste each JSON into "Bulk-create redirects from JSON" at
               /admin/multi-dim-redirects.

        Rehearsable on staging, executed in production. The admin's bulk endpoint accepts explorer
        sources only, which is why chart redirects take a different route (see chart-redirects).

        An explorer that already has entries under /admin/site-redirects needs those deleted first,
        or they conflict with the new ones. Neither of our two explorers has any, and `check`
        confirms that before this step runs.
        """,
    ),
    Step(
        name="archive-redirects",
        manual=False,
        instructions="""
        Creates the two site redirects to the frozen archive copies.

        Site redirects are baked into _redirects unconditionally, unlike chart and multidim
        redirects which are only consulted when the URL 404s. So these take effect on the next bake
        even while the charts are still published, and there is no window where the URL breaks.

        Unpublish both charts afterwards so they stop appearing in listings and search. That is
        housekeeping, not a prerequisite.
        """,
    ),
    Step(
        name="chart-redirects",
        manual=True,
        instructions="""
        The settled chart -> multidim redirects.

            1. Run `/map-charts-to-mdim`. It matches charts to views by indicator id and audits
               every article, explorer, narrative chart, data insight and static viz that links or
               embeds each chart, with the replacement URL.
            2. Take the `;`-delimited CSV it produces.
            3. From the owid-grapher repo, against production:
                   yarn tsx devTools/createMultiDimRedirectsFromCsv.ts <csv> --dry-run
                   yarn tsx devTools/createMultiDimRedirectsFromCsv.ts <csv> --user-id <id>

        The CLI creates the redirects AND unpublishes the source charts in one transaction, and
        rolls back on --dry-run.

        Do not use the admin UI for these: it rejects any source that is already the target of an
        old slug, and 22 of our charts are. `check` lists them.
        """,
    ),
    Step(
        name="references",
        manual=True,
        instructions="""
        Everything that points at a retired chart, from the audit produced by `/map-charts-to-mdim`:

            * replace chart and explorer links inside gdoc articles;
            * re-create narrative charts that were built on a retired chart (no automatic path);
            * redefine featured metrics at /admin/featured-metrics (the multidim views have to be
              added by hand).
        """,
    ),
    Step(
        name="retire-banners",
        manual=False,
        instructions="""
        After the cutover date: clears the deprecation notices, then the bannered charts are
        redirected via the same chart-redirects route.

        Clearing the notice does not remove it from archived snapshots, which is intended: each
        snapshot is a faithful record of what the page said at that time.
        """,
    ),
]

STEPS_BY_NAME = {s.name: s for s in STEPS}


def _env() -> OWIDEnv:
    return OWIDEnv.from_env_file(str(config.ENV_FILE_PROD))


def _admin_api_key_is_set() -> bool:
    """Whether the prod env file supplies an admin API key. The value is never read or logged."""
    return bool(dotenv_values(str(config.ENV_FILE_PROD)).get("ADMIN_API_KEY") or config.ADMIN_API_KEY)


def _read(env: OWIDEnv, sql: str, **params) -> pd.DataFrame:
    """Query with pymysql paramstyle. A tuple parameter expands into an IN list."""
    return env.read_sql(sql, params=params or None)


def _column_exists(env: OWIDEnv, table: str, column: str) -> bool:
    return not _read(
        env,
        """SELECT 1 FROM information_schema.columns
           WHERE table_schema = DATABASE() AND table_name = %(t)s AND column_name = %(c)s""",
        t=table,
        c=column,
    ).empty


def check(env: OWIDEnv) -> list[str]:
    """Read-only prerequisites and guardrails. Returns a list of problems, empty if all clear."""
    problems: list[str] = []
    redirects = pd.read_csv(REDIRECTS_CSV)
    affected = pd.read_csv(AFFECTED_CSV)

    # -- Capabilities -----------------------------------------------------------------------
    with env.get_engine().connect() as con:
        grants = [r[0] for r in con.execute(sa.text("SHOW GRANTS FOR CURRENT_USER()"))]
    if not any("ALL PRIVILEGES" in g or "INSERT" in g for g in grants):
        problems.append("the prod DB user cannot write; chart and redirect steps will fail")
    if not _admin_api_key_is_set():
        problems.append(
            "ADMIN_API_KEY is not set for production. Chart edits must go through the admin API, "
            "not raw SQL, or the R2 config and revision history desynchronise."
        )

    # -- Step 1: multidims published --------------------------------------------------------
    published = _read(
        env,
        "SELECT catalogPath, slug, published FROM multi_dim_data_pages WHERE catalogPath IN %(paths)s",
        paths=tuple(MDIM_SLUGS),
    )
    for path, want in MDIM_SLUGS.items():
        row = published[published["catalogPath"] == path]
        if row.empty:
            problems.append(f"multidim {path} does not exist in production yet")
        elif row["slug"].iloc[0] != want or not row["published"].iloc[0]:
            problems.append(
                f"multidim {path} is slug={row['slug'].iloc[0]!r} published={row['published'].iloc[0]}; "
                f"expected slug={want!r} published=1"
            )

    # -- The chain guardrail ----------------------------------------------------------------
    # Grapher's admin refuses a redirect whose source is already the target of an old slug. Those
    # charts have to go through createMultiDimRedirectsFromCsv instead.
    slugs = sorted(
        {s.removeprefix("/grapher/") for s in redirects["source"] if s.startswith("/grapher/")} | set(affected["slug"])
    )
    chained = _read(
        env,
        """SELECT cc.slug, COUNT(*) AS old_slugs
           FROM chart_slug_redirects csr
           JOIN charts c ON csr.chart_id = c.id
           JOIN chart_configs cc ON c.configId = cc.id
           WHERE cc.slug IN %(slugs)s
           GROUP BY cc.slug ORDER BY old_slugs DESC""",
        slugs=tuple(slugs),
    )
    if len(chained):
        log.info(
            "admin_would_reject_these_use_the_cli",
            n_charts=len(chained),
            n_old_slugs=int(chained["old_slugs"].sum()),
            charts=chained["slug"].tolist(),
        )

    # -- Redirect sanity, before anything is written ----------------------------------------
    if not redirects["source"].is_unique:
        dupes = redirects["source"][redirects["source"].duplicated()].tolist()
        problems.append(f"duplicate redirect sources in {REDIRECTS_CSV.name}: {dupes}")
    site = _read(env, "SELECT source, target FROM redirects")
    clash = set(redirects["source"]) & set(site["source"])
    if clash:
        problems.append(f"{len(clash)} redirect sources already exist as site redirects: {sorted(clash)[:5]}")
    chain = set(redirects["source"]) & set(site["target"])
    if chain:
        problems.append(f"{len(chain)} redirect sources are already site-redirect targets (chain): {sorted(chain)[:5]}")
    # A real target is either a site-relative path or an absolute URL; anything else is a leftover
    # placeholder from when the plan was being drafted.
    unresolved = redirects[~redirects["target"].str.match(r"^(/|https?://)", na=False)]
    if len(unresolved):
        problems.append(
            f"{len(unresolved)} redirects still have placeholder targets: {unresolved['source'].tolist()[:5]}"
        )

    # -- Step 2/3: banners, and whether the archive caught them -----------------------------
    # The column only exists once owid-grapher's deprecation-notice migration is deployed. Until
    # then the banner steps cannot run, and their checks are skipped rather than failing.
    if not _column_exists(env, "charts", "deprecationNotice"):
        log.info(
            "banner_feature_not_deployed",
            detail="charts.deprecationNotice does not exist in production yet; banner steps are blocked",
        )
        return problems

    notices = _read(
        env,
        """SELECT cc.slug, c.deprecationNotice IS NOT NULL AS has_notice, c.updatedAt
           FROM charts c JOIN chart_configs cc ON c.configId = cc.id
           WHERE cc.slug IN %(slugs)s""",
        slugs=tuple(affected["slug"]),
    )
    missing = sorted(set(affected["slug"]) - set(notices["slug"]))
    if missing:
        problems.append(f"{len(missing)} methodology-affected charts not found in production: {missing[:5]}")
    without = notices[~notices["has_notice"].astype(bool)]["slug"].tolist()
    log.info("banners", set_on=int(notices["has_notice"].astype(bool).sum()), of=len(affected))
    if without and len(without) < len(affected):
        log.info("banners_missing", charts=without[:10])

    # A banner only reaches the archive if a snapshot was taken after it was set.
    if len(notices) and notices["has_notice"].astype(bool).any():
        stale = _read(
            env,
            """SELECT cc.slug, c.updatedAt, MAX(acv.archivalTimestamp) AS latest_archive
               FROM charts c
               JOIN chart_configs cc ON c.configId = cc.id
               LEFT JOIN archived_chart_versions acv ON acv.grapherId = c.id
               WHERE cc.slug IN %(slugs)s AND c.deprecationNotice IS NOT NULL
               GROUP BY cc.slug, c.updatedAt
               HAVING latest_archive IS NULL OR latest_archive < c.updatedAt""",
            slugs=tuple(affected["slug"]),
        )
        if len(stale):
            problems.append(
                f"{len(stale)} bannered charts have no archive snapshot newer than the banner; "
                f"run an archival bake before retiring them (e.g. {stale['slug'].tolist()[:3]})"
            )

    # -- Step 5: the two archive redirects --------------------------------------------------
    for source, target in ARCHIVE_REDIRECTS.items():
        row = site[site["source"] == source]
        if row.empty:
            log.info("archive_redirect_missing", source=source)
        elif row["target"].iloc[0] != target:
            problems.append(f"{source} redirects to {row['target'].iloc[0]}, expected the frozen copy")

    # -- Step 6/8: nothing left published without a redirect --------------------------------
    still = _read(
        env,
        """SELECT cc.slug FROM charts c JOIN chart_configs cc ON c.configId = cc.id
           WHERE cc.slug IN %(slugs)s AND cc.full ->> "$.isPublished" = "true" """,
        slugs=tuple(slugs),
    )
    redirected = _read(
        env,
        """SELECT DISTINCT SUBSTRING_INDEX(source, '/grapher/', -1) AS slug
           FROM multi_dim_redirects WHERE source LIKE '/grapher/%'
           UNION SELECT SUBSTRING_INDEX(source, '/grapher/', -1) FROM redirects WHERE source LIKE '/grapher/%'""",
    )
    orphans = sorted(set(still["slug"]) - set(redirected["slug"]))
    log.info("charts_in_scope", total=len(slugs), still_published=len(still), without_a_redirect=len(orphans))

    return problems


def _put_deprecation_notice(env: OWIDEnv, chart_id: int, config_full: dict, notice: str | None) -> None:
    """Write the notice via the admin chart endpoint, which carries it as a query parameter.

    Deliberately not raw SQL: the endpoint also refreshes the R2 config and records a revision, and
    an UPDATE straight into the DB would leave both stale. The chart config is sent back unchanged;
    only the query parameter differs.
    """
    from apps.chart_sync.admin_api import TIMEOUT, AdminAPI
    from etl.http import session as http_session

    api = AdminAPI(env)
    resp = http_session.put(
        f"{env.admin_api}/charts/{chart_id}",
        headers=api._headers(),
        json=config_full,
        params={"deprecationNotice": notice or ""},
        timeout=TIMEOUT,
    )
    resp.raise_for_status()
    body = resp.json()
    if not body.get("success"):
        raise click.ClickException(f"chart {chart_id}: {body}")


def banner_targets() -> pd.DataFrame:
    """The charts that get a banner, with the successor each one points at.

    Only the charts being retired: the five that are updated in place keep their slug, so their
    successor is themselves and a banner would be nonsense. Their FAUST carries the change instead.
    """
    affected = pd.read_csv(AFFECTED_CSV)
    return affected[affected["action"] != "update"].reset_index(drop=True)


def set_banners(env: OWIDEnv, dry_run: bool, clear: bool = False) -> None:
    """Set (or clear) the deprecation notice on the charts being retired."""
    import json

    targets = banner_targets()
    ids = _read(
        env,
        """SELECT cc.slug, c.id, cc.patch FROM charts c JOIN chart_configs cc ON c.configId = cc.id
           WHERE cc.slug IN %(slugs)s""",
        slugs=tuple(targets["slug"]),
    ).set_index("slug")
    for row in targets.itertuples():
        if row.slug not in ids.index:
            log.warning("chart_not_in_production", slug=row.slug)
            continue
        if not clear and not row.archive:
            # Without an archived copy the banner would promise a version that does not exist.
            log.error("no_archive_snapshot_skipping", slug=row.slug)
            continue
        notice = None if clear else BANNER_TEMPLATE.format(successor_url=row.successor, archive_url=row.archive)
        log.info("banner", slug=row.slug, action="clear" if clear else "set", successor=row.successor)
        if not dry_run:
            _put_deprecation_notice(env, int(ids.loc[row.slug, "id"]), json.loads(ids.loc[row.slug, "patch"]), notice)
    log.info(
        "banner_total", charts=len(targets), skipped_updated_in_place=len(pd.read_csv(AFFECTED_CSV)) - len(targets)
    )


def create_archive_redirects(env: OWIDEnv, dry_run: bool) -> None:
    """Point the two retired charts at their frozen archive copies."""
    from apps.chart_sync.admin_api import AdminAPI

    api = None if dry_run else AdminAPI(env)
    existing = set(_read(env, "SELECT source FROM redirects")["source"])
    for source, target in ARCHIVE_REDIRECTS.items():
        if source in existing:
            log.info("archive_redirect_exists", source=source)
            continue
        log.info("archive_redirect_create", source=source, target=target)
        if api is not None:
            api.create_site_redirect(source=source, target=target)


def run_chart_redirect_cli(csv_path: Path, dry_run: bool, grapher_repo: Path, user_id: int) -> None:
    """Drive grapher's createMultiDimRedirectsFromCsv, which the admin UI cannot replace."""
    cmd = ["yarn", "tsx", "devTools/createMultiDimRedirectsFromCsv.ts", str(csv_path)]
    cmd += ["--dry-run"] if dry_run else ["--user-id", str(user_id)]
    log.info("running", cmd=" ".join(cmd), cwd=str(grapher_repo))
    subprocess.run(cmd, cwd=grapher_repo, check=True)


@click.command(name="energy-transition", cls=RichCommand, help=__doc__)
@click.argument("step", type=click.Choice(["check", *STEPS_BY_NAME]))
@click.option("--apply", is_flag=True, help="Actually write. Without this the step only reports.")
@click.option("--csv", "csv_path", type=click.Path(exists=True, path_type=Path), help="CSV for chart-redirects.")
@click.option("--grapher-repo", type=click.Path(exists=True, path_type=Path), help="owid-grapher checkout.")
def cli(step: str, apply: bool, csv_path: Path | None, grapher_repo: Path | None) -> None:
    env = _env()
    dry_run = not apply

    if step != "check":
        entry = STEPS_BY_NAME[step]
        click.echo(click.style(f"\n{entry.name}", bold=True) + (" (manual)" if entry.manual else ""))
        click.echo(entry.instructions.rstrip())
        if entry.manual:
            click.echo("\nNothing to run: follow the instructions above, then re-run `check`.")
            return

    problems = check(env)
    for p in problems:
        log.error("check_failed", problem=p)
    if problems and step != "check":
        raise click.ClickException(f"{len(problems)} prerequisite(s) unmet; fix them before running {step}.")
    if step == "check":
        click.echo(click.style("\nall clear" if not problems else f"\n{len(problems)} problem(s)", bold=True))
        return

    if step == "banners":
        set_banners(env, dry_run=dry_run)
    elif step == "retire-banners":
        set_banners(env, dry_run=dry_run, clear=True)
    elif step == "archive-redirects":
        create_archive_redirects(env, dry_run=dry_run)
    elif step == "chart-redirects":
        if not (csv_path and grapher_repo):
            raise click.ClickException("chart-redirects needs --csv and --grapher-repo")
        run_chart_redirect_cli(csv_path, dry_run, grapher_repo, config.GRAPHER_USER_ID)  # type: ignore[arg-type]

    click.echo("\n" + ("dry run: nothing written. Re-run with --apply." if dry_run else "applied. Re-run `check`."))


if __name__ == "__main__":
    cli()
