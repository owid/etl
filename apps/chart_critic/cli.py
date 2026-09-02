"""Review published charts with an LLM and report what would mislead a reader.

**Examples**

```
# a handful of charts by name
etl chart-critic --slugs life-expectancy-vs-gdp-per-capita,share-people-fully-vaccinated-covid

# 20 charts sampled by readership (a chart with 10x the readers is 10x as likely)
etl chart-critic --sample 20

# the 50 most-viewed charts, with an HTML report
etl chart-critic --top 50 --output /tmp/critic.html

# cheapest tier, no renders, and see the bill before spending it
etl chart-critic --sample 100 --cheap --no-image --dry-run
```

Roughly $0.003 per chart with the render attached on the default model, so a pass over every
published chart is a few dollars. The binding constraint is not the bill — it is how many
findings a human will read, which is why an empty result is a first-class outcome here.
"""

from __future__ import annotations

import concurrent.futures as cf
import json
import re
import time
from pathlib import Path
from typing import Any

import rich_click as click
from rich.console import Console
from rich.table import Table

from apps.chart_critic import cache, digest, fixtures, mdim, report
from apps.chart_critic.bundle import GRAPHER_URL, Bundle, ChartGone, build, render
from apps.chart_critic.critic import (
    CHEAP_MODEL,
    DEFAULT_MODEL,
    FALLBACK_PRICES,
    build_agent,
    format_views,
    issue_params,
    prompt_parts,
)
from apps.utils.llms.costs import estimate_llm_cost
from etl.db import read_sql

console = Console()

SEVERITY_COLOR = {"high": "red", "medium": "yellow", "low": "cyan"}

# Grapher query parameters the model is allowed to put in a link. Anything else it invents is
# dropped rather than passed through into a URL a reviewer will click.
ALLOWED_PARAMS = {"country", "time", "tab", "region", "stackMode", "mapSelect", "facet", "yScale", "xScale"}
# Values are percent-encoded (mdim dimension slugs are not promised to be URL-safe), so "%" and
# "+" have to survive the filter or the encoded parameter is dropped and the link lands on the
# default view — the exact wrong-chart problem the extra keys exist to prevent.
SAFE_VALUE = re.compile(r"[A-Za-z0-9_~.\-+%]+")

# Charts below this get little enough traffic that a finding is rarely worth a ticket.
DEFAULT_MIN_VIEWS = 2000


def _pageviews(min_views: int) -> Any:
    """Published chart slugs with their annual pageviews, most-viewed first."""
    df = read_sql(
        """
        SELECT REPLACE(url, 'https://ourworldindata.org/grapher/', '') AS slug, views_365d
        FROM analytics_pageviews
        WHERE url LIKE 'https://ourworldindata.org/grapher/%%'
          AND day = (SELECT MAX(day) FROM analytics_pageviews)
          AND views_365d >= %(min_views)s
        ORDER BY views_365d DESC
        """,
        params={"min_views": min_views},
    )
    # Slugs with query strings or paths are chart *views*, not charts.
    return df[~df.slug.str.contains(r"[?#/]", regex=True)].drop_duplicates("slug").reset_index(drop=True)


def _expand_mdims(targets: list[tuple[str, int, str]], mdim_views: int, seed: int) -> list[tuple[str, int, str]]:
    """Replace each multi-dim slug with a sample of its views.

    One view of a multi-dim is a chart in its own right — same render, same metadata, same values
    — so reviewing only the default view leaves most of an mdim unreviewed. 40 mdims are
    published and some have dozens of views.
    """
    if mdim_views < 1:
        return targets
    expanded: list[tuple[str, int, str]] = []
    for slug, pageviews, params in targets:
        try:
            views = [] if params else mdim.sample_views(slug, mdim_views, seed=seed)
        except mdim.MdimLookupError as e:
            # The caller explicitly asked for N views. Reviewing the bare slug once and calling
            # that done would be a silent substitution of a different, weaker job.
            raise click.ClickException(f"--mdim-views was requested but {e}") from e
        if not views:
            expanded.append((slug, pageviews, params))
            continue
        for _, view_params in views:
            merged = "&".join(x for x in (params, view_params) if x)
            expanded.append((slug, pageviews, merged))
    return expanded


def _pool(min_views: int) -> Any:
    """Everything reviewable, with a weight: ordinary charts, and every view of every mdim.

    An mdim's traffic is recorded only against its base slug — analytics attributes **zero** views
    to query-string URLs — so per-view popularity does not exist. Rather than pretend otherwise,
    an mdim's pageviews are split evenly across its declared views. A multi-dim therefore carries
    the same total weight as a chart with the same readership, spread over the views it offers,
    and a 112-view mdim contributes many light candidates rather than one heavy one.
    """
    import pandas as pd

    charts = _pageviews(min_views)
    try:
        views_by_slug = mdim.all_published_views()
    except mdim.MdimLookupError as e:
        # Without the registry every mdim silently becomes an ordinary chart reviewed at its
        # default view, which is a different sample than the one asked for.
        raise click.ClickException(f"cannot build the review pool: {e}") from e

    rows = []
    for slug, base_views in zip(charts.slug, charts.views_365d):
        views = views_by_slug.get(slug)
        if views:
            share = base_views / len(views)
            rows += [{"slug": slug, "params": p, "views": int(base_views), "weight": share} for p in views]
        else:
            rows.append({"slug": slug, "params": "", "views": int(base_views), "weight": float(base_views)})
    return pd.DataFrame(rows)


def _select(
    slugs: str | None, sample: int | None, top: int | None, min_views: int, seed: int, params: str = ""
) -> list[tuple[str, int, str]]:
    if slugs:
        return [(s.strip(), 0, params) for s in slugs.split(",") if s.strip()]

    if top:
        df = _pageviews(min_views)
        if df.empty:
            raise click.ClickException("No pageview data found — is the database reachable?")
        df = df.head(top)
        return [(slug, int(v), params) for slug, v in zip(df.slug, df.views_365d)]

    if not sample:
        raise click.ClickException("Pass one of --slugs, --sample or --top")

    import numpy as np

    pool = _pool(min_views)
    if pool.empty:
        raise click.ClickException("No pageview data found — is the database reachable?")
    if sample > len(pool):
        raise click.ClickException(f"--sample {sample} exceeds the {len(pool)} candidates above {min_views} views")
    rng = np.random.default_rng(seed)
    idx = rng.choice(len(pool), size=sample, replace=False, p=pool.weight / pool.weight.sum())
    picked = pool.iloc[idx]
    return [(r.slug, int(r.views), r.params) for r in picked.itertuples()]


def _cost(model: str, input_tokens: int, output_tokens: int) -> float:
    """Price a call, falling back to published rates for models genai_prices does not know."""
    try:
        return estimate_llm_cost(model, input_tokens=input_tokens, output_tokens=output_tokens)
    except (KeyError, ValueError, LookupError):
        rate_in, rate_out = FALLBACK_PRICES.get(model, (0.0, 0.0))
        return input_tokens / 1e6 * rate_in + output_tokens / 1e6 * rate_out


def chart_link(slug: str, params: str = "", extra_keys: set[str] | None = None) -> str:
    """A chart URL, carrying only query parameters that grapher actually understands.

    ``extra_keys`` carries an mdim's own dimension names (``metric``, ``antigen``, …), which are
    per-mdim and so cannot be in a fixed allowlist. Without them a finding about one view of a
    multi-dim would link to the default view instead — a link to the wrong chart.
    """
    allowed = ALLOWED_PARAMS | (extra_keys or set())
    clean = []
    for pair in (params or "").lstrip("?").split("&"):
        key, _, value = pair.partition("=")
        key, value = key.strip(), value.strip()
        if key in allowed and value and SAFE_VALUE.fullmatch(value):
            clean.append(f"{key}={value}")
    return f"{GRAPHER_URL}/{slug}" + ("?" + "&".join(clean) if clean else "")


def _link_keys(slug: str, params: str) -> set[str]:
    """Extra query keys a finding's link may carry, for a chart that may be a multi-dim.

    Two sources, and the order matters. The keys already present on the reviewed target need no
    lookup: they were either enumerated from the mdim config or typed by the user, so they are
    trusted by construction. The registry is then consulted only as a bonus, for a dimension the
    model names that the target did not — and its failure is tolerated here, unlike in selection.

    That asymmetry is the point. ``--slugs`` is documented to work without a database, and a
    lookup that raises at *link* time would abort the command after every review had been paid
    for — so a run would succeed only when it found nothing.
    """
    keys = {pair.partition("=")[0].strip() for pair in (params or "").lstrip("?").split("&") if pair}
    try:
        return keys | mdim.dimension_keys(slug)
    except mdim.MdimLookupError:
        return keys


def _claim_tokens(claim: str) -> set[str]:
    # Crude singular/plural folding. Without it "the unit for all three indicators is incorrectly
    # set to 'doses'" and "the indicator unit is incorrectly set to 'doses'" scored below the
    # merge threshold and were reported as two findings.
    return {w.rstrip("s") for w in re.findall(r"[a-z0-9]{4,}", claim.lower())}


def _merge(issues: list[dict[str, Any]], new: dict[str, Any]) -> None:
    """Fold a finding into the list, merging it with the same finding from an earlier pass.

    The model rewords freely between passes — "life expectancy of around 18–20 years" and
    "under 20 years" are one finding — so matching on a prefix counts them separately. Overlap
    of the significant words is crude but survives the rewording.
    """
    tokens = _claim_tokens(new["claim"])
    for existing in issues:
        other = _claim_tokens(existing["claim"])
        overlap = len(tokens & other) / max(len(tokens | other), 1)
        if existing["kind"] == new["kind"] and overlap >= 0.4:
            existing["passes"] += 1
            return
    issues.append(new | {"passes": 1})


def _review_one(
    slug: str,
    pageviews: int,
    model: str,
    with_image: bool,
    dry_run: bool,
    repeat: int = 1,
    use_cache: bool = True,
    ttl_hours: float = cache.DEFAULT_TTL_HOURS,
    views: int = 1,
    params: str = "",
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "slug": slug,
        "views": pageviews,
        "params": params,
        "issues": [],
        "cost": 0.0,
        "status": "ok",
        "cached": 0,
    }
    try:
        bundle: Bundle = build(slug, with_image=with_image, use_cache=use_cache, ttl_hours=ttl_hours, params=params)
    except ChartGone:
        result["status"] = "gone"
        return result
    except Exception as e:  # noqa: BLE001 — network and parsing failures are per-chart, not fatal
        result["status"] = f"bundle failed: {str(e)[:90]}"
        return result

    result["notes"] = bundle.notes
    result["summary"] = bundle.summary
    if dry_run:
        result["status"] = "not reviewed (dry run)"
        return result

    # A second view: the chart's other tab. A map-default chart hides the time dimension
    # entirely, and a series misbehaving over time is where problems are most often noticed.
    #
    # An earlier rung that picked out the entities holding the extreme values was dropped. Three
    # different rules for choosing which entities to show all failed to include the country the
    # one finding it ever produced was about, and that finding turned out to come from the max
    # line in the numeric summary rather than from any view. A view selector that needs a tuned
    # score to decide what to look at is a threshold in disguise.
    extra_views: list[tuple[str, bytes]] = []
    if with_image and views > 1 and bundle.other_tab_params:
        try:
            extra_views.append(
                (
                    f"the same chart's other tab, at ?{bundle.other_tab_params} — the default view "
                    "hides what this one shows",
                    render(slug, bundle.other_tab_params),
                )
            )
        except Exception:  # noqa: BLE001 — an extra view is a bonus, not a requirement
            pass

    agent = build_agent(model)
    parts = prompt_parts(bundle, extra_views)
    found: list[dict[str, Any]] = []
    in_tokens = out_tokens = 0

    # The model is not reproducible even at temperature 0: on repeated passes over the same
    # chart it raises a genuine finding on some and not others. Measured on three charts with
    # known findings, one pass caught 1-2 of them and three passes caught all three. So a pass
    # count is a recall dial, and at these prices it is cheap to turn up.
    bundle_hash = cache.content_hash(bundle.summary, bundle.png, *(png for _, png in extra_views))

    # Take every pass already cached for this bundle, not the first --repeat of them. Passes
    # disagree, so replaying a fixed slot that happened to miss froze a chart as "clean" forever;
    # using all of them means the cache can only add findings. Then top up to --repeat with fresh
    # passes, so asking for more than is cached still buys new evidence.
    already = cache.cached_passes(slug, model, bundle_hash) if use_cache else {}
    for issues in already.values():
        for issue in issues:
            _merge(found, issue)
    result["cached"] = len(already)

    next_index = (max(already) + 1) if already else 0
    for offset in range(max(max(repeat, 1) - len(already), 0)):
        # The model answers 503 often enough that a pass lost to one is a chart nobody reviewed.
        run = None
        for attempt in range(3):
            try:
                run = agent.run_sync(parts)
                break
            except Exception as e:  # noqa: BLE001 — one pass failing should not end the sweep
                if "503" not in str(e) and "overloaded" not in str(e).lower() or attempt == 2:
                    result["status"] = f"review failed: {str(e)[:90]}"
                    break
                time.sleep(3 * (attempt + 1))
        if run is None:
            break
        issues = [i.model_dump() for i in run.output.issues]
        for issue in issues:
            _merge(found, issue)
        if use_cache:
            cache.write_review(slug, model, bundle_hash, next_index + offset, issues)
        usage = run.usage
        in_tokens += usage.input_tokens or 0
        out_tokens += usage.output_tokens or 0

    result["issues"] = sorted(found, key=lambda i: (-i["passes"], i["severity"]))
    result["passes"] = max(max(repeat, 1), len(already))
    result["bundle_cached"] = bundle.from_cache
    result["views_reviewed"] = 1 + len(extra_views)
    result["input_tokens"], result["output_tokens"] = in_tokens, out_tokens
    result["cost"] = _cost(model, in_tokens, out_tokens)
    return result


@click.command(name="chart-critic", cls=click.RichCommand, help=__doc__)
@click.option("--slugs", type=str, default=None, help="Comma-separated chart slugs to review.")
@click.option(
    "--sample", type=int, default=None, help="Review N charts sampled with probability proportional to pageviews."
)
@click.option("--top", type=int, default=None, help="Review the N most-viewed charts.")
@click.option(
    "--min-views",
    type=int,
    default=DEFAULT_MIN_VIEWS,
    show_default=True,
    help="Ignore charts below this many annual pageviews.",
)
@click.option("--seed", type=int, default=0, show_default=True, help="Sampling seed, so a run is reproducible.")
@click.option("--model", type=str, default=DEFAULT_MODEL, show_default=True, help="Model to review with.")
@click.option("--cheap", is_flag=True, help=f"Use {CHEAP_MODEL} instead — roughly 15x cheaper.")
@click.option("--no-image", is_flag=True, help="Send metadata and numbers only, without the chart render.")
@click.option(
    "--repeat",
    type=int,
    default=3,
    show_default=True,
    help=(
        "Review each chart N times and union the findings. The model is not reproducible, so this is the "
        "recall dial: on charts with known errors a single pass catches roughly half of them and five "
        "catches all. Use 1 for a cheap first look, knowing it will miss things."
    ),
)
@click.option(
    "--changed-since",
    type=int,
    default=None,
    help="Review the published charts whose configuration changed in the last N days, most-read first.",
)
@click.option(
    "--include-data-updates",
    is_flag=True,
    help=(
        "With --changed-since, also include charts whose underlying dataset was re-run. An order of "
        "magnitude more charts and very uneven day to day, because one dataset update touches thousands."
    ),
)
@click.option(
    "--digest",
    "digest_out",
    type=click.Path(dir_okay=False, path_type=Path),
    default=None,
    help="Write a Slack-ready digest of the new findings here, and record them as posted.",
)
@click.option(
    "--post",
    is_flag=True,
    help=(
        "Post the digest to #we-need-to-correct-it instead of only writing it. For scheduled runs, "
        "where there is no human between the file and the channel. Needs SLACK_API_TOKEN."
    ),
)
@click.option(
    "--eval",
    "run_eval",
    is_flag=True,
    help="Run the known-answer fixtures instead of a sweep, and exit nonzero if any regressed.",
)
@click.option(
    "--views",
    type=int,
    default=1,
    show_default=True,
    help=(
        "How many views of each chart to show: 1 is the default view, 2 adds the chart's other tab "
        "where it has one (a map-default chart hides time, which is where problems usually show)."
    ),
)
@click.option(
    "--mdim-views",
    type=int,
    default=0,
    show_default=True,
    help=(
        "With --slugs, expand each multi-dim into this many of its views. Not needed with --sample: "
        "that pool already contains every mdim view, weighted by readership."
    ),
)
@click.option(
    "--params",
    type=str,
    default="",
    help="Review a specific view of every requested chart, e.g. 'country=~COM&time=2014..latest'.",
)
@click.option("--workers", type=int, default=6, show_default=True, help="Charts reviewed in parallel.")
@click.option(
    "--output", type=click.Path(dir_okay=False, path_type=Path), default=None, help="Write an HTML report here."
)
@click.option(
    "--json-out", type=click.Path(dir_okay=False, path_type=Path), default=None, help="Write raw results as JSON here."
)
@click.option(
    "--dry-run", is_flag=True, help="Build the bundles and print what would be reviewed, without calling the model."
)
@click.option("--no-cache", is_flag=True, help="Ignore the cache and re-fetch and re-review everything.")
@click.option(
    "--clear-cache",
    is_flag=True,
    help="Delete the cache and exit. Editing the prompt or the schema already invalidates it on its own.",
)
@click.option(
    "--cache-ttl",
    type=float,
    default=cache.DEFAULT_TTL_HOURS,
    show_default=True,
    help="Hours a cached chart bundle stays fresh. Use -1 to keep bundles indefinitely.",
)
def cli(
    slugs: str | None,
    sample: int | None,
    top: int | None,
    min_views: int,
    seed: int,
    model: str,
    cheap: bool,
    no_image: bool,
    repeat: int,
    views: int,
    mdim_views: int,
    params: str,
    workers: int,
    output: Path | None,
    json_out: Path | None,
    dry_run: bool,
    no_cache: bool,
    clear_cache: bool,
    cache_ttl: float,
    run_eval: bool,
    changed_since: int | None,
    include_data_updates: bool,
    digest_out: Path | None,
    post: bool,
) -> None:
    if clear_cache:
        removed, freed = cache.clear()
        console.print(f"Cleared {removed} cached files ({freed:.1f} MB) from {cache.CRITIC_CACHE_DIR}")
        return

    if cheap:
        model = CHEAP_MODEL

    if run_eval:
        # Five passes, because that is what was measured to reach 8/8: at two passes the two
        # subtlest cases (a subtitle typo and a baseline offset) are missed about half the time,
        # and a regression test that cries wolf is worse than one that costs 22 cents.
        _evaluate(model, not no_image, max(repeat, 5), not no_cache, cache_ttl)
        return

    use_cache = not no_cache
    charts_cached, reviews_cached, cache_mb = cache.stats()

    if changed_since is not None:
        changed = digest.changed_slugs(changed_since, include_data_updates)
        if not changed:
            console.print(f"No published charts changed in the last {changed_since} day(s).")
            return
        # Order by readership so a cap keeps the charts that matter.
        ranked = _pageviews(0)
        by_views = dict(zip(ranked.slug, ranked.views_365d))
        changed.sort(key=lambda s: -by_views.get(s, 0))
        console.print(f"[bold]{len(changed)} published charts changed in the last {changed_since} day(s)[/bold]")
        targets = [(s, int(by_views.get(s, 0)), params) for s in changed[: top or len(changed)]]
    else:
        targets = _expand_mdims(_select(slugs, sample, top, min_views, seed, params), mdim_views, seed)
    console.print(
        f"[bold]Reviewing {len(targets)} charts[/bold] with {model}"
        f"{' (no renders)' if no_image else ''}"
        f"{f', {repeat} passes each' if repeat > 1 else ''}"
        f"{f', {views} views each' if views > 1 else ''}{' — dry run' if dry_run else ''}"
    )
    if use_cache and charts_cached:
        console.print(
            f"[dim]cache: {charts_cached} charts, {reviews_cached} reviews, {cache_mb:.1f} MB "
            f"(algorithm {cache.algo_fingerprint()})[/dim]"
        )

    results: list[dict[str, Any]] = []
    with cf.ThreadPoolExecutor(workers) as ex:
        futures = [
            ex.submit(
                _review_one,
                slug,
                chart_pageviews,
                model,
                not no_image,
                dry_run,
                repeat,
                use_cache,
                cache_ttl,
                views,
                target_params,
            )
            for slug, chart_pageviews, target_params in targets
        ]
        for f in cf.as_completed(futures):
            r = f.result()
            results.append(r)
            if r["issues"]:
                view = f" [dim]?{r['params']}[/dim]" if r.get("params") else ""
                console.print(f"  [red]●[/red] {r['slug']}{view} — {len(r['issues'])} issue(s)")
                for i in r["issues"]:
                    # Keep the parameters that select the reviewed view, merged with whatever the
                    # model asked for, so an mdim finding links to the view it is about.
                    merged = issue_params(r.get("params", ""), i)
                    i["url"] = chart_link(r["slug"], merged, _link_keys(r["slug"], r.get("params", "")))
                    colour = SEVERITY_COLOR.get(i["severity"], "white")
                    seen = f" [dim]({i['passes']}/{r['passes']} passes)[/dim]" if r.get("passes", 1) > 1 else ""
                    console.print(f"      [{colour}]{i['severity']}/{i['kind']}[/{colour}] {i['claim']}{seen}")
                    console.print(f"      [dim]found in: {i.get('found_in', '?')}[/dim]")
                    console.print(f"      [link={i['url']}][blue underline]{i['url']}[/blue underline][/link]")
            elif r["status"] != "ok":
                console.print(f"  [dim]○ {r['slug']} — {r['status']}[/dim]")

    results.sort(key=lambda r: (-len(r["issues"]), -r["views"]))
    incomplete_run = _print_summary(results, model)

    if json_out:
        json_out.write_text(json.dumps(results, indent=1))
        console.print(f"raw results → {json_out}")
    if output:
        report.write(results, output, model=model)
        console.print(f"report → {output}")

    if post and not digest_out:
        raise click.ClickException("--post needs --digest <path> — the file is the record of what was sent")

    if digest_out:
        state = digest.load_state()
        # Resolved once and used for both the lookup and the record — see digest.stamp().
        facts = digest.chart_facts(results)
        fresh = digest.new_findings(results, state, facts)
        incomplete = sum(1 for r in results if r["status"].startswith(("bundle failed", "review failed")))
        message = digest.format_slack(
            fresh,
            reviewed=len(results),
            candidates=len(targets),
            incomplete=incomplete,
            window_days=changed_since,
            facts=facts,
        )
        digest_out.write_text(message)
        if message:
            console.print(f"\n[bold]digest ({len(fresh)} new finding(s)) → {digest_out}[/bold]")
            console.print(message)
            if post:
                # Post before recording, so a failed post is re-attempted tomorrow rather than
                # marked delivered. Duplicating a finding is a smaller harm than dropping one.
                _post_digest(message)
            digest.save_state(digest.stamp(fresh, state, facts))
        else:
            already = sum(len(r["issues"]) for r in results)
            console.print(
                f"\n[dim]nothing new to post — {already} finding(s), all previously reported[/dim]"
                if already
                else "\n[dim]nothing new to post — no findings[/dim]"
            )

    # Exit non-zero only once every output exists. Exiting inside the summary meant a single
    # failed chart suppressed the digest and its state, so the next run re-posted everything.
    if incomplete_run:
        raise SystemExit(2)


def _post_digest(message: str) -> None:
    """Send the digest to the channel, or fail the run.

    Deliberately not tolerant of a missing token: ``send_slack_message`` prints to stdout when
    ``SLACK_API_TOKEN`` is unset, which in a scheduled build is indistinguishable from a
    successful post — the failure mode where a job runs green for weeks and nobody notices the
    channel has been silent.
    """
    from etl import config
    from etl.slack_helpers import send_slack_message

    if not config.SLACK_API_TOKEN:
        raise click.ClickException("--post needs SLACK_API_TOKEN; refusing to silently print instead")
    send_slack_message(digest.SLACK_CHANNEL, message)
    console.print(f"[green]posted to {digest.SLACK_CHANNEL}[/green]")


def _evaluate(model: str, with_image: bool, repeat: int, use_cache: bool, ttl_hours: float) -> None:
    """Run the fixtures and report which known answers the critic still gets right."""
    console.print(
        f"[bold]Evaluating against {len(fixtures.CASES)} known-answer cases[/bold] with {model}, {repeat} passes each"
    )
    results = []
    with cf.ThreadPoolExecutor(min(len(fixtures.CASES), 6)) as ex:
        futures = {
            ex.submit(
                _review_one, c.slug, 0, model, with_image, False, repeat, use_cache, ttl_hours, c.views, c.params
            ): c
            for c in fixtures.CASES
        }
        for fut in cf.as_completed(futures):
            case = futures[fut]
            r = fut.result()
            # A chart that could not be fetched has no issues, which looks exactly like a clean
            # review — so a guard case would "pass" on a chart nobody looked at, and the eval
            # would green-light a critic that never ran.
            ok = r["status"] == "ok" and fixtures.matches(case, r["issues"])
            results.append((case, r, ok))

    table = Table(title="Known-answer evaluation")
    table.add_column("chart")
    table.add_column("expects")
    table.add_column("result")
    table.add_column("what the critic said", max_width=54)
    for case, r, ok in sorted(results, key=lambda x: (x[2], x[0].slug)):
        expects = "finds: " + ", ".join(case.expect_keywords) if case.expect_keywords else "nothing"
        said = "; ".join(i["claim"] for i in r["issues"])[:200] or (
            "—" if r["status"] == "ok" else f"[yellow]not reviewed: {r['status']}[/yellow]"
        )
        verdict = "[green]PASS[/green]" if ok else "[red]FAIL[/red]"
        if case.guards_against and ok:
            verdict += " [dim](guard)[/dim]"
        table.add_row(case.slug, expects, verdict, said)
    console.print(table)

    passed = sum(1 for _, _, ok in results if ok)
    # The gate ignores cases marked flaky. Eight of the nine are deterministic in practice
    # (15/15 cached passes each), so a failure among those is a regression; the ninth lands
    # roughly one pass in fifteen, and gating on it would fail most scheduled runs and teach
    # everyone to ignore the result.
    regressions = [(c, r) for c, r, ok in results if not ok and not c.flaky]
    flaky_misses = [c for c, _, ok in results if not ok and c.flaky]
    found = [(c, r) for c, r, ok in results if c.expect_keywords and ok]
    clean_ok = [(c, r) for c, r, ok in results if not c.expect_keywords and ok]
    cost = sum(r["cost"] for _, r, _ in results)
    console.print(
        f"\n[bold]{passed}/{len(results)} cases pass[/bold] — "
        f"{len(found)}/{sum(1 for c in fixtures.CASES if c.expect_keywords)} known errors found, "
        f"{len(clean_ok)}/{sum(1 for c in fixtures.CASES if not c.expect_keywords)} clean charts left alone"
        f" · ${cost:.4f}"
    )
    for case, r, ok in results:
        if not ok:
            tag = "[yellow]MISS[/yellow]" if case.flaky else "[red]FAIL[/red]"
            console.print(f"{tag} {case.slug}: {case.why}")
            if case.guards_against:
                console.print(f"       this case guards against: {case.guards_against}")
    if flaky_misses:
        console.print(
            f"\n[yellow]{len(flaky_misses)} flaky case(s) missed[/yellow] — a recall probe, not a "
            "regression, and not gated."
        )
    if regressions:
        console.print(
            "\n[dim]A fixture can also fail because the chart was fixed — check the chart before "
            "assuming the critic broke, then update the case and note the date.[/dim]"
        )
        raise SystemExit(1)


def _print_summary(results: list[dict[str, Any]], model: str) -> bool:
    """Print the run summary. Returns True when charts were left unreviewed."""
    flagged = [r for r in results if r["issues"]]
    issues: list[dict[str, Any]] = [i for r in flagged for i in r["issues"]]
    cost = sum(r["cost"] for r in results)

    table = Table(title="Charts with issues", show_lines=False)
    table.add_column("chart")
    table.add_column("views/day", justify="right")
    table.add_column("severity")
    table.add_column("kind")
    table.add_column("claim", max_width=70)
    for r in flagged:
        for i in r["issues"]:
            colour = SEVERITY_COLOR.get(i["severity"], "white")
            views = format_views(r["views"]).removesuffix(" views/day") or "—"
            url = i.get("url") or chart_link(
                r["slug"], issue_params(r.get("params", ""), i), _link_keys(r["slug"], r.get("params", ""))
            )
            table.add_row(
                f"[link={url}]{r['slug']}[/link]", views, f"[{colour}]{i['severity']}[/{colour}]", i["kind"], i["claim"]
            )
    if flagged:
        console.print(table)

    gone = [r for r in results if r["status"] == "gone"]
    failed = [r for r in results if r["status"].startswith(("bundle failed", "review failed"))]
    no_values = [r for r in results if r.get("notes")]

    console.print(
        f"\n[bold]{len(results)} charts reviewed[/bold] · "
        f"{len(flagged)} with issues ({len(issues)} in total) · "
        f"{len(results) - len(flagged) - len(gone) - len(failed)} clean"
    )

    # A cached negative looks exactly like a fresh negative, and because the model is flaky the
    # first pass over a chart with a real error is often a miss. Cache that miss and the chart is
    # "clean" for good. So always say how much of a run was replayed rather than reviewed.
    cached_passes = sum(r.get("cached", 0) for r in results)
    total_passes = sum(r.get("passes", 0) for r in results if r["status"] == "ok")
    if cached_passes:
        console.print(
            f"[dim]{cached_passes} of {total_passes} passes served from cache. Every cached pass is used, "
            f"so re-running can only add findings; raise --repeat or use --no-cache to buy fresh ones[/dim]"
        )
    no_render = [r for r in results if any("no render" in n for n in r.get("notes", []))]
    if no_values:
        console.print(
            f"[dim]{len(no_values) - len(no_render)} reviewed without values (non-redistributable data)[/dim]"
        )
    if no_render:
        console.print(
            f"[yellow]{len(no_render)} reviewed without a render[/yellow] — the static export 500s for "
            "most multi-dim views, so those were judged on metadata and values alone"
        )
    if gone:
        console.print(f"[dim]{len(gone)} slugs no longer resolve: {', '.join(r['slug'] for r in gone[:5])}[/dim]")
    if failed:
        # A finding that fires is loud; a chart that was never looked at is silent. So failures
        # are grouped by cause and the run exits non-zero, rather than being a line nobody reads
        # under a headline that says everything is clean. A sweep of 100 once lost 11 charts to
        # two bugs of mine, and one of them was hiding a verified finding.
        by_cause: dict[str, list[str]] = {}
        for r in failed:
            by_cause.setdefault(r["status"], []).append(r["slug"])
        console.print(f"\n[bold red]{len(failed)} charts were not reviewed[/bold red] — treat the run as incomplete:")
        for cause, slugs in sorted(by_cause.items(), key=lambda kv: -len(kv[1])):
            shown = ", ".join(slugs[:4]) + (f" … +{len(slugs) - 4}" if len(slugs) > 4 else "")
            console.print(f"  [red]{len(slugs):>3}[/red]  {cause}\n       {shown}")
    # Where a run's findings came from. Self-reported by the model, so it is a hypothesis about
    # which channels are earning their place — the answer comes from ablation, not from this.
    mechanisms: dict[str, int] = {}
    for issue in issues:
        mechanisms[issue.get("found_in", "?")] = mechanisms.get(issue.get("found_in", "?"), 0) + 1
    if mechanisms:
        breakdown = ", ".join(f"{k} {v}" for k, v in sorted(mechanisms.items(), key=lambda kv: -kv[1]))
        console.print(f"[dim]found in (self-reported): {breakdown}[/dim]")

    if cost:
        console.print(f"[dim]cost: ${cost:.4f} (${cost / max(len(results), 1):.4f}/chart) on {model}[/dim]")

    console.print(
        "\n[dim]Every finding is a claim to check, not a verdict: confirm it against the data before "
        "filing anything, and remember an error in an indicator affects every chart using it.[/dim]"
    )
    return bool(failed)
