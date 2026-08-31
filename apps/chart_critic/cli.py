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
from pathlib import Path
from typing import Any

import rich_click as click
from rich.console import Console
from rich.table import Table

from apps.chart_critic import cache, fixtures, mdim, report
from apps.chart_critic.bundle import GRAPHER_URL, Bundle, ChartGone, build, render
from apps.chart_critic.critic import (
    CHEAP_MODEL,
    DEFAULT_MODEL,
    FALLBACK_PRICES,
    Issue,
    build_agent,
    prompt_parts,
)
from apps.utils.llms.costs import estimate_llm_cost
from etl.db import read_sql

console = Console()

SEVERITY_COLOR = {"high": "red", "medium": "yellow", "low": "cyan"}

# Grapher query parameters the model is allowed to put in a link. Anything else it invents is
# dropped rather than passed through into a URL a reviewer will click.
ALLOWED_PARAMS = {"country", "time", "tab", "region", "stackMode", "mapSelect", "facet", "yScale", "xScale"}

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
        views = [] if params else mdim.sample_views(slug, mdim_views, seed=seed)
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
    views_by_slug = mdim.all_published_views()

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
        if key in allowed and value and re.fullmatch(r"[A-Za-z0-9_~.\-]+", value):
            clean.append(f"{key}={value}")
    return f"{GRAPHER_URL}/{slug}" + ("?" + "&".join(clean) if clean else "")


def _claim_tokens(claim: str) -> set[str]:
    return {w for w in re.findall(r"[a-z0-9]{4,}", claim.lower())}


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

    extra_views: list[tuple[str, bytes]] = []
    if views > 1 and with_image and bundle.extremes_params:
        try:
            extra_views.append(
                (
                    f"the same chart at ?{bundle.extremes_params} — the entities holding the highest and "
                    "lowest values in the series, which the default view does not show",
                    render(slug, bundle.extremes_params),
                )
            )
        except Exception:  # noqa: BLE001 — a second view is a bonus, not a requirement
            pass

    agent = build_agent(model)
    parts = prompt_parts(bundle, extra_views)
    found: list[dict[str, Any]] = []
    in_tokens = out_tokens = 0

    # The model is not reproducible even at temperature 0: on repeated passes over the same
    # chart it raises a genuine finding on some and not others. Measured on three charts with
    # known findings, one pass caught 1-2 of them and three passes caught all three. So a pass
    # count is a recall dial, and at these prices it is cheap to turn up.
    bundle_hash = cache.content_hash(bundle.summary, bundle.png)
    for pass_index in range(max(repeat, 1)):
        if use_cache:
            hit = cache.read_review(slug, model, bundle_hash, pass_index)
            if hit is not None:
                for issue in hit:
                    _merge(found, issue)
                result["cached"] += 1
                continue
        try:
            run = agent.run_sync(parts)
        except Exception as e:  # noqa: BLE001 — one pass failing should not end the sweep
            result["status"] = f"review failed: {str(e)[:90]}"
            break
        issues = [i.model_dump() for i in run.output.issues]
        for issue in issues:
            _merge(found, issue)
        if use_cache:
            cache.write_review(slug, model, bundle_hash, pass_index, issues)
        usage = run.usage
        in_tokens += usage.input_tokens or 0
        out_tokens += usage.output_tokens or 0

    result["issues"] = sorted(found, key=lambda i: (-i["passes"], i["severity"]))
    result["passes"] = max(repeat, 1)
    result["bundle_cached"] = bundle.from_cache
    result["views_reviewed"] = 1 + len(extra_views)
    result["extremes_params"] = bundle.extremes_params
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
    default=1,
    show_default=True,
    help="Review each chart N times and union the findings. The model is not reproducible, so this is the recall dial.",
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
        "How many views of each chart to show: 1 is the default view; 2 adds the entities holding the "
        "highest and lowest values, which is where implausible numbers usually hide."
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
                    merged = "&".join(x for x in (r.get("params", ""), i.get("chart_params", "")) if x)
                    i["url"] = chart_link(r["slug"], merged, mdim.dimension_keys(r["slug"]))
                    colour = SEVERITY_COLOR.get(i["severity"], "white")
                    seen = f" [dim]({i['passes']}/{r['passes']} passes)[/dim]" if r.get("passes", 1) > 1 else ""
                    console.print(f"      [{colour}]{i['severity']}/{i['kind']}[/{colour}] {i['claim']}{seen}")
                    console.print(f"      [link={i['url']}][blue underline]{i['url']}[/blue underline][/link]")
            elif r["status"] != "ok":
                console.print(f"  [dim]○ {r['slug']} — {r['status']}[/dim]")

    results.sort(key=lambda r: (-len(r["issues"]), -r["views"]))
    _print_summary(results, model)

    if json_out:
        json_out.write_text(json.dumps(results, indent=1))
        console.print(f"raw results → {json_out}")
    if output:
        report.write(results, output, model=model)
        console.print(f"report → {output}")


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
            results.append((case, r, fixtures.matches(case, r["issues"])))

    table = Table(title="Known-answer evaluation")
    table.add_column("chart")
    table.add_column("expects")
    table.add_column("result")
    table.add_column("what the critic said", max_width=54)
    for case, r, ok in sorted(results, key=lambda x: (x[2], x[0].slug)):
        expects = "finds: " + ", ".join(case.expect_keywords) if case.expect_keywords else "nothing"
        said = "; ".join(i["claim"] for i in r["issues"])[:200] or "—"
        verdict = "[green]PASS[/green]" if ok else "[red]FAIL[/red]"
        if case.guards_against and ok:
            verdict += " [dim](guard)[/dim]"
        table.add_row(case.slug, expects, verdict, said)
    console.print(table)

    passed = sum(1 for _, _, ok in results if ok)
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
            console.print(f"[red]FAIL[/red] {case.slug}: {case.why}")
            if case.guards_against:
                console.print(f"       this case guards against: {case.guards_against}")
    if passed < len(results):
        console.print(
            "\n[dim]The model is not reproducible — re-run before treating a single failure as a "
            "regression. A fixture can also fail because the chart was fixed; check, then update the case.[/dim]"
        )
        raise SystemExit(1)


def _print_summary(results: list[dict[str, Any]], model: str) -> None:
    flagged = [r for r in results if r["issues"]]
    issues: list[Issue] = [i for r in flagged for i in r["issues"]]
    cost = sum(r["cost"] for r in results)

    table = Table(title="Charts with issues", show_lines=False)
    table.add_column("chart")
    table.add_column("views/yr", justify="right")
    table.add_column("severity")
    table.add_column("kind")
    table.add_column("claim", max_width=70)
    for r in flagged:
        for i in r["issues"]:
            colour = SEVERITY_COLOR.get(i["severity"], "white")
            views = f"{r['views']:,}" if r["views"] else "—"
            url = i.get("url") or chart_link(r["slug"], i.get("chart_params", ""))
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
        console.print(f"[yellow]{len(failed)} failed[/yellow]: {', '.join(r['slug'] for r in failed[:5])}")
    if cost:
        console.print(f"[dim]cost: ${cost:.4f} (${cost / max(len(results), 1):.4f}/chart) on {model}[/dim]")

    console.print(
        "\n[dim]Every finding is a claim to check, not a verdict: confirm it against the data before "
        "filing anything, and remember an error in an indicator affects every chart using it.[/dim]"
    )
