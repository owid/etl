#!/usr/bin/env python
# /// script
# requires-python = ">=3.12"
# dependencies = []
# ///
#
# Note: This script imports from the etl package, so it must be run from within the etl environment.
# Use: .venv/bin/python scripts/vocabulary/simple_vocab_cli.py
"""
Simple LLM-based vocabulary extraction for OWID topics.

This CLI takes all chart titles and subtitles for topics and uses an LLM
to directly extract good keywords/phrases for search without NLP preprocessing.

Usage:
    # Extract for all topics (console output)
    .venv/bin/python scripts/vocabulary/simple_vocab_cli.py

    # Extract for specific topics
    .venv/bin/python scripts/vocabulary/simple_vocab_cli.py --topic energy --topic climate-change

    # Save to file
    .venv/bin/python scripts/vocabulary/simple_vocab_cli.py --topic energy --output vocab.json
"""

import asyncio
import json
import os
import sys
import tempfile
from pathlib import Path

import click
import httpx
from dotenv import load_dotenv  # ty: ignore
from google import genai  # ty: ignore
from google.genai import errors as genai_errors  # ty: ignore

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from owid.catalog import s3_utils  # ty: ignore

from coverage_model import (  # ty: ignore
    DEFAULT_VIEWS_COLUMN,
    EmptyAnalyticsError,
    TopicSelection,
    ViewAnalytics,
    WeightedUniverse,
    fetch_topic_universe,
    load_mdim_view_config_ids,
    load_view_analytics,
    select_terms_by_coverage,
    weigh_universe,
)
from coverage_report import (  # ty: ignore
    render_html_report,
    render_run_summary,
    render_topic_table,
)
from etl.config import OWID_ENV  # ty: ignore

S3_BUCKET_NAME = "owid-public"
DEFAULT_S3_VOCABULARY_PATH = "topic_vocabulary.json"

DEFAULT_MODEL = "gemini-3.7-flash"

# A whole-vocabulary run fires one request per topic at once, which draws the
# occasional 503/429 out of the API. Those are transient, but a topic that
# gives up is a topic missing from the upload, so retry each one a few times
# before letting it fail.
LLM_MAX_ATTEMPTS = 4
LLM_RETRY_BACKOFF_SECONDS = 4

# How many topics' record sets to fetch at once.
SEARCH_CONCURRENCY = 8

# How many terms to publish. The site shows five, then drops any that name a place
# or repeat the topic's own name, so a couple of spares keep the line full.
DEFAULT_MAX_TERMS = 8

# A term revealing less than this share of a topic's views is not worth one of
# five slots on a single line. Terms just below it are reported as near misses so
# the floor can be judged from the output.
DEFAULT_MIN_MARGINAL_SHARE = 0.01

# Gemini pricing per million tokens, for the cost estimate the CLI prints.
# Models absent from here still run; their cost is simply reported as unknown,
# so this list going out of date can't stop a regeneration (the whole run costs
# a couple of cents either way).
PRICING = {
    "gemini-2.5-flash-lite": {"input": 0.015, "output": 0.06},
    "gemini-3-flash-preview": {"input": 0.0375, "output": 0.15},
}


def get_all_topic_slugs() -> list[str]:
    """Get all available topic slugs from database.

    Returns: list of topic slugs
    """
    query = """
        SELECT DISTINCT t.slug
        FROM tags t
        JOIN chart_tags ct ON t.id = ct.tagId
        JOIN charts c ON ct.chartId = c.id
        WHERE c.publishedAt IS NOT NULL
          AND t.slug IS NOT NULL
        ORDER BY t.slug
    """
    df = OWID_ENV.read_sql(query)
    return df["slug"].tolist()


def get_topic_names(topic_slugs: list[str]) -> dict[str, str]:
    """Map topic slugs to the tag names the search index and gdocs use.

    One query for every topic rather than one per topic: OWID_ENV.read_sql opens a
    connection per call, and the old per-topic version spent most of a run's
    wall-clock on 125 of them.
    """
    placeholders = ", ".join(["%s"] * len(topic_slugs))
    rows = OWID_ENV.read_sql(
        f"SELECT slug, name FROM tags WHERE slug IN ({placeholders})",
        params=tuple(topic_slugs),
    )
    return {row.slug: row.name for row in rows.itertuples(index=False)}


def texts_for_prompt(universe: WeightedUniverse, limit: int = 200) -> list[str]:
    """The topic's chart texts to show the model, most-viewed first.

    Taken from the topic's *rendered* record set rather than from the `charts`
    table, which was the old source and a real hole: multi-dim and explorer views
    never reached the model, so on a topic like Climate Change it was proposing
    terms having seen 67 of 714 records. Since selection now scores terms on how
    much of that list they cover, the model has to be shown the list it is
    being scored against.

    Ordered by views and cut at `limit` rather than sampled at random, so the
    terms proposed are the ones describing what people actually look at, and two
    runs see the same input.
    """
    ordered = sorted(
        universe.records.items(),
        key=lambda item: -universe.weights.get(item[0], 0.0),
    )
    texts: list[str] = []
    seen: set[str] = set()
    for _, record in ordered:
        for text in (record.get("title"), record.get("subtitle")):
            if not text or text in seen:
                continue
            seen.add(text)
            texts.append(str(text))
        if len(texts) >= limit:
            break
    return texts[:limit]

async def extract_keywords_with_llm(
    topic_slug: str, topic_name: str, texts: list[str], api_key: str, model_id: str = DEFAULT_MODEL
) -> dict:
    """Use Gemini to extract good keywords/phrases from chart texts.

    Returns: dict with topic info, keywords, and token usage
    """
    client = genai.Client(api_key=api_key)

    # Combine all texts
    combined_text = "\n".join(texts)

    prompt = f"""Below are the titles and subtitles of the charts Our World in Data publishes on
the topic "{topic_name}", most-viewed first. They include the individual views of
its multi-dimensional charts and its explorers, which is what the topic page
actually lists:

{combined_text}

Give me search terms for this topic. They are offered to a reader on the topic's
page, under a search box that filters exactly the charts listed above, as a short
line of suggestions: "Suggested: term, term, term…". Only the first few are ever
shown, so put the ones you would most want a reader to see first.

What makes this list good is that each term takes the reader somewhere *different*.

1. Cover the range of what these charts are about. Read the whole list above and
   work out which distinct subjects it spans, then give at least one term for
   each. A topic's charts usually cover several: charts about sex ratios AND
   about unemployment AND about mental health all belong to "Gender Ratio", so a
   list that only describes the sex-ratio charts has missed most of the topic.
2. One term per subject. Do not give near-synonyms or variations on the same
   phrase — they land the reader on the same charts and waste the line. Pick the
   single most natural form and move on.
   - Bad: "sex ratio", "sex ratio at birth", "sex ratio by age", "sex ratio at age 5"
     (one subject, four terms)
   - Bad: "missing women", "sex-selective abortion", "excess female mortality"
     (all three lead to the same two charts)
   - Good: "sex ratio", "missing women", "life expectancy", "unemployment", "judiciary"
     (five terms, five different parts of the chart list)
3. Order the list so that each term adds something the ones before it did not.
4. Terms should be things a reader would plausibly type: specific subjects,
   technologies, diseases, materials, policies, groups.
5. Prefer the shortest form of a term that still names its subject. A reader
   clicking "sex ratio" is shown every chart about it; "sex ratio at birth"
   hides most of them for no gain. Give the broad form, not the specific one.
6. Words from the topic's own name are not just allowed, they are usually
   required. Look at the most-viewed charts at the top of the list: whatever they
   call their subject is the term readers want, even when it repeats the topic's
   name almost exactly. On "Child & Infant Mortality", whose biggest charts are
   titled "Child mortality rate", give "child mortality". On "Electricity Mix",
   give "electricity production". Offer them first.

   This includes the topic's own name, and the bare root of it. On "Religion",
   "religion" and "religious" are allowed and are probably the best first
   suggestions, because a third of that topic's traffic is one chart called
   "Share of the population who are religious" and nothing narrower reaches it.
   Include the short form as well as the compounds: give "religious" as well as
   "religious affiliation", "mortality" as well as "child mortality". The short
   one brings up everything the long one does and more, and if it turns out to
   add nothing it is dropped, so it costs nothing to offer.
7. Skip anything too broad to narrow the list down: a bare "men", "women",
   "children", "countries", "population".
8. Never name a place. No countries, regions, continents or income groups —
   not "United States", "Ukraine", "China", "low-income countries". The charts
   above mention them constantly and they are searched for separately.
9. Skip measurement and units language: percentage, per capita, share, rate,
   level, annual, total, average, "GDP per capita", and terms that are only a
   generic process word, such as "energy consumption" or "oil prices" (prefer
   "oil"). "GDP" on its own is only a term for a topic that is actually about
   GDP.
10. Skip historical or obsolete terms unless they are still what people search for.

Give a generous list — the more real candidates you offer, the better the final
selection can be. Terms are scored by how many of the charts above they actually
bring up, so favour the words those titles really use. Only a handful are shown to the reader, and they are chosen
from your list by measuring how much of the chart list above each one actually
brings up, so a term that is right but not obvious costs nothing to include and
a term that duplicates another costs nothing to leave out.

Output JSON (up to 40 terms; far fewer is right when the topic is narrow —
never pad the list with variations to reach a number):
{{
  "keywords": ["term1", "term2", ...]
}}"""

    try:
        response = None
        for attempt in range(1, LLM_MAX_ATTEMPTS + 1):
            try:
                response = await client.aio.models.generate_content(model=model_id, contents=prompt)
                break
            except (genai_errors.ServerError, genai_errors.ClientError) as e:
                # 429 (rate limited) and 5xx are worth another go; anything else
                # (a bad model id, a rejected prompt) will fail again identically.
                retriable = getattr(e, "code", None) == 429 or isinstance(e, genai_errors.ServerError)
                if not retriable or attempt == LLM_MAX_ATTEMPTS:
                    raise
                delay = LLM_RETRY_BACKOFF_SECONDS * attempt
                click.secho(
                    f"  {topic_name}: {getattr(e, 'code', '?')} from the API, retrying in {delay}s "
                    f"(attempt {attempt}/{LLM_MAX_ATTEMPTS - 1})",
                    fg="yellow",
                )
                await asyncio.sleep(delay)
        assert response is not None
        result_text = response.text.strip()

        # Extract token counts
        usage = response.usage_metadata
        input_tokens: int = (
            int(usage.prompt_token_count)
            if hasattr(usage, "prompt_token_count") and usage.prompt_token_count is not None
            else 0
        )
        output_tokens: int = (
            int(usage.candidates_token_count)
            if hasattr(usage, "candidates_token_count") and usage.candidates_token_count is not None
            else 0
        )

        # Extract JSON
        if "```json" in result_text:
            result_text = result_text.split("```json")[1].split("```")[0].strip()
        elif "```" in result_text:
            result_text = result_text.split("```")[1].split("```")[0].strip()

        result = json.loads(result_text)
        keywords = result.get("keywords", [])

        return {
            "topic_slug": topic_slug,
            "topic_name": topic_name,
            "keywords": keywords,
            "stats": {
                "num_charts_texts": len(texts),
                "num_keywords": len(keywords),
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
            },
        }

    except Exception as e:
        raise RuntimeError(f"LLM extraction failed for {topic_name}: {e}")


async def build_topic_universe(
    client: httpx.AsyncClient,
    topic_slug: str,
    topic_name: str,
    analytics: ViewAnalytics | None,
    mdim_config_ids: dict,
    semaphore: asyncio.Semaphore,
) -> WeightedUniverse:
    """Fetch and weight everything the all-charts block lists for one topic."""
    records = await fetch_topic_universe(client, topic_name, semaphore)
    if not records:
        raise ValueError(f"no charts indexed for topic '{topic_slug}'")
    return weigh_universe(topic_name, records, analytics, mdim_config_ids)


async def process_topics(
    topic_slugs: list[str],
    api_key: str,
    model: str,
    weighting: str,
    views_column: str,
    max_terms: int,
    min_marginal_share: float,
) -> tuple[list[dict], list[str], dict[str, WeightedUniverse], list[TopicSelection]]:
    """Generate a vocabulary for each topic and choose its terms by coverage.

    Four phases. The first three are shared setup and one request per topic; only
    the LLM call is per topic and expensive.

    1. Load, once: topic names, chart view counts, and the multi-dim view →
       config id map that lets a view's own popularity be found.
    2. Per topic, fetch the record set the all-charts block lists and weight each
       record by its views.
    3. Ask the model for candidate terms, having shown it that record set.
    4. Choose the terms that cover the most of it — see select_terms_by_coverage.

    There used to be a second LLM pass here, re-picking the shortlist from what
    the search returned for each candidate. Coverage subsumes it: a term whose
    rows are already covered adds nothing and is not chosen, and a term the block
    would show nothing for covers nothing and cannot be chosen at all.

    Returns: (results, failed slugs, universes by topic name, selections)
    """
    failed: list[str] = []

    click.echo("Loading topic names, chart views and multi-dim views...")
    names_by_slug = get_topic_names(topic_slugs)
    analytics = None if weighting == "uniform" else load_view_analytics(views_column)
    mdim_config_ids = load_mdim_view_config_ids() if analytics else {}
    if analytics:
        click.secho(
            f"✓ {len(analytics.by_slug):,} charts and {len(analytics.by_config_id):,} "
            f"views have {views_column}; {len(mdim_config_ids):,} multi-dim views mapped",
            fg="green",
        )
    else:
        click.secho(
            "! --weighting uniform: every chart counts the same, popularity is ignored",
            fg="yellow",
        )

    semaphore = asyncio.Semaphore(SEARCH_CONCURRENCY)
    universes: dict[str, WeightedUniverse] = {}
    prompt_inputs: list[tuple[str, str, list[str]]] = []

    click.echo(f"\nFetching the chart list for {len(topic_slugs)} topics...")
    async with httpx.AsyncClient() as client:
        async def load(topic_slug: str):
            topic_name = names_by_slug.get(topic_slug)
            if not topic_name:
                raise ValueError(f"no tag named for topic '{topic_slug}'")
            return topic_slug, topic_name, await build_topic_universe(
                client, topic_slug, topic_name, analytics, mdim_config_ids, semaphore
            )

        for outcome in await asyncio.gather(
            *(load(slug) for slug in topic_slugs), return_exceptions=True
        ):
            if isinstance(outcome, BaseException):
                click.secho(f"✗ {outcome}", fg="red")
                continue
            topic_slug, topic_name, universe = outcome
            universes[topic_name] = universe
            prompt_inputs.append((topic_slug, topic_name, texts_for_prompt(universe)))
            click.secho(
                f"✓ {topic_name}: {len(universe.records)} records, "
                f"{universe.total_weight:,.0f} views",
                fg="green",
            )

    # Any topic whose record set didn't load can't be generated at all.
    loaded = {slug for slug, _, _ in prompt_inputs}
    failed.extend(slug for slug in topic_slugs if slug not in loaded)

    if not prompt_inputs:
        return [], failed, universes, []

    click.echo(f"\nAsking for candidate terms for {len(prompt_inputs)} topics...")
    candidate_results = await asyncio.gather(
        *(
            extract_keywords_with_llm(topic_slug, topic_name, texts, api_key, model)
            for topic_slug, topic_name, texts in prompt_inputs
        ),
        return_exceptions=True,
    )

    click.echo("\nChoosing the terms that cover the most of each topic...")
    results: list[dict] = []
    selections: list[TopicSelection] = []
    for (topic_slug, topic_name, _), candidates in zip(
        prompt_inputs, candidate_results, strict=True
    ):
        if isinstance(candidates, BaseException):
            click.secho(f"✗ {topic_slug}: {candidates}", fg="red")
            failed.append(topic_slug)
            continue

        universe = universes[topic_name]
        offered = list(candidates["keywords"])
        selection = select_terms_by_coverage(
            universe,
            candidates["keywords"],
            max_terms=max_terms,
            min_marginal_share=min_marginal_share,
        )
        selections.append(selection)

        total = selection.total_weight
        candidates["keywords"] = [term.term for term in selection.selected]
        candidates["stats"].update(
            {
                "num_keywords": len(selection.selected),
                "num_candidates": len(offered),
                "num_records": selection.total_count,
                "weighted_coverage": round(
                    selection.covered_weight / total if total else 0.0, 4
                ),
                "chart_coverage": round(
                    (selection.total_count - len(selection.uncovered))
                    / selection.total_count
                    if selection.total_count
                    else 0.0,
                    4,
                ),
                "topic_name_share": round(selection.topic_name_share, 4),
                "weighting": weighting,
                "views_column": views_column,
                "selection": "coverage",
                # The consumer keys "trust this order" off `refined`; the order is
                # now measured rather than guessed, so it still holds. See
                # rankSuggestedKeywords in owid-grapher.
                "refined": True,
            }
        )
        candidates["keyword_stats"] = [
            {
                "term": term.term,
                "own_share": round(term.own_share(total), 4),
                "own_records": term.own_count,
                "marginal_share": round(term.marginal_share(total), 4),
                "marginal_records": term.marginal_count,
                "cumulative_share": round(term.cumulative_share(total), 4),
            }
            for term in selection.selected
        ]
        results.append(candidates)

        share = selection.covered_weight / total if total else 0.0
        click.secho(
            f"✓ {topic_name}: {len(selection.selected)} terms cover {share:.0%} of views "
            f"— {', '.join(term.term for term in selection.selected) or '(none)'}",
            fg="green" if share >= 0.35 else "yellow",
        )

    return results, failed, universes, selections


@click.command()
@click.option(
    "--topic",
    multiple=True,
    help="Topic slug(s) to extract vocabulary for (e.g., 'energy'). Can be specified multiple times. If not provided, extracts for all topics.",
)
@click.option("--output", help="Output JSON file path (optional, prints to console if not provided)")
@click.option(
    "--model",
    default=DEFAULT_MODEL,
    help=f"Gemini model to use (default: {DEFAULT_MODEL}). Any model id the API accepts works.",
)
@click.option(
    "--upload-path",
    default=DEFAULT_S3_VOCABULARY_PATH,
    help=(
        f"Key to upload to inside the {S3_BUCKET_NAME} bucket (default: {DEFAULT_S3_VOCABULARY_PATH}, "
        "the one the site reads). Use another key to try a vocabulary on a staging server without "
        "overwriting production, e.g. --upload-path topic_vocabulary/my-branch.json"
    ),
)
@click.option(
    "--report",
    "report_path",
    type=click.Path(dir_okay=False, writable=True),
    help="Write an HTML coverage report here: per term, what it reveals and what it adds.",
)
@click.option(
    "--weighting",
    type=click.Choice(["views", "uniform"]),
    default="views",
    help="Weight charts by how much they are viewed (default), or count them equally.",
)
@click.option(
    "--views-column",
    type=click.Choice(["views_7d", "views_14d", "views_365d"]),
    default=DEFAULT_VIEWS_COLUMN,
    help=(
        f"Which view window to weight by (default: {DEFAULT_VIEWS_COLUMN}). A vocabulary is "
        "read for weeks, so the year is steadier than the week."
    ),
)
@click.option(
    "--max-terms",
    default=DEFAULT_MAX_TERMS,
    show_default=True,
    help="Most terms to publish per topic.",
)
@click.option(
    "--min-marginal-share",
    default=DEFAULT_MIN_MARGINAL_SHARE,
    show_default=True,
    help="Stop once the next term would reveal less than this share of a topic's views.",
)
@click.option("--no-upload", is_flag=True, help="Skip uploading to R2 (useful for testing)")
def main(
    topic: tuple[str, ...],
    output: str | None,
    model: str,
    upload_path: str,
    report_path: str | None,
    weighting: str,
    views_column: str,
    max_terms: int,
    min_marginal_share: float,
    no_upload: bool,
):
    """Build the OWID topic vocabulary: the suggested search terms per topic.

    For each topic, fetches the chart list its all-charts block shows, weights
    every chart by how much it is viewed, asks an LLM for candidate search terms,
    and then picks the terms covering the most of that list — most-revealing
    first, each next one adding the most that is still uncovered.
    """
    # Load .env file from project root
    env_path = Path(__file__).parent.parent.parent / ".env"
    load_dotenv(env_path)

    # Load API key
    api_key = os.getenv("GOOGLE_API_KEY")
    if not api_key:
        click.secho("✗ GOOGLE_API_KEY not found in environment", fg="red")
        click.echo(f"  Looked in: {env_path}")
        sys.exit(1)

    # Determine which topics to process
    if topic:
        topic_slugs = list(topic)
    else:
        click.echo("No topics specified, extracting for all topics...")
        topic_slugs = get_all_topic_slugs()

    click.echo("=" * 80)
    click.echo("OWID TOPIC VOCABULARY")
    click.echo(f"Topics:    {len(topic_slugs)}")
    click.echo(f"Model:     {model}")
    click.echo(
        f"Weighting: {'every chart equally' if weighting == 'uniform' else views_column}"
    )
    click.echo(f"Terms:     at most {max_terms}, stopping below {min_marginal_share:.1%}")
    upload_target = "skipped" if no_upload else f"s3://{S3_BUCKET_NAME}/{upload_path}"
    click.echo(f"Upload:    {upload_target}")
    click.echo("=" * 80)
    click.echo()

    try:
        results, failed_slugs, universes, selections = asyncio.run(
            process_topics(
                topic_slugs,
                api_key,
                model,
                weighting=weighting,
                views_column=views_column,
                max_terms=max_terms,
                min_marginal_share=min_marginal_share,
            )
        )
    except EmptyAnalyticsError as e:
        click.echo()
        click.secho(f"✗ {e}", fg="red")
        sys.exit(1)

    if not results:
        click.secho("\n✗ No results generated", fg="red")
        sys.exit(1)

    # Calculate total costs
    pricing = PRICING.get(model)
    total_input_tokens = sum(r["stats"]["input_tokens"] for r in results)
    total_output_tokens = sum(r["stats"]["output_tokens"] for r in results)
    if pricing:
        total_input_cost = (total_input_tokens / 1_000_000) * pricing["input"]
        total_output_cost = (total_output_tokens / 1_000_000) * pricing["output"]
        total_cost = total_input_cost + total_output_cost

        # Add cost to each result
        for result in results:
            stats = result["stats"]
            input_cost = (stats["input_tokens"] / 1_000_000) * pricing["input"]
            output_cost = (stats["output_tokens"] / 1_000_000) * pricing["output"]
            stats["total_cost_usd"] = round(input_cost + output_cost, 6)

    # Output results
    click.echo()
    click.echo("=" * 80)
    click.echo("RESULTS")
    click.echo("=" * 80)
    click.echo()

    # A run covers every topic, so the per-term arithmetic only goes to the
    # terminal when few enough topics were asked for to read it; otherwise it is
    # one line each, worst-covered first, and the detail goes to --report.
    if len(selections) <= 3:
        for selection in selections:
            click.echo(render_topic_table(selection, universes[selection.topic_name]))
            click.echo()
    else:
        click.echo(render_run_summary(selections))
        click.echo()

    if report_path:
        Path(report_path).write_text(
            render_html_report(selections, universes, weighting, views_column),
            encoding="utf-8",
        )
        click.secho(f"✓ Coverage report: {report_path}", fg="green")
        click.echo()

    # Output total cost
    click.echo("=" * 80)
    click.echo("API USAGE & COST (TOTAL)")
    click.echo("=" * 80)
    click.echo(f"Topics processed: {len(results)}")
    click.echo(f"Input tokens:  {total_input_tokens:,}")
    click.echo(f"Output tokens: {total_output_tokens:,}")
    click.echo(f"Total tokens:  {total_input_tokens + total_output_tokens:,}")
    click.echo()
    if pricing:
        click.echo(f"Input cost:  ${total_input_cost:.6f}")
        click.echo(f"Output cost: ${total_output_cost:.6f}")
        click.echo(f"Total cost:  ${total_cost:.6f}")
    else:
        click.secho(f"Cost: unknown — no pricing recorded for {model} (add it to PRICING)", fg="yellow")

    # Build output data
    if len(results) == 1:
        output_data = results[0]
    else:
        output_data = {r["topic_slug"]: r for r in results}

    # Refuse to publish a vocabulary that is missing topics. A run fans out one
    # request per topic, so a transient API failure used to mean those topics
    # quietly vanished from the file — and uploading that over a complete
    # vocabulary loses their suggestions until someone notices. Keep the result
    # (--output still writes it) but make publishing it a deliberate choice.
    if failed_slugs and not no_upload:
        click.echo()
        click.secho(
            f"✗ {len(failed_slugs)} of {len(topic_slugs)} topics failed: {', '.join(failed_slugs)}",
            fg="red",
        )
        click.secho(
            "  Not uploading a partial vocabulary over a complete one. Re-run (optionally with "
            f"{' '.join('--topic ' + s for s in failed_slugs)}) or pass --no-upload to keep the partial result.",
            fg="red",
        )
        sys.exit(1)

    # Upload to R2
    if not no_upload:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(output_data, f, indent=2)
            tmp_path = f.name

        try:
            s3_url = f"s3://{S3_BUCKET_NAME}/{upload_path}"
            s3_utils.upload(s3_url, tmp_path, public=True)
            click.echo()
            # files.ourworldindata.org is the worker in front of this bucket; it
            # is what the site reads, because it adds CORS and an edge cache.
            click.secho(f"✓ Uploaded to: https://files.ourworldindata.org/{upload_path}", fg="green")
            if upload_path != DEFAULT_S3_VOCABULARY_PATH:
                click.secho(
                    "  (not the key the site reads by default — point TOPIC_VOCABULARY_URL at it to try it out)",
                    fg="yellow",
                )
        finally:
            os.unlink(tmp_path)

    # Save to local file if requested
    if output:
        with open(output, "w") as f:
            json.dump(output_data, f, indent=2)
        click.echo()
        click.secho(f"✓ Saved results to: {output}", fg="green")


if __name__ == "__main__":
    main()
