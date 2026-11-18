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
import hashlib
import json
import sys
from pathlib import Path

import anthropic
import click
import httpx
from genai_prices import Usage, calc_price

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from coverage_model import (  # ty: ignore
    DEFAULT_VIEWS_COLUMN,
    CandidateReach,
    EmptyAnalyticsError,
    TopicSelection,
    ViewAnalytics,
    WeightedUniverse,
    fetch_topic_universe,
    load_mdim_view_config_ids,
    load_view_analytics,
    measure_candidates,
    measure_line,
    region_spellings,
    regions_definitions,
    weigh_universe,
)
from coverage_report import (  # ty: ignore
    render_html_report,
    render_run_summary,
    render_topic_table,
)

from apps.utils.llms.claude import text_of  # ty: ignore
from etl import config  # ty: ignore
from etl.config import OWID_ENV  # ty: ignore
from etl.paths import CACHE_DIR  # ty: ignore
from etl.steps.export.r2.owid.latest.topic_vocabulary import (  # ty: ignore
    PRODUCTION_KEY,
    VOCABULARY_FILE,
    published_vocabulary,
    upload,
)

# Choosing a line is a judgment call, where Opus is clearly better than Sonnet; a full run costs about $5.
DEFAULT_MODEL = "claude-opus-5-5"

# Reuse the answer for an identical request. Left alone, two runs of identical code
# disagreed on almost every topic — Gender Ratio came back covering 30% of its
# traffic in one run and 79% in the next — which makes a prompt improvement
# indistinguishable from luck, and this prompt has been iterated on a lot. The
# model can't be made to sample deterministically, so a topic whose prompt (its
# chart list included) and model are unchanged gets its earlier answer back: a
# change in the candidates is then attributable to a change in the inputs. Delete
# the file to sample afresh.
LLM_CACHE_FILE = CACHE_DIR / "vocabulary_llm_answers.json"

# A whole-vocabulary run asks about every topic, which draws the occasional 429/5xx
# out of the API. Those are transient, but a topic that gives up is a topic missing
# from the upload, so the client retries each one a few times (with backoff) before
# letting it fail, and only a few run at once, so they don't all hit the rate limit together.
LLM_MAX_RETRIES = 3
LLM_CONCURRENCY = 8

# Structured output: the answer is exactly this JSON, with nothing before or after it.
KEYWORDS_SCHEMA = {
    "type": "json_schema",
    "schema": {
        "type": "object",
        "properties": {"keywords": {"type": "array", "items": {"type": "string"}}},
        "required": ["keywords"],
        "additionalProperties": False,
    },
}

COUNTRIES_SCHEMA = {
    "type": "json_schema",
    "schema": {
        "type": "object",
        "properties": {"countries": {"type": "array", "items": {"type": "string"}}},
        "required": ["countries"],
        "additionalProperties": False,
    },
}

# How many topics' record sets to fetch at once.
SEARCH_CONCURRENCY = 8

# How many terms to publish. The site shows five, skipping the one that matches
# what the reader has already typed, so a couple of spares keep the line full.
DEFAULT_MAX_TERMS = 8


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
    terms having seen 67 of 714 records. Since candidates are measured on what
    they show of that list, the model has to be shown the list it is being
    measured against.

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
    topic_slug: str,
    topic_name: str,
    texts: list[str],
    client: anthropic.AsyncAnthropic,
    cache: dict[str, str],
    semaphore: asyncio.Semaphore,
    model_id: str = DEFAULT_MODEL,
) -> dict:
    """Ask the model for candidate terms for a topic's suggestion line, having shown it the chart texts.

    Returns: dict with topic info, the candidates as `keywords`, and token usage (zero when cached)
    """
    # Combine all texts
    combined_text = "\n".join(texts)

    prompt = f"""Below are the titles and subtitles of the charts Our World in Data publishes on
the topic "{topic_name}", most-viewed first:

{combined_text}

On the topic's page, a search box filters these charts, and a short line under it
suggests search terms. Clicking a term shows the charts whose title or subtitle
contains it. Propose candidates for that line. A later step measures what each one
would show and chooses the line, so a good term is lost only if nobody proposes it:
be generous.

Work out the distinct things these charts are about (a topic's charts usually span
more than its name suggests) and, for each, give the terms a reader would use to
look for it: the plain word and the narrower phrases the titles build on it
("alcohol" and "alcohol consumption"), the topic's own name included. Use the
titles' own wording, since a term only finds the charts that contain it. Each term
should name something a reader would want charts of: not only how a number is
expressed (a unit, a share, "per capita"), and not a place.

Output JSON, up to 40 terms (fewer when the topic is narrow):
{{
  "keywords": ["term1", "term2", ...]
}}"""

    try:
        result, input_tokens, output_tokens = await ask_for_json(
            prompt, KEYWORDS_SCHEMA, client, cache, semaphore, model_id
        )
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


def line_schema(terms: list[str]) -> dict:
    """An answer naming only these terms, so the model can choose among the candidates but not add to them."""
    return {
        "type": "json_schema",
        "schema": {
            "type": "object",
            "properties": {"keywords": {"type": "array", "items": {"type": "string", "enum": terms}}},
            "required": ["keywords"],
            "additionalProperties": False,
        },
    }


def describe_candidate(universe: WeightedUniverse, candidate: CandidateReach, examples: int = 3) -> str:
    """One candidate as the model sees it: what clicking it shows, and the most-viewed of those charts."""
    total = universe.total_weight
    share = candidate.weight / total if total else 0.0
    titles: list[str] = []
    for identity in sorted(candidate.records, key=lambda identity: -universe.weights.get(identity, 0.0)):
        title = universe.records[identity].get("title")
        if title and title not in titles:
            titles.append(str(title))
        if len(titles) == examples:
            break
    return f'- "{candidate.term}": {len(candidate.records)} charts, {share:.0%} of views — ' + "; ".join(
        f'"{title}"' for title in titles
    )


async def choose_line_with_llm(
    universe: WeightedUniverse,
    candidates: list[CandidateReach],
    max_terms: int,
    client: anthropic.AsyncAnthropic,
    cache: dict[str, str],
    semaphore: asyncio.Semaphore,
    model_id: str = DEFAULT_MODEL,
) -> dict:
    """Ask the model to choose the suggestion line from the measured candidates.

    Returns: dict with the chosen candidates in the model's order, and token usage.
    """
    if not candidates:
        return {"line": [], "input_tokens": 0, "output_tokens": 0}
    ordered = sorted(candidates, key=lambda candidate: -candidate.weight)
    described = "\n".join(describe_candidate(universe, candidate) for candidate in ordered)
    prompt = f"""On the Our World in Data page for the topic "{universe.topic_name}", a search box
filters the topic's {len(universe.records)} charts, and under it a line suggests a few search
terms: "Suggested: term, term, term…". Clicking a term shows the charts whose title or
subtitle contains it. Only the first five are shown.

Here are the candidate terms. For each: how many charts clicking it shows, the share of the
topic's views those charts get, and the most-viewed of them.

{described}

Choose the line: up to {max_terms} of these terms, in the order to show them. A good line is
one a curious reader would want to click through, and it gets there by three things at once:

- **Each term leads somewhere different.** Read the example charts: two terms whose charts
  are mostly the same, or that are two phrasings or spellings of one subject ("cancer deaths"
  and "cancer death rate", "R&D" and "research and development", "area burnt" and "area
  burned"), spend two slots on one subject. Keep the one that shows more.
- **Together they reach what readers come for.** The view shares say where the topic's
  readers go; a line that skips its most-viewed charts has missed the topic. Put the terms
  that open the most-viewed parts first.
  A subject the charts are only compared against, such as GDP per capita on the axis of
  "... vs. GDP per capita" charts, is not what the topic's readers come for, unless the topic
  is about it (GDP per capita on an economic topic).
- **Each term makes sense on its own.** The reader sees the term alone, with no chart next to
  it, and should know what clicking it will show. A subject ("working hours", "lung cancer")
  passes. These don't, however much they reach: a word that only means something next to the
  rest of a title ("age" or "work" on Time Use), a fragment of a title ("time with", or
  "street" lifted out of "living on the street"), a unit or measure ("tonnes", "death rate",
  "share"), something about the data rather than the world (how it was collected, how complete
  it is, which source it comes from), or a place, including its people ("Americans"). It
  should also be what a reader would type: a few words, not a chart's whole title. A term
  repeating the topic's own name is fine when it narrows: on Alcohol Consumption, "alcohol
  consumption" opens its central charts.

The site shows five, so give at least five when the candidates allow it, plus spares in case
one is hidden; stop short only when the next term would not be worth a reader's click.

Output JSON:
{{
  "keywords": ["term1", "term2", ...]
}}"""

    result, input_tokens, output_tokens = await ask_for_json(
        prompt, line_schema([candidate.term for candidate in ordered]), client, cache, semaphore, model_id
    )

    # The schema's enum keeps every answer among the candidates; a term named twice counts once.
    by_term = {candidate.term: candidate for candidate in candidates}
    line = [by_term[term] for term in dict.fromkeys(result["keywords"])]
    return {"line": line[:max_terms], "input_tokens": input_tokens, "output_tokens": output_tokens}


def load_country_names() -> dict[str, str]:
    """OWID's current country names, keyed by every spelling of them we know (name, short name, aliases), casefolded.

    The site filters charts by these exact names, so a suggestion has to be one of them.
    """
    names: dict[str, str] = {}
    for region in regions_definitions():
        if region.get("region_type", "country") != "country" or region.get("is_historical"):
            continue
        for spelling in region_spellings(region):
            names.setdefault(spelling.casefold(), region["name"])
    return names


async def suggest_countries_with_llm(
    topic_name: str,
    texts: list[str],
    country_names: dict[str, str],
    client: anthropic.AsyncAnthropic,
    cache: dict[str, str],
    semaphore: asyncio.Semaphore,
    model_id: str = DEFAULT_MODEL,
) -> dict:
    """Ask the model which countries are most worth looking at on this topic.

    Returns: dict with the countries (as OWID names, in the model's order), the names that matched no
    country, and token usage.
    """
    combined_text = "\n".join(texts)
    prompt = f"""Below are the titles and subtitles of the charts Our World in Data publishes on
the topic "{topic_name}", most-viewed first:

{combined_text}

On the topic's page, a search box filters these charts, and under it a short line
suggests what to look at. Besides search terms, that line will suggest a few
countries: clicking one shows the topic's charts for that country.

Which countries should it suggest? Pick the ones whose story on this topic is most
worth seeing, the countries someone who knows the topic well would point a curious
reader to first: those the topic is famous for, leaders and laggards, striking
outliers, and remarkable turnarounds. France on nuclear power, Norway on electric
cars, South Korea on fertility.

What makes the line useful is that it is specific to this topic. Don't fall back
on the same handful of large countries for every topic; suggest one only when it
really is a big part of this topic's story. The site adds the reader's own country
separately, so there is no need to cover where readers live. Suggest countries
only, not regions, continents or income groups, and use their common English names.

Put the one most worth seeing first; only the first few are shown. About five is
right for most topics; give fewer when only a few countries stand out.

Output JSON:
{{
  "countries": ["country1", "country2", ...]
}}"""

    try:
        result, input_tokens, output_tokens = await ask_for_json(
            prompt, COUNTRIES_SCHEMA, client, cache, semaphore, model_id
        )
    except Exception as e:
        raise RuntimeError(f"country suggestions failed for {topic_name}: {e}")

    countries: list[str] = []
    unrecognized: list[str] = []
    for suggestion in result.get("countries", []):
        name = country_names.get(suggestion.strip().casefold())
        if name is None:
            unrecognized.append(suggestion)
        elif name not in countries:
            countries.append(name)
    return {
        "countries": countries,
        "unrecognized": unrecognized,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
    }


async def ask_for_json(
    prompt: str,
    schema: dict,
    client: anthropic.AsyncAnthropic,
    cache: dict[str, str],
    semaphore: asyncio.Semaphore,
    model_id: str,
) -> tuple[dict, int, int]:
    """Ask the model for an answer in `schema`, reusing the saved answer to an identical request.

    Returns: (the parsed answer, input tokens, output tokens), the tokens zero when the answer came from the cache.
    """
    cache_key = hashlib.sha256(f"{model_id}\n{json.dumps(schema)}\n{prompt}".encode()).hexdigest()
    input_tokens = output_tokens = 0
    if cache_key in cache:
        result_text = cache[cache_key]
    else:
        async with semaphore:
            response = await client.messages.create(
                model=model_id,
                max_tokens=16000,
                messages=[{"role": "user", "content": prompt}],
                output_config={"format": schema},
            )
        if response.stop_reason != "end_turn":
            raise RuntimeError(f"the model stopped early ({response.stop_reason})")
        result_text = text_of(response)
        # Thinking tokens are billed as output and included in output_tokens.
        input_tokens, output_tokens = response.usage.input_tokens, response.usage.output_tokens

    result = json.loads(result_text)
    # Only an answer that parsed is worth reusing.
    cache[cache_key] = result_text
    return result, input_tokens, output_tokens


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
    model: str,
    weighting: str,
    views_column: str,
    max_terms: int,
    kept_countries: dict[str, dict] | None = None,
) -> tuple[list[dict], list[str], dict[str, WeightedUniverse], list[TopicSelection]]:
    """Generate a vocabulary for each topic: its suggestion line and its countries.

    1. Load, once: topic names, chart view counts, and the multi-dim view →
       config id map that lets a view's own popularity be found.
    2. Per topic, fetch the record set the all-charts block lists and weight each
       record by its views.
    3. Ask the model for candidate terms and for countries, having shown it that
       record set. A topic in kept_countries (slug → its committed `countries` and
       `unrecognized`) reuses those instead.
    4. Measure what each candidate would show (measure_candidates), and ask the
       model to choose the line from those measurements. Which terms make a good
       line is a judgment (whether a term means anything on its own, whether two
       are the same subject), so the model makes it; code only rules out what can
       be checked, and measures the result (measure_line).

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
            return (
                topic_slug,
                topic_name,
                await build_topic_universe(client, topic_slug, topic_name, analytics, mdim_config_ids, semaphore),
            )

        for outcome in await asyncio.gather(*(load(slug) for slug in topic_slugs), return_exceptions=True):
            if isinstance(outcome, BaseException):
                click.secho(f"✗ {outcome}", fg="red")
                continue
            topic_slug, topic_name, universe = outcome
            universes[topic_name] = universe
            prompt_inputs.append((topic_slug, topic_name, texts_for_prompt(universe)))
            click.secho(
                f"✓ {topic_name}: {len(universe.records)} records, {universe.total_weight:,.0f} views",
                fg="green",
            )

    # Any topic whose record set didn't load can't be generated at all.
    loaded = {slug for slug, _, _ in prompt_inputs}
    failed.extend(slug for slug in topic_slugs if slug not in loaded)

    if not prompt_inputs:
        return [], failed, universes, []

    kept_countries = kept_countries or {}
    click.echo(f"\nAsking for candidate terms and countries for {len(prompt_inputs)} topics...")
    country_names = load_country_names()

    client = anthropic.AsyncAnthropic(api_key=config.ANTHROPIC_API_KEY, max_retries=LLM_MAX_RETRIES)
    try:
        cache: dict[str, str] = json.loads(LLM_CACHE_FILE.read_text()) if LLM_CACHE_FILE.exists() else {}
    except json.JSONDecodeError:
        click.secho(f"  Ignoring an unreadable answer cache at {LLM_CACHE_FILE}", fg="yellow")
        cache = {}
    llm_semaphore = asyncio.Semaphore(LLM_CONCURRENCY)

    async def countries_for(topic_slug: str, topic_name: str, texts: list[str]) -> dict:
        if topic_slug in kept_countries:
            return {**kept_countries[topic_slug], "input_tokens": 0, "output_tokens": 0}
        return await suggest_countries_with_llm(topic_name, texts, country_names, client, cache, llm_semaphore, model)

    results: list[dict] = []
    selections: list[TopicSelection] = []
    try:
        candidate_results, country_results = await asyncio.gather(
            asyncio.gather(
                *(
                    extract_keywords_with_llm(topic_slug, topic_name, texts, client, cache, llm_semaphore, model)
                    for topic_slug, topic_name, texts in prompt_inputs
                ),
                return_exceptions=True,
            ),
            asyncio.gather(
                *(countries_for(topic_slug, topic_name, texts) for topic_slug, topic_name, texts in prompt_inputs),
                return_exceptions=True,
            ),
        )

        # Topics whose first requests failed go no further.
        to_choose = []
        for (topic_slug, topic_name, _), candidates, countries in zip(
            prompt_inputs, candidate_results, country_results, strict=True
        ):
            if isinstance(candidates, BaseException) or isinstance(countries, BaseException):
                error = candidates if isinstance(candidates, BaseException) else countries
                click.secho(f"✗ {topic_slug}: {error}", fg="red")
                failed.append(topic_slug)
                continue
            universe = universes[topic_name]
            to_choose.append(
                (topic_slug, universe, candidates, countries, measure_candidates(universe, candidates["keywords"]))
            )

        click.echo(f"\nChoosing the suggestion line for {len(to_choose)} topics...")
        line_results = await asyncio.gather(
            *(
                choose_line_with_llm(universe, measured, max_terms, client, cache, llm_semaphore, model)
                for _, universe, _, _, measured in to_choose
            ),
            return_exceptions=True,
        )
    finally:
        # Saved even if the run is interrupted, so the answers already paid for aren't asked for again.
        LLM_CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
        tmp_file = LLM_CACHE_FILE.with_suffix(".tmp")
        tmp_file.write_text(json.dumps(cache))
        tmp_file.replace(LLM_CACHE_FILE)  # atomic, so an interrupted write can't leave a broken cache

    for (topic_slug, universe, candidates, countries, measured), chosen in zip(to_choose, line_results, strict=True):
        topic_name = universe.topic_name
        if isinstance(chosen, BaseException):
            click.secho(f"✗ {topic_slug}: {chosen}", fg="red")
            failed.append(topic_slug)
            continue
        if countries["unrecognized"]:
            click.secho(
                f"! {topic_name}: dropped suggestions that aren't OWID country names: "
                f"{', '.join(countries['unrecognized'])}",
                fg="yellow",
            )

        selection = measure_line(universe, chosen["line"], measured)
        selections.append(selection)

        total = selection.total_weight
        stats = candidates["stats"]
        results.append(
            {
                "topic_slug": topic_slug,
                "topic_name": topic_name,
                "keywords": [term.term for term in selection.selected],
                "countries": countries["countries"],
                "stats": {
                    **stats,
                    "num_keywords": len(selection.selected),
                    # All three requests count towards the topic's cost.
                    "input_tokens": stats["input_tokens"] + countries["input_tokens"] + chosen["input_tokens"],
                    "output_tokens": stats["output_tokens"] + countries["output_tokens"] + chosen["output_tokens"],
                    "num_candidates": len(candidates["keywords"]),
                    "num_measured_candidates": len(measured),
                    "num_records": selection.total_count,
                    "weighted_coverage": round(selection.covered_weight / total if total else 0.0, 4),
                    "chart_coverage": round(
                        (selection.total_count - len(selection.uncovered)) / selection.total_count
                        if selection.total_count
                        else 0.0,
                        4,
                    ),
                    "topic_name_share": round(selection.topic_name_share, 4),
                    "weighting": weighting,
                    "views_column": views_column,
                    "selection": "model",
                    "num_countries": len(countries["countries"]),
                    "unrecognized_countries": countries["unrecognized"],
                },
                "keyword_stats": [
                    {
                        "term": term.term,
                        "own_share": round(term.own_share(total), 4),
                        "own_records": term.own_count,
                        "marginal_share": round(term.marginal_share(total), 4),
                        "marginal_records": term.marginal_count,
                        "cumulative_share": round(term.cumulative_share(total), 4),
                    }
                    for term in selection.selected
                ],
            }
        )

        share = selection.covered_weight / total if total else 0.0
        click.secho(
            f"✓ {topic_name}: {len(selection.selected)} terms cover {share:.0%} of views "
            f"— {', '.join(term.term for term in selection.selected) or '(none)'}"
            f" | countries: {', '.join(countries['countries']) or '(none)'}",
            fg="green" if share >= 0.35 else "yellow",
        )

    return results, failed, universes, selections


@click.command()
@click.option(
    "--topic",
    multiple=True,
    help="Topic slug(s) to extract vocabulary for (e.g., 'energy'). Can be specified multiple times. If not provided, extracts for all topics.",
)
@click.option(
    "--output",
    type=click.Path(dir_okay=False, writable=True, path_type=Path),
    default=VOCABULARY_FILE,
    help=(
        "Write the full results, stats included, to this JSON file (default: the committed vocabulary "
        "next to the export://r2/owid/latest/topic_vocabulary step, which publishes it once merged)."
    ),
)
@click.option(
    "--model",
    default=DEFAULT_MODEL,
    help=f"Claude model to use (default: {DEFAULT_MODEL}). Any model id the API accepts works.",
)
@click.option(
    "--upload-path",
    help=(
        "Also upload the published part to this key in the public bucket, to try the vocabulary on a "
        "staging server, e.g. --upload-path topic_vocabulary/my-branch.json. Production is published "
        "only by the export step, from the committed file."
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
    "--keep-countries",
    is_flag=True,
    help=(
        "Reuse each topic's countries from the committed vocabulary instead of asking for them again, "
        "to iterate on the keywords for less. Topics missing from it are still asked."
    ),
)
@click.option(
    "--max-terms",
    default=DEFAULT_MAX_TERMS,
    show_default=True,
    help="Most terms to publish per topic.",
)
def main(
    topic: tuple[str, ...],
    output: Path,
    model: str,
    upload_path: str | None,
    report_path: str | None,
    weighting: str,
    views_column: str,
    max_terms: int,
    keep_countries: bool,
):
    """Build the OWID topic vocabulary: the suggested search terms per topic.

    For each topic, fetches the chart list its all-charts block shows, weights
    every chart by how much it is viewed, asks an LLM for candidate search terms,
    measures what each candidate would show, and asks the LLM to choose the line
    from those measurements.
    """
    if not config.ANTHROPIC_API_KEY:
        click.secho("✗ ANTHROPIC_API_KEY not set (in .env or the environment)", fg="red")
        sys.exit(1)

    # Determine which topics to process
    if topic:
        topic_slugs = list(topic)
    else:
        click.echo("No topics specified, extracting for all topics...")
        topic_slugs = get_all_topic_slugs()

    # The committed file *replaces* the vocabulary rather than merging into it,
    # and merging it publishes it, so a run over some of the topics can't be
    # written there: every topic it skipped would lose its suggestions. Retrying a
    # handful of topics is still useful, written to another --output (and
    # optionally uploaded to a scratch --upload-path), but the committed file
    # takes a run over every topic. Checked before the first API call, so the
    # mistake costs nothing.
    writes_committed_file = output.resolve() == VOCABULARY_FILE.resolve()
    if topic and writes_committed_file:
        click.secho(
            f"✗ Refusing to write a {len(topic_slugs)}-topic run to the committed vocabulary "
            f"({VOCABULARY_FILE.name}): it would drop every other topic's suggestions once merged.",
            fg="red",
        )
        click.secho("  Drop --topic to cover every topic, or pass --output somewhere else.", fg="red")
        sys.exit(1)
    if upload_path == PRODUCTION_KEY:
        click.secho(
            f"✗ Refusing to upload to {PRODUCTION_KEY}, the key the site reads: production is published "
            "only by the export step, from the committed file, so that the live file is always the one on master.",
            fg="red",
        )
        sys.exit(1)

    click.echo("=" * 80)
    click.echo("OWID TOPIC VOCABULARY")
    click.echo(f"Topics:    {len(topic_slugs)}")
    click.echo(f"Model:     {model}")
    click.echo(f"Weighting: {'every chart equally' if weighting == 'uniform' else views_column}")
    click.echo(f"Terms:     at most {max_terms}")
    if keep_countries:
        click.echo(f"Countries: kept from {VOCABULARY_FILE.name}")
    click.echo(f"Output:    {output}")
    if upload_path:
        click.echo(f"Upload:    {upload_path} (scratch key)")
    click.echo("=" * 80)
    click.echo()

    # Each kept topic's countries, and the suggestions that were dropped from them, as last committed.
    kept_countries = (
        {
            slug: {"countries": entry["countries"], "unrecognized": entry["stats"].get("unrecognized_countries", [])}
            for slug, entry in json.loads(VOCABULARY_FILE.read_text()).items()
        }
        if keep_countries
        else None
    )
    try:
        results, failed_slugs, universes, selections = asyncio.run(
            process_topics(
                topic_slugs,
                model,
                weighting=weighting,
                views_column=views_column,
                max_terms=max_terms,
                kept_countries=kept_countries,
            )
        )
    except EmptyAnalyticsError as e:
        click.echo()
        click.secho(f"✗ {e}", fg="red")
        sys.exit(1)

    if not results:
        click.secho("\n✗ No results generated", fg="red")
        sys.exit(1)

    # Calculate total costs, pricing each topic on its own: long-context tiers apply per
    # request, so pricing the run's summed tokens could put all of it in the expensive
    # tier (a topic's requests together stay far below it). A model genai-prices doesn't know
    # still runs; its cost is simply reported as unknown.
    total_input_tokens = sum(r["stats"]["input_tokens"] for r in results)
    total_output_tokens = sum(r["stats"]["output_tokens"] for r in results)
    try:
        prices = [
            calc_price(
                Usage(input_tokens=r["stats"]["input_tokens"], output_tokens=r["stats"]["output_tokens"]),
                model,
                provider_id="anthropic",
            )
            for r in results
        ]
    except LookupError:
        prices = None
    if prices:
        total_input_cost = float(sum(p.input_price for p in prices))
        total_output_cost = float(sum(p.output_price for p in prices))
        total_cost = float(sum(p.total_price for p in prices))

        # Add cost to each result
        for result, price in zip(results, prices):
            result["stats"]["total_cost_usd"] = round(float(price.total_price), 6)

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
    if prices:
        click.echo(f"Input cost:  ${total_input_cost:.6f}")
        click.echo(f"Output cost: ${total_output_cost:.6f}")
        click.echo(f"Total cost:  ${total_cost:.6f}")
    else:
        click.secho(
            f"Cost: unknown — no price for {model} in genai-prices (try `uv lock --upgrade-package genai-prices`)",
            fg="yellow",
        )

    # Always keyed by topic slug, single-topic runs included: the site reads the
    # file as a mapping of topics, and a bare entry object indexes to nothing at
    # all rather than to one topic.
    output_data = {r["topic_slug"]: r for r in results}

    # Refuse to write a vocabulary that is missing topics over the committed one.
    # A run fans out one request per topic, so a transient API failure means
    # those topics are missing from the results, and merging that file would
    # lose their suggestions until someone notices. The answers that did come
    # back are cached, so the rerun only asks again for the failed topics.
    if failed_slugs and writes_committed_file:
        click.echo()
        click.secho(
            f"✗ {len(failed_slugs)} of {len(topic_slugs)} topics failed: {', '.join(failed_slugs)}",
            fg="red",
        )
        click.secho(
            f"  Not writing a partial vocabulary over {output}. Re-run over every topic, or pass "
            "--output somewhere else to keep this partial result.",
            fg="red",
        )
        sys.exit(1)

    output.write_text(json.dumps(output_data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    click.echo()
    click.secho(f"✓ Saved results to: {output}", fg="green")
    if writes_committed_file:
        click.secho("  Commit it in a PR: merging is what publishes it.", fg="green")

    # Upload to a scratch key, to try the vocabulary on a staging server.
    if upload_path:
        url = upload(published_vocabulary(output_data), upload_path)
        click.echo()
        click.secho(f"✓ Uploaded to: {url}", fg="green")
        click.secho("  (point TOPIC_VOCABULARY_URL at it to try it out on a staging server)", fg="yellow")


if __name__ == "__main__":
    main()
