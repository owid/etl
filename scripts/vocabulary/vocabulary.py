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

# Phase 2 asks the site's own search API what each candidate term actually
# returns, so phase 3 can judge the shortlist on real results rather than on how
# the terms read. This is the same Algolia index the all-charts block searches,
# filtered to the same topic, so "what would a reader see if they clicked this
# chip" is answered directly instead of guessed at.
DEFAULT_SEARCH_API = "https://ourworldindata.org/api/search"
# How many results define a term's destination. A suggestion is a shortcut to
# the top of a result list, not to all of it — two terms whose first few charts
# are identical are duplicates however long their tails are.
SEARCH_API_TOP_N = 3
SEARCH_API_CONCURRENCY = 8
SEARCH_API_TIMEOUT_SECONDS = 30

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


def extract_chart_texts(topic_slug: str) -> tuple[str, list[str]]:
    """Extract all chart titles and subtitles for a topic.

    Returns: (topic_name, list_of_texts)
    """
    query = """
        SELECT DISTINCT
            t.name as topic_name,
            t.slug as topic_slug,
            cc.full->'$.title' as chart_title,
            cc.full->'$.subtitle' as chart_subtitle
        FROM charts c
        JOIN chart_configs cc ON c.configId = cc.id
        JOIN chart_tags ct ON c.id = ct.chartId
        JOIN tags t ON ct.tagId = t.id
        WHERE c.publishedAt IS NOT NULL
          AND t.slug = %s
    """

    df = OWID_ENV.read_sql(query, params=(topic_slug,))

    if df.empty:
        raise ValueError(f"No charts found for topic '{topic_slug}'")

    topic_name = df["topic_name"].iloc[0]

    # Collect all non-null titles and subtitles
    texts = []
    for _, row in df.iterrows():
        if row["chart_title"]:
            texts.append(str(row["chart_title"]))
        if row["chart_subtitle"]:
            texts.append(str(row["chart_subtitle"]))

    # Deduplicate texts (many charts have similar/identical titles)
    unique_texts = list(set(texts))

    # If still too many, take a representative sample
    if len(unique_texts) > 200:
        import random

        random.seed(42)  # Reproducible sampling
        unique_texts = random.sample(unique_texts, 200)

    return topic_name, unique_texts


async def extract_keywords_with_llm(
    topic_slug: str, topic_name: str, texts: list[str], api_key: str, model_id: str = DEFAULT_MODEL
) -> dict:
    """Use Gemini to extract good keywords/phrases from chart texts.

    Returns: dict with topic info, keywords, and token usage
    """
    client = genai.Client(api_key=api_key)

    # Combine all texts
    combined_text = "\n".join(texts)

    prompt = f"""Below are the titles and subtitles of every chart Our World in Data
publishes on the topic "{topic_name}":

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
5. Skip anything too broad to narrow the list down: a bare "men", "women",
   "children", "countries", "population", or the topic's own name.
6. Never name a place. No countries, regions, continents or income groups —
   not "United States", "Ukraine", "China", "low-income countries". The charts
   above mention them constantly and they are searched for separately.
7. Skip measurement and units language: percentage, per capita, share, rate,
   level, annual, total, average, "GDP per capita", and terms that are only a
   generic process word, such as "energy consumption" or "oil prices" (prefer
   "oil"). "GDP" on its own is only a term for a topic that is actually about
   GDP.
8. Skip historical or obsolete terms unless they are still what people search for.

Output JSON (up to 30 terms; far fewer is right when the topic is narrow —
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


async def fetch_term_results(
    client: httpx.AsyncClient,
    api_base: str,
    topic_name: str,
    term: str,
    semaphore: asyncio.Semaphore,
) -> list[str] | None:
    """Titles of the first few charts the site's search returns for a term, within a topic.

    Returns: chart titles, best match first; [] when the term genuinely finds
    nothing, and None when the search could not be performed at all. The two
    must not be conflated: "no results" is evidence against a term, whereas
    "unmeasurable" is no evidence either way, and treating the second as the
    first deletes perfectly good terms.

    Today the API conflates them itself: a topic-filtered search that finds
    nothing 400s if the topic is outside the 100 commonest of our ~140 topic
    tags, claiming the topic doesn't exist (owid/owid-grapher#7026 fixes this).
    Until that ships, terms in those topics come back unmeasurable and keep
    their place unjudged, which is the safe direction. Afterwards an empty
    result set arrives as a 200 and counts as evidence, as it should.
    """
    params = {"q": term, "topics": topic_name, "hitsPerPage": str(SEARCH_API_TOP_N)}
    async with semaphore:
        for attempt in range(1, LLM_MAX_ATTEMPTS + 1):
            try:
                response = await client.get(api_base, params=params, timeout=SEARCH_API_TIMEOUT_SECONDS)
                response.raise_for_status()
                results = response.json().get("results") or []
                return [r["title"] for r in results[:SEARCH_API_TOP_N] if r.get("title")]
            except httpx.HTTPStatusError as e:
                # A rejected topic or query will be rejected identically on a
                # retry; only server-side trouble is worth another go.
                if e.response.status_code < 500:
                    return None
                if attempt == LLM_MAX_ATTEMPTS:
                    return None
                await asyncio.sleep(LLM_RETRY_BACKOFF_SECONDS * attempt)
            except (httpx.HTTPError, KeyError, ValueError):
                if attempt == LLM_MAX_ATTEMPTS:
                    return None
                await asyncio.sleep(LLM_RETRY_BACKOFF_SECONDS * attempt)
    return None


async def measure_coverage(
    api_base: str, topic_name: str, terms: list[str], semaphore: asyncio.Semaphore
) -> dict[str, list[str] | None]:
    """What each candidate term actually returns, per the site's search API.

    Returns: term -> titles of its top results, or None where it couldn't be measured
    """
    async with httpx.AsyncClient() as client:
        tasks = [fetch_term_results(client, api_base, topic_name, term, semaphore) for term in terms]
        results = await asyncio.gather(*tasks)
    return dict(zip(terms, results, strict=True))


async def refine_keywords_with_llm(
    topic_name: str,
    candidates: list[str],
    coverage: dict[str, list[str] | None],
    api_key: str,
    model_id: str,
) -> tuple[list[str], int, int]:
    """Re-pick the shortlist now that each candidate's real results are known.

    Only terms whose results could actually be measured are put to the model.
    Unmeasured ones are appended afterwards, unjudged, so a search that couldn't
    answer never costs a term its place.

    Returns: (keywords, input_tokens, output_tokens)
    """
    client = genai.Client(api_key=api_key)

    measured = [c for c in candidates if coverage.get(c) is not None]
    unmeasured = [c for c in candidates if coverage.get(c) is None]
    if not measured:
        return candidates, 0, 0

    lines = []
    for term in measured:
        titles = coverage[term] or []
        shown = "; ".join(titles) if titles else "NOTHING"
        lines.append(f'- "{term}" → {shown}')
    findings = "\n".join(lines)

    prompt = f"""These are candidate search terms for the Our World in Data topic
"{topic_name}", each followed by the charts our site's search actually returns for
it, within this topic, best match first:

{findings}

They are offered to a reader as a short line of suggestions under a search box:
"Suggested: term, term, term…". Only the first few are shown. A term is worth a
slot only if clicking it takes the reader somewhere the earlier terms didn't.

Return the terms worth keeping, best first, applying what the results above show:

1. Drop a term whose results are the same charts an earlier kept term already
   returns. Keep whichever of them is the more natural thing to type.
2. Drop a term that returned NOTHING.
3. Drop a term whose results are unrelated to it — that means our search doesn't
   understand the term, so the reader would get nonsense. Acronyms often fail
   this way: check the titles actually concern the term's subject.
4. Order them so each adds a subject the ones before it didn't, most central to
   "{topic_name}" first.
5. Keep every term that earns its slot; don't trim to a round number, and don't
   invent terms that weren't offered above.

Output JSON:
{{
  "keywords": ["term1", "term2", ...]
}}"""

    response = None
    for attempt in range(1, LLM_MAX_ATTEMPTS + 1):
        try:
            response = await client.aio.models.generate_content(model=model_id, contents=prompt)
            break
        except (genai_errors.ServerError, genai_errors.ClientError) as e:
            retriable = getattr(e, "code", None) == 429 or isinstance(e, genai_errors.ServerError)
            if not retriable or attempt == LLM_MAX_ATTEMPTS:
                raise
            await asyncio.sleep(LLM_RETRY_BACKOFF_SECONDS * attempt)
    assert response is not None

    result_text = response.text.strip()
    if "```json" in result_text:
        result_text = result_text.split("```json")[1].split("```")[0].strip()
    elif "```" in result_text:
        result_text = result_text.split("```")[1].split("```")[0].strip()
    parsed = json.loads(result_text)

    usage = response.usage_metadata
    input_tokens = int(getattr(usage, "prompt_token_count", 0) or 0)
    output_tokens = int(getattr(usage, "candidates_token_count", 0) or 0)

    # Never let the refinement introduce a term that wasn't measured.
    allowed = {c.lower(): c for c in measured}
    keywords = [allowed[k.lower()] for k in parsed.get("keywords", []) if k.lower() in allowed]
    return keywords + unmeasured, input_tokens, output_tokens


async def process_topics(
    topic_slugs: list[str],
    api_key: str,
    model: str,
    search_api: str | None,
) -> tuple[list[dict], list[str]]:
    """Process multiple topics in parallel using asyncio.gather.

    When `search_api` is set, each topic's candidates go through two more
    phases: ask that API what each candidate actually returns, then ask the
    model to re-pick the shortlist knowing the real results.

    Returns: (results, slugs that failed)
    """
    # Extract chart texts for all topics first
    failed: list[str] = []
    topic_data = []
    for topic_slug in topic_slugs:
        try:
            topic_name, texts = extract_chart_texts(topic_slug)
            topic_data.append((topic_slug, topic_name, texts))
            click.secho(f"✓ {topic_name}: Found {len(texts)} texts", fg="green")
        except ValueError as e:
            click.secho(f"✗ {topic_slug}: {e}", fg="red")
            failed.append(topic_slug)

    if not topic_data:
        return [], failed

    # Extract keywords for all topics in parallel
    click.echo(f"\nExtracting keywords using LLM for {len(topic_data)} topics in parallel...")
    tasks = [
        extract_keywords_with_llm(topic_slug, topic_name, texts, api_key, model)
        for topic_slug, topic_name, texts in topic_data
    ]

    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Handle any exceptions
    final_results = []
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            topic_slug = topic_data[i][0]
            click.secho(f"✗ {topic_slug}: {result}", fg="red")
            failed.append(topic_slug)
        else:
            final_results.append(result)
            click.secho(f"✓ {result['topic_name']}: Extracted {len(result['keywords'])} keywords", fg="green")

    if search_api and final_results:
        click.echo(f"\nChecking what each candidate returns via {search_api} ...")
        semaphore = asyncio.Semaphore(SEARCH_API_CONCURRENCY)
        coverages = await asyncio.gather(
            *(measure_coverage(search_api, r["topic_name"], r["keywords"], semaphore) for r in final_results)
        )

        click.echo("Re-picking each shortlist from those results ...")
        refinements = await asyncio.gather(
            *(
                refine_keywords_with_llm(r["topic_name"], r["keywords"], coverage, api_key, model)
                for r, coverage in zip(final_results, coverages, strict=True)
            ),
            return_exceptions=True,
        )

        unmeasurable = [
            r["topic_name"]
            for r, coverage in zip(final_results, coverages, strict=True)
            if coverage and all(c is None for c in coverage.values())
        ]
        if unmeasurable:
            click.secho(
                f"! {len(unmeasurable)} topics could not be measured and keep their unrefined "
                f"candidates: {', '.join(unmeasurable)}",
                fg="yellow",
            )

        for result, coverage, refinement in zip(final_results, coverages, refinements, strict=True):
            if isinstance(refinement, BaseException):
                # Keep the unrefined shortlist rather than losing the topic; say
                # so, since it is measurably weaker than the refined ones.
                click.secho(f"! {result['topic_name']}: refinement failed, keeping candidates ({refinement})", fg="yellow")
                result["stats"]["refined"] = False
                continue
            keywords, input_tokens, output_tokens = refinement
            if not keywords:
                click.secho(f"! {result['topic_name']}: refinement returned nothing, keeping candidates", fg="yellow")
                result["stats"]["refined"] = False
                continue
            dropped = [k for k in result["keywords"] if k not in keywords]
            result["keywords"] = keywords
            result["stats"]["refined"] = True
            result["stats"]["num_keywords"] = len(keywords)
            result["stats"]["input_tokens"] += input_tokens
            result["stats"]["output_tokens"] += output_tokens
            result["stats"]["terms_measured"] = sum(1 for c in coverage.values() if c is not None)
            result["stats"]["terms_returning_nothing"] = sum(1 for c in coverage.values() if c == [])
            click.secho(
                f"✓ {result['topic_name']}: kept {len(keywords)}"
                + (f", dropped {', '.join(dropped)}" if dropped else ""),
                fg="green",
            )

    return final_results, failed


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
    "--search-api",
    default=DEFAULT_SEARCH_API,
    help=(
        f"Search API used to check what each candidate term actually returns (default: {DEFAULT_SEARCH_API}). "
        "Point it at a staging server to measure against that branch's index."
    ),
)
@click.option(
    "--no-refine",
    is_flag=True,
    help="Skip the search-API check and the second LLM pass; keep the first set of candidates as-is.",
)
@click.option("--no-upload", is_flag=True, help="Skip uploading to R2 (useful for testing)")
def main(
    topic: tuple[str, ...],
    output: str | None,
    model: str,
    upload_path: str,
    search_api: str,
    no_refine: bool,
    no_upload: bool,
):
    """Extract vocabulary for topics using LLM (simple approach).

    Takes all chart titles and subtitles for topics and asks an LLM to extract
    characteristic keywords/phrases. Processes multiple topics in parallel.
    Shows API cost by default.
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
    click.echo("SIMPLE LLM-BASED VOCABULARY EXTRACTION")
    click.echo(f"Topics: {len(topic_slugs)}")
    click.echo(f"Model: {model}")
    upload_target = "skipped" if no_upload else f"s3://{S3_BUCKET_NAME}/{upload_path}"
    click.echo(f"Upload: {upload_target}")
    click.echo(f"Refine: {'skipped' if no_refine else search_api}")
    click.echo("=" * 80)
    click.echo()

    # Extract chart texts and process with LLM
    click.echo("Extracting chart titles and subtitles from database...")
    results, failed_slugs = asyncio.run(
        process_topics(topic_slugs, api_key, model, None if no_refine else search_api)
    )

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

    for result in results:
        click.echo(f"Topic: {result['topic_name']}")
        click.echo(f"Keywords extracted: {len(result['keywords'])}")
        click.echo()
        click.echo("Keywords:")
        for i, kw in enumerate(result["keywords"], 1):
            click.echo(f"  {i:2d}. {kw}")
        click.echo()
        click.echo("-" * 80)
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
