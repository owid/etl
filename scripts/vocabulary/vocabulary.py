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
from pathlib import Path

import click
from dotenv import load_dotenv  # type: ignore
from google import genai  # type: ignore

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from etl.config import OWID_ENV  # type: ignore

# Gemini pricing (as of Feb 2025)
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
    topic_slug: str, topic_name: str, texts: list[str], api_key: str, model_id: str = "gemini-3-flash-preview"
) -> dict:
    """Use Gemini to extract good keywords/phrases from chart texts.

    Returns: dict with topic info, keywords, and token usage
    """
    client = genai.Client(api_key=api_key)

    # Combine all texts
    combined_text = "\n".join(texts)

    prompt = f"""Extract search keywords for the topic "{topic_name}" from these chart titles and subtitles:

{combined_text}

Rules:
1. Extract ONLY modern, currently-relevant terms for "{topic_name}"
2. Keep: specific technologies, energy sources, or concepts people search for TODAY
   - Good: "solar panels", "fossil fuels", "coal", "wind turbines", "nuclear power"
   - Bad: "muscle energy", "firewood", "water engines" (historical, not searchable)
3. Remove generic words from phrases:
   - "oil prices" → "oil"
   - "energy consumption" → skip (both words generic)
4. Skip ALL terms containing: consumption, production, prices, investment, trade, access, growth, change, demand, supply, emissions, generation, reserves, intensity, transition, costs, spending
5. Skip measurements: percentage, per capita, share, rate, level, annual, total, average
6. Skip historical/obsolete terms unless still relevant today

Output JSON (up to 30 terms, but fewer is fine if not enough relevant ones):
{{
  "keywords": ["term1", "term2", ...]
}}"""

    try:
        response = await client.aio.models.generate_content(model=model_id, contents=prompt)
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


async def process_topics(topic_slugs: list[str], api_key: str, model: str) -> list[dict]:
    """Process multiple topics in parallel using asyncio.gather.

    Returns: list of results (one per topic)
    """
    # Extract chart texts for all topics first
    topic_data = []
    for topic_slug in topic_slugs:
        try:
            topic_name, texts = extract_chart_texts(topic_slug)
            topic_data.append((topic_slug, topic_name, texts))
            click.secho(f"✓ {topic_name}: Found {len(texts)} texts", fg="green")
        except ValueError as e:
            click.secho(f"✗ {topic_slug}: {e}", fg="red")

    if not topic_data:
        return []

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
        else:
            final_results.append(result)
            click.secho(f"✓ {result['topic_name']}: Extracted {len(result['keywords'])} keywords", fg="green")

    return final_results


@click.command()
@click.option(
    "--topic",
    multiple=True,
    help="Topic slug(s) to extract vocabulary for (e.g., 'energy'). Can be specified multiple times. If not provided, extracts for all topics.",
)
@click.option("--output", help="Output JSON file path (optional, prints to console if not provided)")
@click.option(
    "--model",
    default="gemini-3-flash-preview",
    type=click.Choice(["gemini-2.5-flash-lite", "gemini-3-flash-preview"], case_sensitive=False),
    help="Gemini model to use (default: gemini-3-flash-preview)",
)
def main(topic: tuple[str, ...], output: str | None, model: str):
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
    click.echo("=" * 80)
    click.echo()

    # Extract chart texts and process with LLM
    click.echo("Extracting chart titles and subtitles from database...")
    results = asyncio.run(process_topics(topic_slugs, api_key, model))

    if not results:
        click.secho("\n✗ No results generated", fg="red")
        sys.exit(1)

    # Calculate total costs
    pricing = PRICING[model]
    total_input_tokens = sum(r["stats"]["input_tokens"] for r in results)
    total_output_tokens = sum(r["stats"]["output_tokens"] for r in results)
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
    click.echo(f"Input cost:  ${total_input_cost:.6f}")
    click.echo(f"Output cost: ${total_output_cost:.6f}")
    click.echo(f"Total cost:  ${total_cost:.6f}")

    # Save to file if requested
    if output:
        # If single topic, save as single object; if multiple, save as dict keyed by slug
        if len(results) == 1:
            output_data = results[0]
        else:
            output_data = {r["topic_slug"]: r for r in results}

        with open(output, "w") as f:
            json.dump(output_data, f, indent=2)
        click.echo()
        click.secho(f"✓ Saved results to: {output}", fg="green")


if __name__ == "__main__":
    main()
