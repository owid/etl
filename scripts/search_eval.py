#!/usr/bin/env python
"""
Search Quality Evaluation Script

Uses Claude as a judge to evaluate search quality by:
1. Scoring query importance (0-3) - weights queries by value
2. Grading individual search results (0-10) - rates relevance
3. Calculating weighted DCG scores to compare search engines

USAGE:
    # Run with sample demo data
    .venv/bin/python scripts/search_eval.py --demo

    # Run with your own queries file
    .venv/bin/python scripts/search_eval.py --input queries.json

    # Interactive mode for testing single queries
    .venv/bin/python scripts/search_eval.py --interactive

    # Verbose output (shows reasoning for each score)
    .venv/bin/python scripts/search_eval.py --demo -v

    # Save results to JSON
    .venv/bin/python scripts/search_eval.py --input queries.json -o results.json

INPUT FILE FORMAT (JSON):
    [
      {
        "query": "life expectancy",
        "algolia_results": [
          {"title": "Life Expectancy", "snippet": "Global data..."}
        ],
        "semantic_results": [
          {"title": "Life Expectancy", "snippet": "Global data..."}
        ]
      }
    ]

ENVIRONMENT:
    Requires ANTHROPIC_API_KEY environment variable.

SCORING:
    Query Importance (weight):
    - 0: Noise (gibberish, off-topic)
    - 1: Vague (generic terms)
    - 2: Clear (specific topics)
    - 3: High-value (complex questions)

    Result Relevance (0-10):
    - 0: Irrelevant
    - 1-3: Tangential
    - 4-6: Broadly relevant
    - 7-9: Highly relevant
    - 10: Perfect match

    Final score uses DCG (Discounted Cumulative Gain) to weight
    results by position, then multiplies by query importance.
"""

import json
import math
import os
import re
from typing import Any

import anthropic
import click
import structlog

log = structlog.get_logger()

# Model to use for evaluation
MODEL = "claude-sonnet-4-5-20250929"


def extract_json(text: str) -> dict[str, Any] | None:
    """Extract JSON object from text that may contain other content."""
    # Try direct parse first
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # Try to find JSON in code blocks
    code_block_match = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.DOTALL)
    if code_block_match:
        try:
            return json.loads(code_block_match.group(1))
        except json.JSONDecodeError:
            pass

    # Try to find any JSON object in the text
    json_match = re.search(r"\{[^{}]*\"reasoning\"[^{}]*\"score\"[^{}]*\}", text, re.DOTALL)
    if json_match:
        try:
            return json.loads(json_match.group(0))
        except json.JSONDecodeError:
            pass

    # Try finding JSON with nested content more broadly
    json_match = re.search(r"\{.*?\}", text, re.DOTALL)
    if json_match:
        try:
            return json.loads(json_match.group(0))
        except json.JSONDecodeError:
            pass

    return None


# ---------------------------------------------------------
# SYSTEM PROMPTS
# ---------------------------------------------------------

QUERY_IMPORTANCE_PROMPT = """You are a Search Intent Classifier for "Our World in Data".

Scoring:
- 0: Noise (gibberish, off-topic like "movies", "login")
- 1: Vague (generic like "data", "stats")
- 2: Clear (specific topics like "gdp per capita", "population china")
- 3: High-value (complex questions like "correlation between life expectancy and gdp")

RESPOND WITH ONLY THIS JSON FORMAT, NO OTHER TEXT:
{"reasoning": "one sentence", "score": 0}"""

RESULT_RELEVANCE_PROMPT = """You are a Search Quality Evaluator for "Our World in Data".

Scoring (0-10):
- 0: Irrelevant
- 1-3: Tangential (keyword match but wrong topic)
- 4-6: Broadly relevant (right category, wrong specificity)
- 7-9: Highly relevant (direct answer)
- 10: Perfect match

Focus on semantic meaning, not keyword matching.

RESPOND WITH ONLY THIS JSON FORMAT, NO OTHER TEXT:
{"reasoning": "one sentence", "score": 0}"""


# ---------------------------------------------------------
# SCORING FUNCTIONS
# ---------------------------------------------------------


def get_query_weight(client: anthropic.Anthropic, query_text: str) -> dict[str, Any]:
    """Asks Claude to rate how important/sensible the query is (0-3)."""
    message = client.messages.create(
        model=MODEL,
        max_tokens=200,
        system=QUERY_IMPORTANCE_PROMPT,
        messages=[{"role": "user", "content": f"Query: '{query_text}'"}],
    )

    response_text = message.content[0].text
    response_json = extract_json(response_text)
    if response_json and "score" in response_json:
        return response_json
    log.warning("Failed to parse query weight response", query=query_text, response=response_text[:100])
    return {"score": 1, "reasoning": "Error parsing response"}


def grade_result(
    client: anthropic.Anthropic, query_text: str, result_title: str, result_snippet: str
) -> dict[str, Any]:
    """Asks Claude to rate a specific result (0-10)."""
    user_content = f"""
User Query: "{query_text}"

Search Result Title: "{result_title}"
Search Result Snippet: "{result_snippet}"
"""

    message = client.messages.create(
        model=MODEL,
        max_tokens=300,
        system=RESULT_RELEVANCE_PROMPT,
        messages=[{"role": "user", "content": user_content}],
    )

    response_text = message.content[0].text
    response_json = extract_json(response_text)
    if response_json and "score" in response_json:
        return response_json
    log.warning(
        "Failed to parse result grade response", query=query_text, title=result_title, response=response_text[:100]
    )
    return {"score": 0, "reasoning": "Error parsing response"}


# ---------------------------------------------------------
# CALCULATION LOGIC (DCG)
# ---------------------------------------------------------


def calculate_search_score(
    client: anthropic.Anthropic, query: str, results_list: list[dict], max_results: int = 3
) -> tuple[float, list[dict]]:
    """
    Calculates score for a single search engine on one query.
    Uses Simplified DCG: Score / log2(position + 1)
    """
    total_score = 0.0
    details = []

    for i, result in enumerate(results_list[:max_results]):
        position = i + 1

        # Ask Claude to grade this specific result
        grade = grade_result(client, query, result["title"], result.get("snippet", ""))
        raw_score = grade["score"]

        # Apply Position Discount (DCG style)
        # Position 1 = score / 1
        # Position 2 = score / 1.58
        # Position 3 = score / 2.0
        discount_factor = math.log2(position + 1)
        discounted_score = raw_score / discount_factor

        total_score += discounted_score

        details.append(
            {
                "pos": position,
                "title": result["title"],
                "raw_grade": raw_score,
                "discounted": round(discounted_score, 2),
                "reason": grade["reasoning"],
            }
        )

    return total_score, details


# ---------------------------------------------------------
# MAIN EVALUATION LOOP
# ---------------------------------------------------------


def run_evaluation(client: anthropic.Anthropic, test_queries: list[dict], verbose: bool = False) -> dict:
    """Run evaluation on a list of test queries."""
    final_stats = {
        "algolia_weighted_total": 0.0,
        "semantic_weighted_total": 0.0,
        "queries_processed": 0,
        "queries_skipped": 0,
    }

    print(f"\n{'QUERY':<30} | {'WEIGHT':<6} | {'ALGOLIA':<8} | {'SEMANTIC':<8} | {'WINNER'}")
    print("-" * 75)

    for q in test_queries:
        query_text = q["query"]

        # Step A: Weight the Query
        weight_data = get_query_weight(client, query_text)
        query_weight = weight_data["score"]

        # Skip noise
        if query_weight == 0:
            print(f"{query_text:<30} | NOISE  | SKIPPED")
            final_stats["queries_skipped"] += 1
            continue

        # Step B: Get Results
        algolia_results = q.get("algolia_results", [])
        semantic_results = q.get("semantic_results", [])

        # Step C: Calculate Scores
        score_algo, algo_details = calculate_search_score(client, query_text, algolia_results)
        score_sem, sem_details = calculate_search_score(client, query_text, semantic_results)

        # Step D: Apply Query Weight
        weighted_algo = score_algo * query_weight
        weighted_sem = score_sem * query_weight

        final_stats["algolia_weighted_total"] += weighted_algo
        final_stats["semantic_weighted_total"] += weighted_sem
        final_stats["queries_processed"] += 1

        winner = (
            "SEMANTIC" if weighted_sem > weighted_algo else ("TIE" if weighted_sem == weighted_algo else "ALGOLIA")
        )
        print(f"{query_text:<30} | {query_weight:<6} | {weighted_algo:<8.1f} | {weighted_sem:<8.1f} | {winner}")

        if verbose:
            print(f"  Algolia details: {algo_details}")
            print(f"  Semantic details: {sem_details}")

    return final_stats


# ---------------------------------------------------------
# SAMPLE DATA FOR DEMO
# ---------------------------------------------------------

SAMPLE_DATA = [
    {"query": "xyz test", "algolia_results": [], "semantic_results": []},
    {
        "query": "life expectancy",
        "algolia_results": [
            {"title": "Life Expectancy", "snippet": "Global life expectancy data..."},
            {"title": "Life Expectancy by Age", "snippet": "Data broken down by age..."},
        ],
        "semantic_results": [
            {"title": "Life Expectancy", "snippet": "Global life expectancy data..."},
            {"title": "Health Outcomes", "snippet": "Includes life expectancy and mortality..."},
        ],
    },
    {
        "query": "do rich countries live longer",
        "algolia_results": [
            {"title": "Richness of biodiversity", "snippet": "Species count per region..."},
            {"title": "Living planet index", "snippet": "Ecological footprint..."},
        ],
        "semantic_results": [
            {"title": "Life Expectancy vs GDP per Capita", "snippet": "Correlation between income and health..."},
            {"title": "Preston Curve", "snippet": "The relationship between life expectancy and real per capita income..."},
        ],
    },
]


# ---------------------------------------------------------
# CLI
# ---------------------------------------------------------


@click.command()
@click.option("--input", "input_file", type=click.Path(exists=True), help="JSON file with queries and results")
@click.option("--demo", is_flag=True, help="Run with sample data to test the evaluation")
@click.option("--interactive", is_flag=True, help="Interactive mode for testing single queries")
@click.option("--verbose", "-v", is_flag=True, help="Show detailed scoring for each result")
@click.option("--output", "-o", type=click.Path(), help="Save results to JSON file")
def main(input_file: str | None, demo: bool, interactive: bool, verbose: bool, output: str | None):
    """Search Quality Evaluation using Claude as a judge."""
    # Get API key
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise click.ClickException(
            "Missing ANTHROPIC_API_KEY environment variable. "
            "Set it in your .env file or export it in your shell."
        )

    client = anthropic.Anthropic(api_key=api_key)
    log.info("Initialized Anthropic client", model=MODEL)

    if interactive:
        run_interactive_mode(client)
        return

    if demo:
        test_data = SAMPLE_DATA
        log.info("Running with sample demo data")
    elif input_file:
        with open(input_file) as f:
            test_data = json.load(f)
        log.info("Loaded queries from file", path=input_file, count=len(test_data))
    else:
        raise click.ClickException("Provide --input FILE, --demo, or --interactive")

    # Run evaluation
    results = run_evaluation(client, test_data, verbose=verbose)

    # Print final results
    print("\n" + "=" * 40)
    print("FINAL RESULTS")
    print("=" * 40)
    print(f"Queries processed: {results['queries_processed']}")
    print(f"Queries skipped (noise): {results['queries_skipped']}")
    print(f"Total Weighted Score (Algolia):  {results['algolia_weighted_total']:.2f}")
    print(f"Total Weighted Score (Semantic): {results['semantic_weighted_total']:.2f}")

    if results["semantic_weighted_total"] > results["algolia_weighted_total"]:
        diff = results["semantic_weighted_total"] - results["algolia_weighted_total"]
        print(f"\nSemantic wins by {diff:.2f} points")
    elif results["algolia_weighted_total"] > results["semantic_weighted_total"]:
        diff = results["algolia_weighted_total"] - results["semantic_weighted_total"]
        print(f"\nAlgolia wins by {diff:.2f} points")
    else:
        print("\nIt's a tie!")

    # Save to file if requested
    if output:
        with open(output, "w") as f:
            json.dump(results, f, indent=2)
        log.info("Results saved", path=output)


def run_interactive_mode(client: anthropic.Anthropic):
    """Interactive mode for testing individual queries."""
    print("\nInteractive Search Evaluation Mode")
    print("Type 'quit' to exit\n")

    while True:
        query = input("Enter search query: ").strip()
        if query.lower() == "quit":
            break

        # Get query weight
        weight_data = get_query_weight(client, query)
        print(f"\nQuery Weight: {weight_data['score']}/3")
        print(f"Reasoning: {weight_data['reasoning']}\n")

        if weight_data["score"] == 0:
            print("Query classified as noise, skipping result grading.\n")
            continue

        # Get a result to grade
        title = input("Enter result title: ").strip()
        snippet = input("Enter result snippet: ").strip()

        if title:
            grade = grade_result(client, query, title, snippet)
            print(f"\nResult Grade: {grade['score']}/10")
            print(f"Reasoning: {grade['reasoning']}\n")


if __name__ == "__main__":
    main()
