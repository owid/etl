"""Utility functions for the inspector app."""

import json
import re
import time
from typing import Any

import anthropic
from rich import print as rprint
from structlog import get_logger

log = get_logger()


def get_text_context(text: str, typo: str, context_words: int = 10) -> str:
    """Extract context around a typo in text.

    Args:
        text: Full text containing the typo
        typo: The typo to find
        context_words: Number of words to show before and after

    Returns:
        Context string with typo highlighted
    """
    # Find the typo in text as a whole word (case insensitive)

    # Search for typo as a whole word using word boundaries
    pattern = r"\b" + re.escape(typo) + r"\b"
    match = re.search(pattern, text, re.IGNORECASE)

    if not match:
        # If not found as whole word, try substring match
        text_lower = text.lower()
        typo_lower = typo.lower()
        pos = text_lower.find(typo_lower)
        if pos == -1:
            # If still not found, return first N chars
            return text[:200] + ("..." if len(text) > 200 else "")
    else:
        pos = match.start()

    # Split into words
    words = text.split()

    # Find which word contains the typo
    char_count = 0
    typo_word_idx = -1
    for i, word in enumerate(words):
        if char_count <= pos < char_count + len(word) + 1:  # +1 for space
            typo_word_idx = i
            break
        char_count += len(word) + 1

    if typo_word_idx == -1:
        return text[:200] + ("..." if len(text) > 200 else "")

    # Get context words
    start_idx = max(0, typo_word_idx - context_words)
    end_idx = min(len(words), typo_word_idx + context_words + 1)

    context_words_list = words[start_idx:end_idx]
    context = " ".join(context_words_list)

    # Add ellipsis if truncated
    if start_idx > 0:
        context = "..." + context
    if end_idx < len(words):
        context = context + "..."

    return context


def extract_json_array(content: str) -> str:
    """Extract JSON array from Claude response, handling markdown and extra text.

    Args:
        content: Raw response text from Claude

    Returns:
        Extracted JSON array as string
    """

    # Handle markdown code blocks
    if "```json" in content:
        return content.split("```json")[1].split("```")[0].strip()
    elif "```" in content:
        return content.split("```")[1].split("```")[0].strip()

    # Try to parse as-is first
    content = content.strip()
    try:
        json.loads(content)
        return content
    except json.JSONDecodeError:
        pass

    # Find JSON array in text (e.g., after explanation text)
    # Look for pattern: [ followed by { or ] (start of array)
    match = re.search(r"(\[(?:\s*\{|\s*\]))", content)
    if match:
        start = match.start(1)
        # Find the matching closing bracket
        bracket_count = 0
        for i in range(start, len(content)):
            if content[i] == "[":
                bracket_count += 1
            elif content[i] == "]":
                bracket_count -= 1
                if bracket_count == 0:
                    return content[start : i + 1]

    # Fallback: return original content
    return content


def estimate_tokens(text: str) -> int:
    """Estimate number of tokens in text.

    Uses a simple heuristic: ~4 characters per token for English text.
    This is reasonably accurate for cost estimation purposes.

    Args:
        text: Text to estimate tokens for

    Returns:
        Estimated number of tokens
    """
    return len(text) // 4


def format_field_value(value: Any) -> str:
    """Format a field value for display, handling lists and JSON arrays specially.

    Args:
        value: The field value (can be str, list, or other)

    Returns:
        Formatted string suitable for AI inspection
    """
    # Handle string values that might be JSON arrays
    if isinstance(value, str):
        value_stripped = value.strip()
        # Check if it looks like a JSON array
        if value_stripped.startswith("[") and value_stripped.endswith("]"):
            try:
                parsed = json.loads(value_stripped)
                if isinstance(parsed, list):
                    # Recursively format the parsed list
                    return format_field_value(parsed)
            except (json.JSONDecodeError, TypeError):
                # If parsing fails, return as-is
                pass
        return value

    if isinstance(value, list):
        # For lists, join items with bullet points for readability
        if not value:
            return ""
        # Filter out empty items
        items = [str(item).strip() for item in value if item and str(item).strip()]
        if not items:
            return ""

        # Try to parse each item as JSON in case it's a JSON-encoded array
        parsed_items = []
        for item in items:
            try:
                # Check if item looks like a JSON array
                if item.startswith("[") and item.endswith("]"):
                    parsed = json.loads(item)
                    if isinstance(parsed, list):
                        # Flatten the nested list
                        parsed_items.extend(str(x).strip() for x in parsed if x and str(x).strip())
                    else:
                        parsed_items.append(item)
                else:
                    parsed_items.append(item)
            except (json.JSONDecodeError, TypeError):
                # If parsing fails, just use the original item
                parsed_items.append(item)

        if not parsed_items:
            return ""
        if len(parsed_items) == 1:
            return parsed_items[0]
        # Use bullet points for multiple items
        return "\n".join(f"• {item}" for item in parsed_items)
    return str(value)


def extract_chart_fields(chart_config: dict[str, Any]) -> list[tuple[str, Any]]:
    """Extract human-readable fields from chart config JSON.

    Args:
        chart_config: Chart configuration dictionary from chart_configs.full

    Returns:
        List of (field_name, field_value) tuples for all text fields
    """
    fields = []

    # Top-level text fields (excluding internalNotes as requested)
    for field_name in ["title", "subtitle", "note", "sourceDesc"]:
        if field_name in chart_config and chart_config[field_name]:
            fields.append((field_name, chart_config[field_name]))

    # Extract display names and other text from dimensions
    if "dimensions" in chart_config and chart_config["dimensions"]:
        for dim_idx, dimension in enumerate(chart_config["dimensions"]):
            if "display" in dimension and dimension["display"]:
                for display_key, display_value in dimension["display"].items():
                    # Only include string fields (names, labels, tooltips, etc.)
                    if isinstance(display_value, str) and display_value.strip():
                        field_key = f"dimension_{dim_idx}_display_{display_key}"
                        fields.append((field_key, display_value))

    return fields


def call_claude(
    client: anthropic.Anthropic,
    model: str,
    max_tokens: int,
    prompt: str,
    max_retries: int = 3,
    display_prompt: bool = False,
) -> anthropic.types.Message:
    """Call Claude API with exponential backoff retry logic.

    Args:
        client: Anthropic client instance
        model: Model name to use
        max_tokens: Maximum tokens in response
        prompt: Prompt text to send
        max_retries: Maximum number of retry attempts
        display_prompt: If True, print the prompt before sending

    Returns:
        API response message

    Raises:
        Exception: If all retries fail
    """
    if display_prompt:
        rprint(f"\n[bold yellow]{'=' * 80}[/bold yellow]")
        rprint(f"[bold yellow]Prompt to {model} (max_tokens={max_tokens}):[/bold yellow]")
        rprint(f"[bold yellow]{'=' * 80}[/bold yellow]")
        rprint(prompt)
        rprint(f"[bold yellow]{'=' * 80}[/bold yellow]\n")

    for attempt in range(max_retries):
        try:
            response = client.messages.create(
                model=model,
                max_tokens=max_tokens,
                messages=[{"role": "user", "content": prompt}],
            )
            return response
        except anthropic.APIError as e:
            if attempt < max_retries - 1:
                # Exponential backoff: 2^attempt seconds (2s, 4s, 8s)
                wait_time = 2 ** (attempt + 1)
                log.warning(f"API error (attempt {attempt + 1}/{max_retries}): {e}. Retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                log.error(f"API error after {max_retries} attempts: {e}")
                raise
        except Exception as e:
            log.error(f"Unexpected error calling Claude API: {e}")
            raise

    # This should never be reached due to raise in the loop, but satisfies type checker
    raise RuntimeError("All retry attempts failed")
