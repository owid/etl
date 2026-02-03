"""Process FrontierMath benchmark data for garden step."""

import re
from datetime import datetime

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Pre-compile regex patterns for performance
PATTERNS = {
    "date": re.compile(r"(\d{4})-?(\d{2})-?(\d{2})"),
    "context_size": re.compile(r"\d+K$"),
    "claude_numeric": re.compile(r"claude-(\d+)-(\d+)-(\w+)", re.IGNORECASE),
    "claude_variant": re.compile(r"claude-(\w+)-(\d+(?:\.\d+)?)", re.IGNORECASE),
    "gpt_version": re.compile(r"gpt-([\d.]+)(?:-(\w+))?", re.IGNORECASE),
    "gpt_fallback": re.compile(r"gpt-(\w+)", re.IGNORECASE),
    "grok": re.compile(r"grok-(\d+)(?:-(\w+))?(?:-(\w+))?", re.IGNORECASE),
    "o_model": re.compile(r"(o\d+)(?:-(\w+))?"),
    "gemini": re.compile(r"gemini-([\d.]+)-?(\w+)?-?(\w+)?", re.IGNORECASE),
    "deepseek": re.compile(r"deepseek-?(.+)?", re.IGNORECASE),
    "mistral": re.compile(r"mistral-(\w+)-(\d+)", re.IGNORECASE),
    "qwen": re.compile(r"qwen(\d+)?-?(.+)?", re.IGNORECASE),
    "kimi": re.compile(r"kimi-?(.+)?", re.IGNORECASE),
    "llama": re.compile(r"llama-(\d+)-?(.+)?", re.IGNORECASE),
    "glm": re.compile(r"glm-([\d.]+)", re.IGNORECASE),
}


def _extract_metadata(model_version: str) -> tuple[str, str, str, bool]:
    """Extract date, suffixes, and clean model name from version string."""
    # Handle paths first
    if "/" in model_version:
        model_version = model_version.split("/")[-1]

    # Extract performance suffix (_high, _medium, _low, _xhigh, etc.)
    perf_suffix = ""
    if "_" in model_version:
        parts = model_version.rsplit("_", 1)
        suffix_word = parts[1].lower()
        if suffix_word in {"high", "medium", "low", "xhigh", "xlow"}:
            model_version = parts[0]
            perf_suffix = f", {parts[1]}"

    # Extract date
    date_str = ""
    has_date = False
    date_match = PATTERNS["date"].search(model_version)
    if date_match:
        try:
            year, month, day = date_match.groups()
            date_obj = datetime(int(year), int(month), int(day))
            date_str = f" ({date_obj.strftime('%b %Y')})"
            has_date = True
            model_version = PATTERNS["date"].sub("", model_version).rstrip("-")
        except ValueError:
            pass

    # Extract context size suffix (_16K, _32K, etc.)
    context_suffix = ""
    if "_" in model_version:
        parts = model_version.rsplit("_", 1)
        if PATTERNS["context_size"].match(parts[1]):
            context_suffix = f", {parts[1]}"
            model_version = parts[0]

    return model_version, date_str, f"{context_suffix}{perf_suffix}", has_date


def _format_claude(model_version: str) -> str:
    """Format Claude model names."""
    # Try numeric pattern first (claude-3-5-sonnet)
    match = PATTERNS["claude_numeric"].match(model_version)
    if match:
        return f"Claude {match.group(1)}.{match.group(2)} {match.group(3).title()}"
    # Try variant pattern (claude-opus-4)
    match = PATTERNS["claude_variant"].match(model_version)
    if match:
        return f"Claude {match.group(1).title()} {match.group(2)}"
    return model_version


def _format_gpt(model_version: str) -> str:
    """Format GPT model names."""
    match = PATTERNS["gpt_version"].match(model_version)
    if match:
        variant = f" {match.group(2)}" if match.group(2) else ""
        return f"GPT {match.group(1)}{variant}"
    match = PATTERNS["gpt_fallback"].match(model_version)
    return f"GPT {match.group(1)}" if match else model_version


def _format_grok(model_version: str) -> str:
    """Format Grok model names."""
    match = PATTERNS["grok"].match(model_version)
    if match:
        # Filter out date-like patterns (4-8 digit numbers)
        variants = "".join(f" {g}" for g in match.groups()[1:] if g and not (g.isdigit() and 4 <= len(g) <= 8))
        return f"Grok {match.group(1)}{variants}"
    return model_version


def _format_mistral(model_version: str) -> str:
    """Format Mistral model names with date parsing."""
    match = PATTERNS["mistral"].match(model_version)
    if match:
        variant = match.group(1).title()
        year_month = match.group(2)
        if len(year_month) == 4:
            try:
                date_obj = datetime(int(f"20{year_month[:2]}"), int(year_month[2:]), 1)
                return f"Mistral {variant} ({date_obj.strftime('%b %Y')})"
            except ValueError:
                pass
        return f"Mistral {variant}"
    return model_version


def _format_generic(pattern_name: str, prefix: str, model_version: str) -> str:
    """Format models with simple patterns."""
    match = PATTERNS[pattern_name].match(model_version)
    if not match:
        return model_version

    groups = [g for g in match.groups() if g]
    if not groups:
        return prefix

    if pattern_name == "gemini":
        variants = "".join(f" {g.title()}" if i == 1 else f" {g}" for i, g in enumerate(groups[1:], 1))
        return f"{prefix} {groups[0]}{variants}"
    elif pattern_name in {"qwen", "llama"}:
        version = groups[0] if len(groups) > 0 else ""
        if len(groups) > 1:
            # Filter out date-like patterns (4-8 digit numbers at the end)
            variant_parts = groups[1].split("-")
            variant_parts = [p for p in variant_parts if not (p.isdigit() and 4 <= len(p) <= 8)]
            variant = " ".join(variant_parts).title()
        else:
            variant = ""
        return f"{prefix}{version} {variant}".strip()
    elif pattern_name in {"deepseek", "kimi"}:
        variant = groups[0].replace("-", " ").title() if groups else ""
        return f"{prefix} {variant}".strip() if variant else prefix
    elif pattern_name == "o_model":
        variant = f" {groups[1]}" if len(groups) > 1 else ""
        return f"{groups[0]}{variant}"
    elif pattern_name == "glm":
        return f"{prefix} {groups[0]}"

    return model_version


def format_model_name(model_version: str) -> str:
    """
    Format technical model names into human-readable format.

    Examples:
        claude-3-5-sonnet-20240620 -> Claude 3.5 Sonnet (Jun 2024)
        gpt-4o-2024-08-06 -> GPT-4o (Aug 2024)
        o1-mini-2024-09-12_high -> o1-mini (Sep 2024, high)
        grok-3-mini-beta_high -> Grok 3-mini-beta (high)
    """
    # Extract metadata
    model_version, date_str, suffix_str, has_date = _extract_metadata(model_version)

    # Dispatch to appropriate formatter based on model family
    model_lower = model_version.lower()

    if "claude" in model_lower:
        model_name = _format_claude(model_version)
    elif model_version.startswith("gpt-"):
        model_name = _format_gpt(model_version)
    elif "grok" in model_lower:
        model_name = _format_grok(model_version)
    elif model_version.startswith("o") and model_version[1:2].isdigit():
        model_name = _format_generic("o_model", "", model_version)
    elif "gemini" in model_lower:
        model_name = _format_generic("gemini", "Gemini", model_version)
    elif "deepseek" in model_lower:
        model_name = _format_generic("deepseek", "DeepSeek", model_version)
    elif "mistral" in model_lower:
        model_name = _format_mistral(model_version)
    elif "qwen" in model_lower:
        model_name = _format_generic("qwen", "Qwen", model_version)
    elif "kimi" in model_lower:
        model_name = _format_generic("kimi", "Kimi", model_version)
    elif "llama" in model_lower:
        model_name = _format_generic("llama", "Llama", model_version)
    elif "glm" in model_lower:
        model_name = _format_generic("glm", "GLM", model_version)
    else:
        model_name = model_version

    # Combine date and suffixes
    if has_date:
        final_name = f"{model_name}{date_str}{suffix_str}"
    elif suffix_str:
        final_name = f"{model_name}{suffix_str}"
    else:
        final_name = model_name

    return final_name.strip()


def run() -> None:
    """Process FrontierMath benchmark data."""
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("frontiermath")

    # Read table from meadow dataset.
    tb = ds_meadow.read("epoch_benchmark_data")

    #
    # Process data.
    #
    tb["mean_score"] = tb["mean_score"] * 100

    # Format model names to be human-readable
    tb["model_version"] = tb["model_version"].apply(format_model_name)

    tb = tb.format(["release_date", "model_version"])
    #
    # Save outputs.
    #
    # Create garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata)

    # Save changes.
    ds_garden.save()
