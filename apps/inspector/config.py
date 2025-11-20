"""Configuration for the inspector app."""

from etl.paths import BASE_DIR

# Path to shared model pricing configuration
MODELS_YAML_PATH = BASE_DIR / "apps" / "utils" / "llms" / "models.yml"

# Claude model configuration
# Choose model based on speed vs quality tradeoff:
# - Haiku: Fastest, cheapest (18% faster, 72% cheaper), excellent quality for this task
# - Sonnet: Higher quality but slower and more expensive
# - Opus: Highest quality but much slower and more expensive
#
# Testing showed Haiku finds same semantic issues as Sonnet with minimal quality difference,
# making it the best choice for large-scale analysis.

# Mapping from YAML model names to Anthropic API model names
MODEL_API_NAMES = {
    "anthropic:claude-haiku-4-5": "claude-haiku-4-5-20251001",
    "anthropic:claude-sonnet-4-5": "claude-sonnet-4-5-20250929",
    "anthropic:claude-opus-4": "claude-opus-4-20250514",
}

# Model for individual issue detection (used many times - cost matters)
CLAUDE_MODEL = MODEL_API_NAMES["anthropic:claude-haiku-4-5"]  # Fast: $1/M in, $5/M out (RECOMMENDED)
# CLAUDE_MODEL = MODEL_API_NAMES["anthropic:claude-sonnet-4-5"]  # Balanced: $3/M in, $15/M out
# CLAUDE_MODEL = MODEL_API_NAMES["anthropic:claude-opus-4"]  # Quality: $15/M in, $75/M out

# Model for grouping/pruning (used once per collection - quality matters more than cost)
GROUPING_MODEL = MODEL_API_NAMES["anthropic:claude-sonnet-4-5"]  # Better reasoning for filtering false positives
# GROUPING_MODEL = MODEL_API_NAMES["anthropic:claude-opus-4"]  # Best quality if budget allows

# Concurrency limit for API requests
# Anthropic rate limits: typically 5-50 concurrent requests depending on tier
# Increase if you have higher tier access, decrease if you hit rate limits
MAX_CONCURRENT_REQUESTS = 25

# Token limits
DETECTION_MAX_TOKENS = 1_024  # Max tokens for individual issue detection
GROUPING_MAX_TOKENS = 3_072  # Max tokens for grouping/pruning
MAX_PROMPT_TOKENS = 195_000  # Leave buffer below 200k context limit
INSTRUCTION_BUFFER_TOKENS = 5_000  # Buffer for prompt instructions

# Context lengths for display/grouping
CONTEXT_LENGTH_SHORT = 200  # Short context for issue display
CONTEXT_LENGTH_MEDIUM = 300  # Medium context for grouping
CONTEXT_LENGTH_LONG = 500  # Long context for debugging


def _load_model_pricing() -> dict[str, dict[str, float]]:
    """Load model pricing from the wizard YAML configuration.

    Returns:
        Dictionary mapping API model names to pricing info with 'input' and 'output' keys.
        For models with tiered pricing, uses the first tier (lowest price).
    """
    from apps.utils.llms import LLM_MODELS_COST

    def _parse_cost(cost):
        if isinstance(cost, (int, float)):
            return float(cost)
        elif isinstance(cost, dict) and "value" in cost and "brackets" in cost:
            return float(cost["value"][0])  # Return the lowest tier price
        else:
            raise ValueError("Invalid cost format")
        return cost

    pricing = {
        MODEL_API_NAMES.get(k, k): {
            "input": _parse_cost(v["in"]),
            "output": _parse_cost(v["out"]),
        }
        for k, v in LLM_MODELS_COST.items()
    }

    return pricing


# Model pricing (USD per million tokens)
# Loaded from apps/utils/llms/models.yml
MODEL_PRICING = _load_model_pricing()

# Fields to inspect for typos and semantic issues
# Both codespell and Claude AI will check these fields
CHART_FIELDS_TO_CHECK = [
    "title",
    "subtitle",
    "note",
]

VARIABLE_FIELDS_TO_CHECK = [
    "variable_name",
    "variable_description",
    "variable_title_public",
    "variable_description_short",
    "variable_description_from_producer",
    "variable_description_key",
    "variable_description_processing",
    "variable_unit",
    "variable_title_variant",
    "variable_attribution",
    "variable_attribution_short",
]
