"""Configuration for the inspector app."""

# Claude model configuration
# Choose model based on speed vs quality tradeoff:
# - Haiku: Fastest and cheapest, but misses too many semantic issues to be the default
#   (false-negative rate was unacceptable on chart-text reviews).
# - Sonnet: Balanced — current default. Good ratio of catches to cost.
# - Opus: Highest quality, noticeably slower and more expensive. Reach for it on
#   high-stakes audits where you can absorb the cost.

# Mapping from pydantic-ai model names to Anthropic API model names
MODEL_API_NAMES = {
    "anthropic:claude-haiku-4-5": "claude-haiku-4-5-20251001",
    "anthropic:claude-sonnet-4-6": "claude-sonnet-4-6",
    "anthropic:claude-opus-4-7": "claude-opus-4-7",
}

# Model for individual issue detection (used many times - cost matters)
CLAUDE_MODEL = MODEL_API_NAMES["anthropic:claude-sonnet-4-6"]  # Balanced (DEFAULT): $3/M in, $15/M out
# CLAUDE_MODEL = MODEL_API_NAMES["anthropic:claude-haiku-4-5"]  # Fast: $1/M in, $5/M out
# CLAUDE_MODEL = MODEL_API_NAMES["anthropic:claude-opus-4-7"]  # Quality: $5/M in, $25/M out

# Model for grouping/pruning (used once per collection - quality matters more than cost)
GROUPING_MODEL = MODEL_API_NAMES["anthropic:claude-sonnet-4-6"]  # Better reasoning for filtering false positives
# GROUPING_MODEL = MODEL_API_NAMES["anthropic:claude-opus-4-7"]  # Best quality if budget allows

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
