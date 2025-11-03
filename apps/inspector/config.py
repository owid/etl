"""Configuration for the inspector app."""

# Claude model configuration
# Choose model based on speed vs quality tradeoff:
# - Haiku: Fastest, cheapest (18% faster, 72% cheaper), excellent quality for this task
# - Sonnet: Higher quality but slower and more expensive
# - Opus: Highest quality but much slower and more expensive
#
# Testing showed Haiku finds same semantic issues as Sonnet with minimal quality difference,
# making it the best choice for large-scale analysis.

# Model for individual issue detection (used many times - cost matters)
CLAUDE_MODEL = "claude-haiku-4-5-20251001"  # Fast: $1/M in, $5/M out (RECOMMENDED)
# CLAUDE_MODEL = "claude-sonnet-4-5-20250929"  # Balanced: $3/M in, $15/M out
# CLAUDE_MODEL = "claude-opus-4-20250514"  # Quality: $15/M in, $75/M out

# Model for grouping/pruning (used once per collection - quality matters more than cost)
GROUPING_MODEL = "claude-sonnet-4-5-20250929"  # Better reasoning for filtering false positives
# GROUPING_MODEL = "claude-opus-4-20250514"  # Best quality if budget allows

# Batch size configuration
# Number of views to check per API call. Larger batches are faster and cheaper,
# but batch sizes >100 cause quality degradation (model misses issues).
#
# Testing results (100 views):
# - batch=10:  86s, $0.25 (baseline)
# - batch=30:  31s, $0.17 (2.8x faster, 33% cheaper)
# - batch=50:  18s, $0.15 (4.8x faster, 40% cheaper) ✓ OPTIMAL
# - batch=100: 20s, $0.16 (4.3x faster, 36% cheaper, slight variance)
# - batch=200: Quality degradation - misses semantic issues!
#
# Default of 50 provides best balance of speed, cost, and reliability.
DEFAULT_BATCH_SIZE = 20

# Concurrency limit for API requests
# Anthropic rate limits: typically 5-50 concurrent requests depending on tier
# Increase if you have higher tier access, decrease if you hit rate limits
MAX_CONCURRENT_REQUESTS = 25

# Token limits
GROUPING_MAX_TOKENS = 3_072  # Keep limited for quality

# Model pricing (USD per million tokens)
# Source: https://claude.com/pricing (verified 2025-11-03)
# Note: We use the regular API, not Batch API (which offers 50% discount but is asynchronous)
# Sonnet 4.5 pricing: $3/M in, $15/M out for prompts ≤200K tokens (our use case)
MODEL_PRICING = {
    "claude-haiku-4-5-20251001": {"input": 1.0, "output": 5.0},
    "claude-3-5-haiku-20241022": {"input": 1.0, "output": 5.0},  # Legacy
    "claude-sonnet-4-5-20250929": {"input": 3.0, "output": 15.0},
    "claude-3-7-sonnet-20250219": {"input": 3.0, "output": 15.0},  # Legacy
    "claude-opus-4-20250514": {"input": 15.0, "output": 75.0},
}

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
