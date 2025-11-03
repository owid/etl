from apps.inspector.config import CLAUDE_MODEL, MODEL_PRICING


def calculate_cost(
    input_tokens: int, output_tokens: int, cache_creation_tokens: int = 0, cache_read_tokens: int = 0
) -> float:
    """Calculate cost in USD for given token counts including cache costs.

    Args:
        input_tokens: Number of regular input tokens (non-cached)
        output_tokens: Number of output tokens
        cache_creation_tokens: Number of tokens written to cache
        cache_read_tokens: Number of tokens read from cache

    Returns:
        Cost in USD
    """
    if CLAUDE_MODEL not in MODEL_PRICING:
        raise ValueError(f"Unknown model '{CLAUDE_MODEL}'. Please add pricing to MODEL_PRICING dictionary.")
    pricing = MODEL_PRICING[CLAUDE_MODEL]

    # Regular input tokens
    regular_input_tokens = input_tokens - cache_creation_tokens - cache_read_tokens
    input_cost = (regular_input_tokens / 1_000_000) * pricing["input"]

    # Cache creation cost (25% premium over regular input)
    cache_write_cost = (cache_creation_tokens / 1_000_000) * pricing["input"] * 1.25

    # Cache read cost (90% discount, so 10% of regular price)
    cache_read_cost = (cache_read_tokens / 1_000_000) * pricing["input"] * 0.01

    # Output cost
    output_cost = (output_tokens / 1_000_000) * pricing["output"]

    return input_cost + cache_write_cost + cache_read_cost + output_cost
