"""LLM cost estimates, priced from the `genai-prices` table (https://github.com/pydantic/genai-prices).

Keep prices out of this repo: when a model is missing or a price is wrong, upgrade the package
(`uv lock --upgrade-package genai-prices`) or fix it upstream.
"""

from genai_prices import Usage, calc_price
from pydantic_ai.usage import UsageBase


def estimate_llm_cost(
    model_name: str,
    usage: Usage | UsageBase | None = None,
    input_tokens: int | None = None,
    output_tokens: int | None = None,
    cache_read_tokens: int = 0,
    cache_write_tokens: int = 0,
) -> float:
    """Estimate the cost of an LLM interaction.

    Args:
        model_name: Name of the model, optionally with a pydantic-ai provider prefix (e.g. "openai:gpt-5-mini").
        usage: Usage of tokens. This is a typical pydantic-ai object (result.usage())
        input_tokens: Number of input tokens used, *including* cache reads and writes. If given, together with
            output_tokens, it overrides `usage`.
        output_tokens: Number of output tokens generated
        cache_read_tokens: Number of input tokens read from the prompt cache (only used with `input_tokens`).
        cache_write_tokens: Number of input tokens written to the prompt cache (only used with `input_tokens`).

    Returns:
        float: Estimated cost in USD

    Raises:
        LookupError: If genai-prices has no price for the model
        ValueError: If token counts are missing or negative
    """
    # Check `usage` or `input_tokens`/`output_tokens`
    if (input_tokens is not None) and (output_tokens is not None):
        usage = Usage(
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cache_read_tokens=cache_read_tokens,
            cache_write_tokens=cache_write_tokens,
        )
    if usage is None:
        raise ValueError("Either `usage` or both `input_tokens` and `output_tokens` must be provided")

    # Sanity checks
    if usage.input_tokens is None or usage.output_tokens is None:
        raise ValueError("Usage must have both input_tokens and output_tokens defined")

    if usage.input_tokens < 0 or usage.output_tokens < 0:
        raise ValueError("Token counts must be non-negative")

    # Get provider id
    if ":" in model_name:
        provider_id, model_name = model_name.split(":", 1)
    else:
        provider_id = None

    return float(calc_price(usage, model_ref=model_name, provider_id=provider_id).total_price)
