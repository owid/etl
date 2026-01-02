from genai_prices import Usage, calc_price
from pydantic_ai.usage import UsageBase

from apps.utils.llms.models import LLM_MODELS

# Get costs
LLM_MODELS_COST: dict[str, dict[str, float | dict[str, list[float]]]] = {
    m["name"]: m["cost"] for m in LLM_MODELS["models"]
}


def estimate_llm_cost(
    model_name: str,
    usage: Usage | UsageBase | None = None,
    input_tokens: int | None = None,
    output_tokens: int | None = None,
) -> float:
    """Estimate the cost of an LLM interaction.

    Args:
        model_name: Name of the model (e.g., "openai:gpt-5-mini")
        usage: Usage of tokens. This is a typical pydantic-ai object (result.usage())
        input_tokens: Number of input tokens used. If given, together with output_tokens, it overrides `usage`.
        output_tokens: Number of output tokens generated

    Returns:
        float: Estimated cost in USD

    Raises:
        KeyError: If model_name is not found in MODELS_COST
        ValueError: If token counts are negative
    """
    # Check `usage` or `input_tokens`/`output_tokens`
    if (input_tokens is not None) and (output_tokens is not None):
        usage = Usage(
            input_tokens=input_tokens,
            output_tokens=output_tokens,
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
        model_name_list = model_name.split(":")
        assert len(model_name_list) == 2, "Model name must be in 'provider:model' format"
        provider_id = model_name_list[0]
        model_name = model_name_list[1]
    else:
        provider_id = None

    # Use genai_prices to calculate cost
    price_data = calc_price(
        usage,
        model_ref=model_name,
        provider_id=provider_id,
    )

    # Convert to float
    total_cost = float(price_data.total_price)

    return total_cost


def _calculate_tiered_cost(cost_config, tokens: int) -> float:
    """Calculate cost based on tiered pricing (like tax brackets).

    Args:
        cost_config: Cost configuration (either float or dict with value/brackets)
        tokens: Number of tokens to calculate cost for

    Returns:
        float: Total cost for the given token count
    """
    # Simple fixed pricing
    if isinstance(cost_config, (int, float)):
        return (tokens / 1_000_000) * float(cost_config)

    # Tiered pricing
    if isinstance(cost_config, dict) and "value" in cost_config and "brackets" in cost_config:
        values = cost_config["value"]
        brackets = cost_config["brackets"]

        assert len(values) == len(brackets), "Values and brackets must match in length"

        total_cost = 0.0
        remaining_tokens = tokens

        # Process each bracket like tax brackets
        for i in range(len(brackets)):
            bracket_start = brackets[i]
            bracket_end = brackets[i + 1] if i + 1 < len(brackets) else float("inf")

            if remaining_tokens <= 0:
                break

            # Calculate tokens in this bracket
            tokens_in_bracket = (
                min(remaining_tokens, bracket_end - bracket_start) if bracket_end != float("inf") else remaining_tokens
            )

            if tokens_in_bracket > 0:
                # Add cost for this bracket
                bracket_cost_per_1m = float(values[i])
                total_cost += (tokens_in_bracket / 1_000_000) * bracket_cost_per_1m
                remaining_tokens -= tokens_in_bracket

        return total_cost

    raise ValueError(f"Invalid cost configuration: {cost_config}")


def generate_pricing_text():
    """
    Input example:
    ```
    {'models':
        [
            {
                'name': 'openai:gpt-5',
                'display_name': 'GPT-5',
                'cost': {'in': 1.25, 'out': 10.0}
            },
        {
            'name': 'openai:gpt-5-mini',
            'display_name': 'GPT-5 mini',
            'cost': {'in': 0.25, 'out': 2.0}
        }
        ]
    }
    ```

    OUTPUT is a markdown table with the following format:
    ```
    | Model | Input (USD/1M tokens) | Output (USD/1M tokens) |
    |-------|-----------------------|------------------------|
    | GPT-5 | 1.25                  | 10.0                   |
    | GPT-5 mini | 0.25             | 2.0                    |
    ```
    """

    def _bake_cost(cost_config):
        """Helper function to format cost configuration."""
        if isinstance(cost_config, (int, float)):
            return f"{cost_config:.2f}"
        elif isinstance(cost_config, dict) and "value" in cost_config and "brackets" in cost_config:
            text = []
            values = cost_config["value"]
            brackets = cost_config["brackets"]
            assert len(values) == len(brackets), "Values and brackets must match in length"
            for i, (value, bracket) in enumerate(zip(values, brackets)):
                if isinstance(value, (int, float)):
                    if i == len(brackets) - 1:
                        value = f"{value:.2f}"
                        text_part = f"{value} (≥{bracket:,} tokens)"
                    else:
                        value = f"{value:.2f}"
                        text_part = f"{value} ({brackets[i]:,} ≤ {brackets[i + 1]:,} tokens)"
                    text.append(text_part)
                else:
                    raise ValueError(f"Invalid value type: {value}")
            text = ", ".join(text)
            return text
        else:
            return str(cost_config)

    import pandas as pd

    try:
        df = pd.DataFrame(LLM_MODELS["models"])
        df["Input (USD/1M tokens)"] = df["cost"].apply(lambda x: _bake_cost(x["in"]))
        df["Output (USD/1M tokens)"] = df["cost"].apply(lambda x: _bake_cost(x["out"]))
        df = df.rename(columns={"display_name": "Model"})[
            [
                "Model",
                "Input (USD/1M tokens)",
                "Output (USD/1M tokens)",
            ]
        ]
    except Exception as _:  # type: ignore
        return "Error generating pricing table: {e}"

    table = df.to_markdown(index=False, tablefmt="pipe", floatfmt=".2f")

    pricing_links = [
        "[OpenAI Pricing](https://openai.com/api/pricing)",
        "[Anthropic Pricing](https://www.anthropic.com/pricing#api)",
        "[Google Cloud Pricing](https://ai.google.dev/pricing)",
    ]

    pricing_list = "\n- ".join(pricing_links)
    text = f"##### Pricing \n{table}\n\nUp-to-date pricing links:\n- {pricing_list}"
    return text
