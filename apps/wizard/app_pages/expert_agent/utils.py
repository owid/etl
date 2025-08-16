from pathlib import Path

import yaml

CURRENT_DIR = Path(__file__).parent


def estimate_llm_cost(model_name: str, input_tokens: int, output_tokens: int) -> float:
    """Estimate the cost of an LLM interaction.

    Args:
        model_name: Name of the model (e.g., "openai:gpt-5-mini")
        input_tokens: Number of input tokens used
        output_tokens: Number of output tokens generated

    Returns:
        float: Estimated cost in USD

    Raises:
        KeyError: If model_name is not found in MODELS_COST
        ValueError: If token counts are negative
    """
    if input_tokens < 0 or output_tokens < 0:
        raise ValueError("Token counts must be non-negative")

    if model_name not in MODELS_COST:
        raise KeyError(f"Model '{model_name}' not found in MODELS_COST")

    cost_config = MODELS_COST[model_name]

    # Calculate input cost
    input_cost = _calculate_tiered_cost(cost_config["in"], input_tokens)

    # Calculate output cost
    output_cost = _calculate_tiered_cost(cost_config["out"], output_tokens)

    return input_cost + output_cost


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


# Load available models
with open(CURRENT_DIR / "models.yml", "r") as f:
    MODELS = yaml.safe_load(f)
MODELS_DISPLAY = {m["name"]: m["display_name"] for m in MODELS["models"]}
MODELS_COST = {m["name"]: m["cost"] for m in MODELS["models"]}
MODELS_AVAILABLE_LIST = list(MODELS_DISPLAY.keys())
