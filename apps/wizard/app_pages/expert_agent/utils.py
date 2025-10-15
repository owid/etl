from pathlib import Path

import yaml
from pydantic import BaseModel, Field
from structlog import get_logger

log = get_logger()

CURRENT_DIR = Path(__file__).parent


# Load available models
## See all of them in https://github.com/pydantic/pydantic-ai/blob/master/pydantic_ai_slim/pydantic_ai/models/__init__.py
with open(CURRENT_DIR / "models.yml", "r") as f:
    MODELS = yaml.safe_load(f)
MODELS_DISPLAY = {m["name"]: m["display_name"] for m in MODELS["models"]}
MODELS_COST = {m["name"]: m["cost"] for m in MODELS["models"]}
MODELS_AVAILABLE_LIST = list(MODELS_DISPLAY.keys())
MODEL_DEFAULT = "openai:gpt-5-mini"


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
        df = pd.DataFrame(MODELS["models"])
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


class DataFrameModel(BaseModel):
    columns: list[str] = Field(description="List of column names in the DataFrame.")
    dtypes: dict[str, str] = Field(description="Dictionary mapping column names to their data types.")
    data: list[list] = Field(description="Sample rows from the DataFrame (limited for performance).")
    total_rows: int = Field(description="Total number of rows in the original DataFrame.")


class QueryResult(BaseModel):
    message: str = Field(
        description="Status message about the query execution. 'SUCCESS' if valid, otherwise error details."
    )
    valid: bool = Field(description="Whether the query executed successfully and returned data.")
    result: DataFrameModel | None = Field(
        default=None, description="The query results as a serialized DataFrame with sample data."
    )
    url_metabase: str | None = Field(
        default=None, description="URL to the created Metabase question for interactive exploration."
    )
    url_datasette: str | None = Field(default=None, description="URL to view the query results in Datasette.")
    card_id_metabase: int | None = Field(
        default=None, description="Metabase card ID that can be used with plotting tools."
    )


def serialize_df(df, num_rows: int | None = None) -> DataFrameModel:
    if num_rows is None:
        df_head = df
    else:
        df_head = df.head(num_rows)

    data = DataFrameModel(
        columns=df.columns.tolist(),
        dtypes={c: str(t) for c, t in df.dtypes.items()},
        data=df_head.to_numpy().tolist(),  # small slice
        total_rows=len(df),
    )
    return data
