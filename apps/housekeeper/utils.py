import json
from datetime import date, datetime
from pathlib import Path
from typing import Any, Literal

import pandas as pd
import structlog
import yaml
from pydantic import BaseModel, Field
from pydantic_ai import Agent
from sqlalchemy.orm import Session

from apps.utils.llms import estimate_llm_cost
from etl.config import OWID_ENV
from etl.db import read_sql
from etl.grapher import model as gm

# Today and one year ago
TODAY = date.today()
YEAR_AGO = TODAY.replace(year=TODAY.year - 1)

# Load LLM configuration
CURRENT_DIR = Path(__file__).parent
with open(CURRENT_DIR / "config.yaml", "r") as f:
    CONFIG = yaml.safe_load(f)

# System prompt to summarize chart information
MODEL_DEFAULT = CONFIG["charts"]["llm"]["model_name"]
SYSTEM_PROMPT = CONFIG["charts"]["llm"]["system_prompt"].format(TODAY=TODAY.strftime("%Y-%m-%d"))


#####################################
# Pydantic Models                   #
#####################################
class ChartRevision(BaseModel):
    """A single revision of a chart."""

    editor_name: str = Field(description="Name of the person who made the edit")
    timestamp: datetime = Field(description="When the edit was made")
    config_diff: dict[str, Any] = Field(description="Fields that changed: {path: {old: ..., new: ...}}")
    config_diff_char_count: int = Field(description="Number of characters changed in the diff")


class VariableInfo(BaseModel):
    """Description of a variable/indicator used in the chart."""

    name: str = Field(description="Name of the indicator")
    unit: str = Field(description="Unit of measurement")
    description: str = Field(description="Short description of the indicator")


class ChartSummaryInput(BaseModel):
    """Structured input for chart summary generation."""

    config: dict[str, Any] = Field(description="Current chart configuration")
    variables: list[VariableInfo] = Field(description="Variables/indicators used in the chart")
    revisions: list[ChartRevision] = Field(description="Edit history with diffs")
    views_365d: int = Field(description="Number of views in the last 365 days")


class ChartSuggestion(BaseModel):
    """Structured suggestion for chart action."""

    action: Literal["Keep chart", "Unpublish chart", "Improve chart"] = Field(
        description="Recommended action for the chart"
    )
    reasons: list[str] = Field(description="List of reasons motivating the suggested action")


class ChartSummaryOutput(BaseModel):
    """Structured output from chart summary LLM."""

    description: str = Field(description="2-3 sentence description of what the chart shows")
    suggestion: ChartSuggestion = Field(description="Recommended action with reasons")


#####################################
# Get / Submit Housekeeper reviews  #
#####################################
def owidb_get_reviews_id(object_type: str, since_year_ago: bool = True) -> list[int]:
    """Get IDs of objects (e.g. charts) that have been suggested for review by Housekeeper.

    Args:
        object_type: Type of object (e.g., 'chart')
        since_year_ago: If True, only return reviews from the last year (allows re-review after 1 year)

    Returns:
        List of object IDs that have been reviewed
    """
    since = datetime.combine(YEAR_AGO, datetime.min.time()) if since_year_ago else None
    with Session(OWID_ENV.engine) as session:
        return gm.HousekeeperReview.load_reviews_object_id(session, object_type=object_type, since=since)


def owidb_submit_review_id(object_type: str, object_id: int):
    """Submit a review suggestion to HousekeeperReview table in MySQL."""
    with Session(OWID_ENV.engine) as session:
        gm.HousekeeperReview.add_review(
            session=session,
            object_type=object_type,
            object_id=object_id,
        )


#####################################
# LLM                               #
#####################################
def get_chart_summary(chart) -> dict[str, str | int | float] | None:
    """Summarize chart details with LLM using structured input/output.

    Generates a message to be shared on Slack.

    Args:
        chart: Dict with chart_id and views_365d keys

    Returns:
        Formatted Slack message with chart summary, or None if LLM call fails
    """
    # Get variables used in chart
    variables = get_indicators_in_chart(chart["chart_id"])
    variable_infos = [
        VariableInfo(
            name=v.name,
            unit=v.unit or "",
            description=v.descriptionShort or v.description or "",
        )
        for v in variables
    ]

    # Get revisions with diffs
    df = get_chart_revisions(chart["chart_id"])
    revisions = build_revisions_from_df(df)

    # Current config (last revision)
    df_sorted = df.sort_values("createdAt")
    config = json.loads(df_sorted.iloc[-1]["config"])

    # Build structured input
    input_data = ChartSummaryInput(
        config=config,
        variables=variable_infos,
        revisions=revisions,
        views_365d=int(chart["views_365d"]),
    )

    # Query LLM with structured output (pretty-printed for readability)
    result = ask_llm(
        user_prompt=input_data.model_dump_json(indent=2),
        output_type=ChartSummaryOutput,
    )

    # Format response for Slack
    if result is not None:
        output: ChartSummaryOutput = result.output
        cost = estimate_llm_cost(
            MODEL_DEFAULT,
            usage=result.usage(),
        )

        return {
            "description": output.description,
            "suggestion": output.suggestion,
            "cost": cost,
        }

    return None


def pretty_model_name(model_name: str) -> str:
    """Convert pydantic model name to pretty format for display."""
    model = model_name.split(":")
    if len(model) == 1:
        return model[0]
    elif len(model) == 2:
        return model[1]
    else:
        raise ValueError(f"Unexpected model name format: {model_name}")


MODEL_DEFAULT_PRETTY = pretty_model_name(MODEL_DEFAULT)


def get_indicators_in_chart(chart_id) -> list[gm.Variable]:
    with Session(OWID_ENV.engine) as session:
        variables = gm.Variable.load_variables_in_chart(session, chart_id)
        return variables


def _get_summary_indicators(variables):
    """String description of all variables."""
    description = []
    for variable in variables:
        description.append(
            {
                "name": variable.name,
                "unit": variable.unit,
                "description": variable.descriptionShort
                if variable.descriptionShort is not None
                else variable.description,
            }
        )

    return description


def _get_summary_indicator(variable):
    """String description of a variable."""
    s = f"""VariableId: {variable.id}
name: {variable.name}
unit: {variable.unit}
description: {variable.descriptionShort if variable.descriptionShort is not None else variable.description}"""
    return s


def get_chart_revisions(chart_id):
    query = f"""
    SELECT u.fullName, c.config, c.createdAt FROM chart_revisions c
    LEFT JOIN users u ON u.id = c.userId
    WHERE c.chartId={chart_id};
    """
    df = read_sql(query)
    return df


def compute_config_diff(old: Any, new: Any, path: str = "") -> dict[str, Any]:
    """Compute deep recursive difference between two values.

    Returns nested dict with changes at each path:
    - {"old": value, "new": value} for changed values
    - {"added": value} for new fields
    - {"removed": value} for removed fields
    """
    diff = {}

    # Both are dicts - recurse
    if isinstance(old, dict) and isinstance(new, dict):
        all_keys = set(old.keys()) | set(new.keys())
        for key in all_keys:
            key_path = f"{path}.{key}" if path else key
            if key not in old:
                diff[key_path] = {"added": new[key]}
            elif key not in new:
                diff[key_path] = {"removed": old[key]}
            else:
                nested_diff = compute_config_diff(old[key], new[key], key_path)
                diff.update(nested_diff)

    # Both are lists - compare element by element
    elif isinstance(old, list) and isinstance(new, list):
        max_len = max(len(old), len(new))
        for i in range(max_len):
            idx_path = f"{path}[{i}]"
            if i >= len(old):
                diff[idx_path] = {"added": new[i]}
            elif i >= len(new):
                diff[idx_path] = {"removed": old[i]}
            else:
                nested_diff = compute_config_diff(old[i], new[i], idx_path)
                diff.update(nested_diff)

    # Leaf values - compare directly
    elif old != new:
        diff[path or "root"] = {"old": old, "new": new}

    return diff


def build_revisions_from_df(df: pd.DataFrame) -> list[ChartRevision]:
    """Build structured revisions with config diffs from revision DataFrame.

    Args:
        df: DataFrame with columns: fullName (str), config (JSON str), createdAt (datetime64)

    Returns:
        List of ChartRevision objects with computed diffs between consecutive revisions.
    """
    # Sort by date ascending
    df = df.sort_values("createdAt").reset_index(drop=True)

    revisions = []
    prev_config = None

    for _, row in df.iterrows():
        current_config = json.loads(row["config"])

        if prev_config is None:
            # First revision - no diff, it's the initial state
            diff = {}
            char_count = 0
        else:
            diff = compute_config_diff(prev_config, current_config)
            char_count = len(json.dumps(diff))

        revisions.append(
            ChartRevision(
                editor_name=row["fullName"] or "Unknown",
                timestamp=row["createdAt"],
                config_diff=diff,
                config_diff_char_count=char_count,
            )
        )
        prev_config = current_config

    return revisions


def ask_llm(
    user_prompt: str,
    system_prompt: str | None = None,
    output_type: type | None = None,
):
    """Get AI response using pydantic-ai.

    Args:
        user_prompt: The user's prompt/question
        system_prompt: Optional system prompt. Defaults to SYSTEM_PROMPT if not provided
        output_type: Optional Pydantic model class for structured output

    Returns:
        RunResult object with .output (str or structured type) and .usage() method
    """
    log = structlog.get_logger()

    # Use defaults if not provided
    if system_prompt is None:
        system_prompt = SYSTEM_PROMPT

    # Create agent with system prompt and optional structured output
    agent = Agent(
        model=MODEL_DEFAULT,
        instructions=system_prompt,
        output_type=output_type,
        retries=2,
    )

    try:
        # Run the agent synchronously
        return agent.run_sync(user_prompt)
    except Exception as e:
        log.error(f"Error querying pydantic-ai: {e}")
        return None
