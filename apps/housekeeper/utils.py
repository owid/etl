from datetime import date, datetime
from pathlib import Path

import structlog
import yaml
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
def get_chart_summary(chart):
    """Summarize chart details with LLM.

    Generates a message to be shared on Slack.
    """
    # Get variables used in chart
    variables = get_indicators_in_chart(chart["chart_id"])
    variable_description = _get_summary_indicators(variables)

    # Get last chart configuration
    df = get_chart_revisions(chart["chart_id"])
    config = df.sort_values(by="createdAt").iloc[-1]["config"]

    # Get all chart revisions
    edit_summary = ""
    for _, row in df.iterrows():
        edit_summary += f"- Edit by '{row['fullName']}'  (date: {row['createdAt']})\n"
        edit_summary += f"  chart config: {row['config']}\n"

    # Get chart views
    num_chart_views = chart["views_365d"]

    # Prepare user prompt
    user_prompt = f"1) Chart config:\n{config}\n{'='*20}\n2) {variable_description}\n{'='*20}\n3) Timeline edits:\n{edit_summary}\n4) Total views in the last 365 days: {num_chart_views}"

    # Query GPT
    result = ask_llm(user_prompt)

    if result is not None:
        usage = result.usage()
        cost = estimate_llm_cost(
            MODEL_DEFAULT,
            usage=usage,
        )

        message = f"*🤖 Summary* ({MODEL_DEFAULT})\n{result.output}\n\n(Cost: {cost} $)"

        return message


def get_indicators_in_chart(chart_id) -> list[gm.Variable]:
    with Session(OWID_ENV.engine) as session:
        variables = gm.Variable.load_variables_in_chart(session, chart_id)
        return variables


def _get_summary_indicators(variables):
    """String description of all variables."""
    s = "Summary of the variables, by variableId:\n"
    for variable in variables:
        s += "---------------\n"
        s += _get_summary_indicator(variable)

    return s


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


def ask_llm(user_prompt: str, system_prompt: str | None = None, model: str | None = None):
    """Get AI summary using pydantic-ai instead of OpenAI library.

    Args:
        user_prompt: The user's prompt/question
        system_prompt: Optional system prompt. Defaults to SYSTEM_PROMPT if not provided
        model: Optional model name. Defaults to MODEL_DEFAULT if not provided

    Returns:
        SimpleNamespace with:
            - message_content: The AI response text
            - cost: Cost in USD (None if unavailable)
    """
    log = structlog.get_logger()

    # Use defaults if not provided
    if system_prompt is None:
        system_prompt = SYSTEM_PROMPT
    if model is None:
        model = MODEL_DEFAULT

    # Create agent with system prompt
    agent = Agent(
        model=model,
        instructions=system_prompt,
        retries=2,
    )

    try:
        # Run the agent synchronously
        return agent.run_sync(user_prompt)
    except Exception as e:
        log.error(f"Error querying pydantic-ai: {e}")
        return None
