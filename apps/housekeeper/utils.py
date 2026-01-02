from datetime import date
from pathlib import Path
from typing import List

import yaml
from sqlalchemy.orm import Session

from apps.utils.llms.gpt import GPTQuery, GPTResponse, OpenAIWrapper
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


def get_reviews_id(object_type: str):
    with Session(OWID_ENV.engine) as session:
        return gm.HousekeeperReview.load_reviews_object_id(session, object_type=object_type)


def get_charts_with_slug_rename_last_year():
    query = f"""SELECT chart_id FROM chart_slug_redirects WHERE createdAt >= '{YEAR_AGO.strftime('%Y-%m-%d')}'"""
    df = OWID_ENV.read_sql(query)
    return df["chart_id"].tolist()


def add_reviews(object_type: str, object_id: int):
    with Session(OWID_ENV.engine) as session:
        gm.HousekeeperReview.add_review(
            session=session,
            object_type=object_type,
            object_id=object_id,
        )


def get_indicators_in_chart(chart_id) -> List[gm.Variable]:
    with Session(OWID_ENV.engine) as session:
        variables = gm.Variable.load_variables_in_chart(session, chart_id)
        return variables


def get_chart_revisions(chart_id):
    query = f"""
    SELECT u.fullName, c.config, c.createdAt FROM chart_revisions c
    LEFT JOIN users u ON u.id = c.userId
    WHERE c.chartId={chart_id};
    """
    df = read_sql(query)
    return df


def get_chart_summary(chart):
    # Get variables used in chart
    variables = get_indicators_in_chart(chart["chart_id"])
    variable_description = _get_summary_variables(variables)

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
    user_prompt = f"1) Chart config:\n{config}\n{'='*20}\n2) {variable_description}\n{'='*20}\n3) Timeline edits:\n{edit_summary}\n4) Chart views: {num_chart_views}"

    # Query GPT
    gpt_response = ask_gpt(user_prompt)

    # Response with cost
    if gpt_response is not None:
        if gpt_response.cost is not None:
            cost = round(gpt_response.cost, 4)
        else:
            cost = "unknown"
        return f"*🤖 Summary* (AI-generated with {MODEL_DEFAULT})\n{gpt_response.message_content}\n\n(Cost: {cost} $)"


def ask_gpt(user_prompt) -> GPTResponse | None:
    """Get AI summary of a chart based on user prompt."""
    api = OpenAIWrapper()
    query = GPTQuery(
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0,
    )
    gpt_response = api.query_gpt(query=query, model=MODEL_DEFAULT)
    return gpt_response


def _get_summary_variable(variable):
    """String description of a variable."""
    s = f"""VariableId: {variable.id}
name: {variable.name}
unit: {variable.unit}
description: {variable.descriptionShort if variable.descriptionShort is not None else variable.description}"""
    return s


def _get_summary_variables(variables):
    """String description of all variables."""
    s = "Summary of the variables, by variableId:\n"
    for variable in variables:
        s += "---------------\n"
        s += _get_summary_variable(variable)

    return s
