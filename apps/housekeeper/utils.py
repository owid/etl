from datetime import date, datetime
from typing import List

from sqlalchemy.orm import Session

from apps.utils.gpt import GPTQuery, GPTResponse, OpenAIWrapper
from etl.config import OWID_ENV
from etl.db import read_sql
from etl.grapher import model as gm

# System prompt to summarize chart information
MODEL = "gpt-4o"
SYSTEM_PROMPT = f"""We are reviewing a chart that was created some time ago. We want to understand if we can remove it from the site, in order to reduce the amount of charts that we have (and hence helping with the maintenance). Your job is to briefly summarize what the chart is about, and to provide some context about the variables used in the chart. You can also check the chart's history to see if it has been edited recently, and the number of views in the last year that it gets (consider that the median of the chart views in last year is ~1300 views; i.e. half of the charts get less than 1300 views).

In your response, consider today's date {date.today().strftime("%Y-%m-%d")}, and provide three blocks:

(i) Chart Description: To this end, look at the information given by the chart configuration parameters, and the different variables used in it. Don't get lost into the details. This should be at most ~3 sentences or so.
(ii) General comment: Based on the information you have, provide a comment on the quality of the chart and its edit activity. You can measure the quality of the chart by checking if there are typos, inconsistencies, and, most importantly, outdated information. Remember that you can compare the views of the chart to the median value. When looking at the activity, focus on whether there have been recent and regular edits. First sentence of your comment should be short and provide the recommended action (e.g. 'Recommended action: Keep the chart', 'Recommended action: Unpublish the chart', 'No action recommended'). Then, follow your recommendation with 2-3 bullet points (use symbol '•') with the reasons. Don't be too verbose, and try to keep your comment concise, short and to the point.
(iii) Edits timeline: Summarize the various edits that the chart has had over time. We are interested in knowing who might be the owner of the chart, so look at the most recent major edits. Provide the list of *all* the edits, with the name of the person who made the edit, and the date (no need to add the hour) of the edit. Sort the list in descending order of the date.

Each block should be formatted with a title in bold (just use one '*'), and a brief text. You can use the following template:

*→ Chart Description*
This chart shows the evolution of the number of COVID-19 cases in the world, by continent. It uses data from the COVID-19 dataset.

*→ General comment*
Recommended action: ...
• Reason 1
• Reason 2
...

*→ Edits timeline*
This chart has been mostly edited by 'Max Roser' and 'Esteban Ortiz-Ospina'. The last edit was done on 2021-01-01 by 'Max Roser'.

1. Edit by 'Max Roser' (date: 2021-01-01)
2. Edit by 'Esteban Ortiz-Ospina' (date: 2020-12-01)
...
"""

# Today and one year ago
TODAY = datetime.today()
YEAR_AGO = TODAY.replace(year=TODAY.year - 1)


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
        return f"*🤖 Summary* (AI-generated with {MODEL})\n{gpt_response.message_content}\n\n(Cost: {cost} $)"


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
    gpt_response = api.query_gpt(query=query, model=MODEL)
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
