"""Display news from ETL."""
from datetime import datetime
from typing import Tuple

import streamlit as st
from st_pages import add_indentation

from apps.wizard.pages.expert.prompts import SYSTEM_PROMPT_GENERIC
from apps.wizard.utils.db import WizardDB
from apps.wizard.utils.gpt import GPTQuery, OpenAIWrapper, get_cost_and_tokens

st.set_page_config(page_title="Wizard: News", page_icon="🪄")
add_indentation()
# st.title("🏐 Metadata playground")
st.title("News 🗞️")

# GPT
MODEL_NAME = "gpt-4-turbo-preview"


@st.cache_data()
def load_pr():
    """Check and load news."""
    # Load latest PR data
    data = WizardDB.get_pr(num_days=7)
    data = data.sort_values("DATE_MERGED", ascending=False)
    data = data[data["MERGED"] == 1]
    return data


def _clean_date(dt_raw: str) -> str:
    """Show date nicely."""
    return datetime.strptime(dt_raw, "%Y-%m-%dT%XZ").strftime("%a %d %b, %Y")


def render_expander(record):
    """Render expander for each PR."""
    with st.expander(f"{record['TITLE']} `by @{record['USERNAME']}`", expanded=False):
        # st.markdown(
        #     f"[See PR]({record['URL_HTML']})",
        # )
        st.markdown(f"[Go to Pull Request]({record['URL_HTML']})  |  [See diff]({record['URL_DIFF']})")
        if record["DESCRIPTION"]:
            st.markdown(record["DESCRIPTION"])


@st.cache_data(show_spinner=False)
def ask_gpt(df) -> Tuple[str, float, int]:
    """Ask GPT for news."""
    SYSTEM_PROMPT = f"""You will be given a markdown table with the pull requests merged in the etl repository in the last 7 days.

    Summarise the main updates and interesting points from the pull requests. Use markdown syntax in your reply if needed.

    To refer to users, use their username.

    {SYSTEM_PROMPT_GENERIC}
    """
    USER_PROMPT = f"{df.to_markdown()}"
    # Ask Chat GPT
    api = OpenAIWrapper()

    query = GPTQuery(
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": USER_PROMPT},
        ]
    )
    response = api.query_gpt(query=query, model=MODEL_NAME)

    summary = response.message_content  # type: ignore
    cost, num_tokens = get_cost_and_tokens(SYSTEM_PROMPT + USER_PROMPT, summary, MODEL_NAME)
    return summary, cost, num_tokens  # type: ignore


tab_1, tab_2 = st.tabs(["Pull Requests: Summary", "Pull Requests: Last 7 days"])
# Get records
df_pr = load_pr()
records = df_pr.to_dict(orient="records")

with tab_1:
    # Summary
    st.header("Pull Requests: Summary")
    with st.popover("Config"):
        st.selectbox("Time window for summary", options=["Last 24 hours", "Last 7 days"])
    with st.spinner():
        summary, cost, num_tokens = ask_gpt(df_pr)
        st.markdown(summary)
        st.divider()
        st.info(f"Cost: ${cost:.2f}\nNumber tokens: {num_tokens}")

with tab_2:
    # Show last 7 day PR timeline
    st.header("Pull Requests: Last 7 days")
    LAST_DATE_HEADER = None
    for record in records:
        date_str = _clean_date(record["DATE_MERGED"])
        if (LAST_DATE_HEADER is None) or (LAST_DATE_HEADER != date_str):
            st.subheader(date_str)
            LAST_DATE_HEADER = date_str
        render_expander(record)
