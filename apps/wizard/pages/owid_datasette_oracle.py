"""Ask chat GPT for help writing datasette queries.
"""
from typing import Any, Dict, cast

import streamlit as st
from st_pages import add_indentation
from streamlit_feedback import streamlit_feedback
from structlog import get_logger

from apps.wizard.pages.owid_datasette_oracle_prompt import OWID_DATASETTE_ORACLE_PROMPT
from apps.wizard.utils import set_states
from apps.wizard.utils.db import DB_IS_SET_UP, WizardDB
from apps.wizard.utils.gpt import OpenAIWrapper, get_cost_and_tokens
from etl.config import load_env

# LOG
log = get_logger()

# CONFIG
st.set_page_config(page_title="OWID Datasette Oracle", page_icon="🔮")
add_indentation()
## Title/subtitle
st.title("**OWID Datasette oracle** 🔮")
st.markdown("Get help writing SQL queries for Datasette!")

## Load variables
load_env()


@st.cache_data(show_spinner=True)
def ask_gpt(query, model):
    response = api.query_gpt(query, model=model)
    return response


# GPT CONFIG
MODEL_DEFAULT = "gpt-4-turbo-preview"
MODELS_AVAILABLE = {
    "gpt-3.5-turbo-0125": "GPT-3.5 Turbo (gpt-3.5-turbo-0125)",
    "gpt-4-turbo-preview": "GPT-4 Turbo (gpt-4-turbo-preview)",
}
MODELS_AVAILABLE_LIST = list(MODELS_AVAILABLE.keys())


# Handle feedback
def handle_feedback(feedback: Dict[str, Any]) -> None:
    """Handle feedback."""
    print("handle feedback")
    print(feedback)
    # st.write(feedback)
    # st.write(st.session_state.prompt)
    # st.write(st.session_state.response)
    WizardDB().add_usage(
        question=st.session_state.messages[-2]["content"],
        answer=st.session_state.response,
        feedback=1 if feedback["score"] == "👍" else 0,
        feedback_text=feedback.get("text", None),
        cost=st.session_state.cost_last,
    )


# Switch category function
def get_system_prompt() -> str:
    """Get appropriate system prompt."""
    return OWID_DATASETTE_ORACLE_PROMPT


# Reset chat history
def reset_messages() -> None:
    """Reset messages to default."""
    set_states(
        {
            "messages": [{"role": "system", "content": get_system_prompt()}],
            "response": None,
            "prompt": None,
        }
    )


## Examples
EXAMPLE_QUERIES = [
    "> Which are our top 10 articles by pageviews?",
    "> How many charts do we have that use only a single indicator?",
    "> Do we have datasets whose indicators are not used in any chart?",
]
with st.popover("See examples"):
    for example in EXAMPLE_QUERIES:
        st.markdown(example)

# Sidebar with GPT config
st.session_state.analytics = st.session_state.get("analytics", True)
with st.sidebar:
    st.button(
        label="Clear chat",
        on_click=reset_messages,
    )
    st.divider()
    st.toggle(
        label="Collect data for analytics",
        value=True,
        on_change=lambda: set_states(
            {
                "analytics": not st.session_state.analytics,
            }
        ),
        help="If enabled, we will collect usage data to improve the app. \n\nThis **is really helpful to improve** how we query chat GPT: E.g. which system prompt to use, optimise costs, and much more 😊. \n\nData collected: questions, responses and feedback submitted. \n\nYou can see how this data is collected [here](https://github.com/owid/etl/blob/master/apps/wizard/utils/db.py). \n\nRecords are anonymous.",
    )
    st.divider()
    st.markdown("## GPT Configuration")
    model_name = st.selectbox(
        label="Select GPT model",
        options=MODELS_AVAILABLE_LIST,
        format_func=lambda x: MODELS_AVAILABLE[x],
        index=MODELS_AVAILABLE_LIST.index(MODEL_DEFAULT),
        help="[Pricing](https://openai.com/pricing) | [Model list](https://platform.openai.com/docs/models/gpt-4-and-gpt-4-turbo)",
    )
    ## See pricing list: https://openai.com/pricing (USD)
    ## See model list: https://platform.openai.com/docs/models/gpt-4-and-gpt-4-turbo

    use_reduced_context = st.toggle(
        "Reduced context window",
        value=False,
        help="If checked, only the last user message will be accounted (i.e less tokens and therefore cheaper).",
    )
    temperature = st.slider(
        "Temperature",
        min_value=0.0,
        max_value=2.0,
        value=0.15,
        step=0.01,
        help="What sampling temperature to use, between 0 and 2. Higher values like 0.8 will make the output more random, while lower values like 0.2 will make it more focused and deterministic.",
    )
    max_tokens = int(
        st.number_input(
            "Max tokens",
            min_value=32,
            max_value=4096,
            value=4096,
            step=32,
            help="The maximum number of tokens in the response.",
        )
    )

# API with OPENAI
api = OpenAIWrapper()

# ACTUAL APP
# Initialize chat history
if "messages" not in st.session_state:
    reset_messages()

# DEGUG
# st.write([m for m in st.session_state.messages if m["role"] != "system"])

# Display chat messages from history on app rerun
for message in st.session_state.messages:
    if message["role"] != "system":
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

# Initialise session state
st.session_state.response = st.session_state.get("response", None)
st.session_state.prompt = st.session_state.get("prompt", None)
st.session_state.feedback_key = st.session_state.get("feedback_key", 0)
st.session_state.cost_last = st.session_state.get("cost_last", 0)

# React to user input
if prompt := st.chat_input("Ask me!"):
    st.session_state.feedback_key += 1
    print("asking GPT...")
    # Display user message in chat message container
    with st.chat_message("user"):
        st.markdown(prompt)

    # Add user message to chat history
    st.session_state.messages.append({"role": "user", "content": prompt})

    # Build GPT query (only use the system prompt and latest user input)
    if use_reduced_context:
        messages = [{"role": "system", "content": get_system_prompt()}, {"role": "user", "content": prompt}]
    else:
        messages = st.session_state.messages

    # Display assistant response in chat message container
    with st.chat_message("assistant"):
        # Ask GPT (stream)
        stream = api.chat.completions.create(
            model=cast(str, model_name),
            messages=messages,  # type: ignore
            temperature=temperature,
            max_tokens=max_tokens,
            stream=True,
        )
        st.session_state.response = cast(str, st.write_stream(stream))

        # Add new response by the System
        st.session_state.messages.append({"role": "assistant", "content": st.session_state.response})

        # Add prompt to session state
        st.session_state.prompt = prompt

        print("finished asking GPT...")

if st.session_state.response:
    # Get cost & tokens
    text_in = "\n".join([m["content"] for m in st.session_state.messages])
    cost, num_tokens = get_cost_and_tokens(text_in, st.session_state.response, cast(str, model_name))
    cost_msg = f"**Cost**: ≥{cost} USD.\n\n **Tokens**: ≥{num_tokens}."
    st.session_state.cost_last = cost

    if DB_IS_SET_UP and st.session_state.analytics:
        # Get feedback only if DB is properly setup
        feedback = streamlit_feedback(
            feedback_type="thumbs",
            optional_text_label="[Optional] Please provide an explanation",
            key=f"feedback_{st.session_state.feedback_key}",
            on_submit=handle_feedback,
        )
    # Show cost below feedback
    st.info(cost_msg)

# DEBUG
# st.write([m for m in st.session_state.messages if m["role"] != "system"])
