"""Ask chat GPT questions about our our docs.

references:
- https://docs.streamlit.io/knowledge-base/tutorials/build-conversational-apps#build-a-chatgpt-like-app
"""

import json
import time
from typing import cast

import streamlit as st
from pydantic_ai.messages import ModelRequest
from structlog import get_logger

from apps.wizard.app_pages.expert_agent.agent import agent_stream2
from apps.wizard.app_pages.expert_agent.utils import MODELS_AVAILABLE_LIST, MODELS_DISPLAY, estimate_llm_cost
from etl.config import load_env

st.set_page_config(
    page_title="Wizard: Ask the Expert",
    page_icon="🪄",
)

# LOG
log = get_logger()

## Load variables
load_env()

# SESSION STATE
st.session_state.setdefault("expert_config", {})
st.session_state.setdefault("agent_messages", [])
st.session_state.setdefault("response", None)

# Models
## See all of them in https://github.com/pydantic/pydantic-ai/blob/master/pydantic_ai_slim/pydantic_ai/models/__init__.py
MODEL_DEFAULT = "openai:gpt-5"

# Sample questions
SAMPLE_QUESTIONS = [
    "Which are our top 10 articles by pageviews?",
    "How many charts do we have that use only a single indicator?",
    "Do we have datasets whose indicators are not used in any chart?",
    "In the metadata yaml file, which field should I use to disable the map tap view?",
    "In the metadata yaml file, how can I define a common `description_processing` that affects all indicators in a specific table?"
    "What is the difference between `description_key` and `description_from_producer`? Be concise.",
    "Is the following snapshot title correct? 'Cherry Blossom Full Blook Dates in Kyoto, Japan'",
    "What is the difference between an Origin and Dataset?",
]


##################################################################
# Functions
##################################################################
def sample_question() -> str:
    """Sample a random question from SAMPLE_QUESTIONS."""
    import random

    return random.choice(SAMPLE_QUESTIONS)


# Reset chat history
def reset_messages() -> None:
    """Reset messages to default."""
    st.session_state["agent_messages"] = []

def show_usage(response_time: float):
    if "last_usage" in st.session_state:
        # st.markdown(st.session_state.last_usage)
        cost = estimate_llm_cost(
            model_name=st.session_state["expert_config"]["model_name"],
            input_tokens=st.session_state.last_usage.request_tokens,
            output_tokens=st.session_state.last_usage.response_tokens,
        )

        # Build message
        time_msg = f":material/timer: {response_time:.2f}s"
        num_tokens_in = st.session_state.last_usage.request_tokens
        num_tokens_out = st.session_state.last_usage.response_tokens
        num_tokens = st.session_state.last_usage.total_tokens
        model_name = st.session_state["expert_config"]["model_name"]
        cost_msg = f":material/paid: ~{cost:.4f} USD"
        tokens_msg = f"{num_tokens:,} tokens (IN: {num_tokens_in}, OUT: {num_tokens_out})"

        # Print message
        st.markdown(
            f":green-badge[:small[{cost_msg}]] :blue-badge[:small[{tokens_msg}]] :gray-badge[:small[{time_msg}]] :gray-badge[:small[{model_name}]]"
        )

def show_reasoning_details():
    with st.expander("**Reasoning details**", expanded=False, icon=":material/auto_awesome:"):
        messages = st.session_state["agent_result"].all_messages_json()
        messages = json.loads(messages)
        for message in messages:
            parts = message["parts"]
            kind = {"request": "Agent request", "response": "LLM response"}.get(
                message["kind"], message["kind"]
            )
            for part in parts:
                part_kind = part["part_kind"]
                title = f"**{kind}** - {part_kind}"
                if "tool_name" in part:
                    title += f" `{part['tool_name']}`"
                with st.expander(title):
                    st.write(part)

def register_message_history():
    agent_messages = [msg for msg in st.session_state["agent_result"].new_messages()]
    ## TEST: only keep user/system prompts and final responses
    filtered_messages = []
    for msg in agent_messages:
        if hasattr(msg, "parts") and any(part.part_kind in ("user-prompt") for part in msg.parts):
            # Only keep messages that are user prompts
            filtered_messages.append(msg)
        elif hasattr(msg, "kind") and msg.kind == "response":
            # Keep only response messages
            filtered_messages.append(msg)
    st.session_state["agent_messages"].extend(agent_messages)


def show_settings_menu():
    with st.container(horizontal=True, vertical_alignment="bottom"):
        model_name = st.selectbox(
            label=":material/memory: Model",
            options=MODELS_AVAILABLE_LIST,
            format_func=lambda x: MODELS_DISPLAY[x],
            index=MODELS_AVAILABLE_LIST.index(MODEL_DEFAULT),
            help="[Pricing](https://openai.com/api/pricing) | [Model list](https://platform.openai.com/docs/models/)",
            key="expert_model_name"
            # on_change=lambda: st.session_state.setdefault("expert_config", {}).update({"model_name": st.session_state["expert_model_name"]}),
        )
        st.session_state["expert_config"]["model_name"] = model_name
    with st.container(horizontal=True, vertical_alignment="bottom"):
        st.button(
            label=":material/clear_all: Clear chat",
            on_click=reset_messages,
        )

##################################################################
# UI starts here
##################################################################
# Title
container = st.container(horizontal=True, horizontal_alignment="left", vertical_alignment="bottom", width="stretch")
with container:
    ## Title/subtitle
    st.title(":rainbow[:material/smart_toy:] Expert")
    st.badge("agent mode", color="primary")
    # Settings
    model_name = MODELS_DISPLAY.get(st.session_state.get("expert_model_name", MODEL_DEFAULT))
    with st.popover(f"{model_name}", icon=":material/settings:", help="Model settings"):
        show_settings_menu()

# Arrange chat input
prompt = st.chat_input(placeholder=f"Ask me anything)")

# React to user input
if prompt:
    # Display user message in chat message container
    with st.chat_message("user", avatar=":material/person:"):
        st.markdown(prompt)


    # Display assistant response in chat message container
    with st.chat_message("assistant"):
        # Notify user that agent is working
        st.toast(
            f"Agent working, model {st.session_state["expert_config"]["model_name"]}...",
            icon=":material/smart_toy:",
        )
        start_time = time.time()

        # Agent to work, and stream its output
        stream = agent_stream2(
            prompt,
            model_name=st.session_state["expert_config"]["model_name"],
            message_history=st.session_state["agent_messages"],
        )
        st.session_state.response = cast(
            str,
            st.write_stream(stream),
        )

        # Show cost and other details
        show_usage(response_time=time.time() - start_time)

        if "agent_result" in st.session_state:
            # Agent execution details
            show_reasoning_details()
            # Add messages to history
            register_message_history()


        print("finished asking LLM...")

