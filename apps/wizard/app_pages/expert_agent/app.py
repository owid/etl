"""Ask chat GPT questions about our our docs.

references:
- https://docs.streamlit.io/knowledge-base/tutorials/build-conversational-apps#build-a-chatgpt-like-app
"""

import json
import time
from datetime import datetime
from typing import cast

import pytz
import streamlit as st
from pydantic_core import to_json
from structlog import get_logger

from apps.wizard.app_pages.expert_agent.agent import agent_stream2, recommender_agent
from apps.wizard.app_pages.expert_agent.utils import (
    MODELS_AVAILABLE_LIST,
    MODELS_DISPLAY,
    estimate_llm_cost,
    generate_pricing_text,
)
from etl.config import load_env

st.set_page_config(
    page_title="Wizard: Ask the Expert",
    page_icon="🪄",
    layout="centered",
)

# LOG
log = get_logger()

# Config
AVATAR_PERSON = ":material/person:"

## Load variables
load_env()

# SESSION STATE
st.session_state.setdefault("expert_config", {})
st.session_state.setdefault("agent_messages", [])
st.session_state.setdefault("recommended_question", None)
# st.session_state.setdefault("expert_use_mcp", True)
# Models
## See all of them in https://github.com/pydantic/pydantic-ai/blob/master/pydantic_ai_slim/pydantic_ai/models/__init__.py
MODEL_DEFAULT = "openai:gpt-5-mini"


##################################################################
# Functions
##################################################################
# Reset chat history
def reset_messages() -> None:
    """Reset messages to default."""
    st.session_state["agent_messages"] = []


def show_usage(response_time: float):
    if "last_usage" in st.session_state:
        # st.markdown(st.session_state.last_usage)
        cost = estimate_llm_cost(
            model_name=st.session_state["expert_config"]["model_name"],
            input_tokens=st.session_state.last_usage.input_tokens,
            output_tokens=st.session_state.last_usage.output_tokens,
        )

        # Build message
        time_msg = f"{response_time:.2f}s"
        num_tokens_in = st.session_state.last_usage.input_tokens
        num_tokens_out = st.session_state.last_usage.output_tokens
        num_tokens = st.session_state.last_usage.total_tokens
        model_name = st.session_state["expert_config"]["model_name"]
        cost_msg = f"~{cost:.4f} USD"
        tokens_msg = f"{num_tokens:,} tokens (IN: {num_tokens_in}, OUT: {num_tokens_out})"

        # Print message
        with st.container(horizontal=True, vertical_alignment="bottom", horizontal_alignment="left"):
            # Cost
            st.markdown(
                f":blue-badge[:small[:material/paid: {cost_msg}]]",
                help="Accumulated cost of the interaction with the LLM.",
                width="content",
            )
            # Tokens
            st.markdown(
                f":blue-badge[:small[{tokens_msg}]]",
                help="Accumulated number of tokens of the interaction with the LLM.",
                width="content",
            )
            # Timer
            st.badge(
                time_msg,
                color="blue",
                icon=":material/timer:",
            )
            # Model name
            st.badge(
                model_name,
                color="gray",
                icon=":material/memory:",
            )


def _load_history_messages():
    messages = []
    if "agent_result" in st.session_state:
        messages = st.session_state["agent_result"].all_messages_json()
        messages = json.loads(messages)
    return messages


# @st.dialog(title="**:material/auto_awesome: Reasoning details**", width="large",)
def show_reasoning_details_dialog():
    messages = _load_history_messages()
    # st.write(messages)
    num_parts = sum(len(message["parts"]) for message in messages)
    counter_parts = 0
    counter_questions = 0
    for _, message in enumerate(messages):
        parts = message["parts"]
        kind = {"request": "Agent request", "response": "LLM response"}.get(message["kind"], message["kind"])
        for _, part in enumerate(parts):
            counter_parts += 1
            part_kind = part["part_kind"]
            title = f"({counter_parts}/{num_parts}) **{kind}** - {part_kind}"
            if "tool_name" in part:
                title += f" `{part['tool_name']}`"
            if part_kind == "user-prompt":
                counter_questions += 1
                st.badge(f"Question {counter_questions}")
            with st.expander(title):
                st.write(part)


@st.fragment
def show_debugging_details():
    messages = _load_history_messages()
    config = st.session_state.get("expert_config", {})
    mcp_use = st.session_state.get("expert_use_mcp", None)
    usage = st.session_state.get("last_usage", {})
    if usage != {}:
        usage = to_json(usage)
        usage = json.loads(usage)

    data = {
        "model_config": config,
        "mcp_use": mcp_use,
        "num_messages": len(messages),
        "messages": messages,
        "usage": usage,
    }
    json_string = json.dumps(data)

    st.download_button(
        label="Download session (JSON)",
        data=json_string,
        file_name=f"session-expert-{datetime.now(pytz.utc).strftime('%Y%m%d_%H%M_%s')}.json",
        mime="application/json",
        icon=":material/download:",
        help="Download the session data as a JSON file for debugging purposes.\n\n**:material/warning: This file contains your session chat history, model configuration, and usage statistics. Don't share this file with the public, as it may contain sensitive information.**",
    )


def register_message_history():
    agent_messages = [msg for msg in st.session_state["agent_result"].new_messages()]
    ## TEST: only keep user/system prompts and final responses
    # filtered_messages = []
    # for msg in agent_messages:
    #     if hasattr(msg, "parts") and any(part.part_kind in ("user-prompt") for part in msg.parts):
    #         # Only keep messages that are user prompts
    #         filtered_messages.append(msg)
    #     elif hasattr(msg, "kind") and msg.kind == "response":
    #         # Keep only response messages
    #         filtered_messages.append(msg)
    st.session_state["agent_messages"].extend(agent_messages)


def build_history_chat():
    # Load messages
    messages = st.session_state["agent_messages"]
    messages = to_json(messages)
    messages = json.loads(messages)
    # Display messages
    chat_history = []
    for message in messages:
        parts = message["parts"]
        kind = message["kind"]
        for part in parts:
            part_kind = part["part_kind"]
            if (kind == "request") and (part_kind == "user-prompt"):
                if "content" in part:
                    chat_history.append(
                        {
                            "kind": "user",
                            "content": part["content"],
                        }
                    )
            elif (kind == "response") and (part_kind == "text"):
                if "content" in part:
                    chat_history.append(
                        {
                            "kind": "assistant",
                            "content": part["content"],
                        }
                    )
    return chat_history


def show_history_chat():
    chat_history = build_history_chat()
    for message in chat_history:
        avatar = AVATAR_PERSON if message["kind"] == "user" else None
        with st.chat_message(message["kind"], avatar=avatar):
            st.markdown(message["content"])


def show_settings_menu():
    with st.container(horizontal=True, vertical_alignment="bottom"):
        model_name = st.selectbox(
            label=":material/memory: Model",
            options=MODELS_AVAILABLE_LIST,
            format_func=lambda x: MODELS_DISPLAY[x],
            index=MODELS_AVAILABLE_LIST.index(MODEL_DEFAULT),
            help=generate_pricing_text(),
            key="expert_model_name",
            # on_change=lambda: st.session_state.setdefault("expert_config", {}).update({"model_name": st.session_state["expert_model_name"]}),
        )
        st.session_state["expert_config"]["model_name"] = model_name
    st.toggle(
        label="Use OWID mcp",
        value=True,
        key="expert_use_mcp",
        help="Use MCPs to access and interact with OWID's data. :material/warning: Note: This feature is new, disable it if you are experiencing any issues.",
    )
    with st.container(horizontal=True, vertical_alignment="bottom"):
        st.button(
            label=":material/clear_all: Clear chat",
            on_click=reset_messages,
        )


def show_suggestions():
    # with st.spinner("Generating recommended questions..."):
    # t0 = time.time()
    chat_history = build_history_chat()
    text = ""
    for msg in chat_history:
        text += f"{msg['kind']}\n========\n{msg['content']}\n"  # type: ignore

    result = recommender_agent.run_sync(
        user_prompt=text,
    )

    with st.container(border=False):
        st.markdown("**What would you like next?**")
        for question in result.output:
            st.button(
                question,
                on_click=lambda q=question: st.session_state.update({"recommended_question": q}),
                icon=":material/assistant:",
                width="stretch",
            )


##################################################################
# UI starts here
##################################################################
# Title
container = st.container(
    horizontal=True, horizontal_alignment="left", vertical_alignment="bottom", width="stretch", border=False
)
with container:
    ## Title/subtitle
    with st.container():
        st.title(":rainbow[:material/smart_toy:] Expert")
        # st.caption("Expert can make mistakes")
    # st.badge("agent mode", color="primary")
    # Settings
    st.badge("**beta** preview", color="orange")
    model_name = MODELS_DISPLAY.get(st.session_state.get("expert_model_name", MODEL_DEFAULT))
    with st.popover(f"{model_name}", icon=":material/settings:", help="Model settings"):
        show_settings_menu()

# Arrange chat input
prompt = st.chat_input(
    placeholder="Ask anything. Expert can make mistakes.",
)

if st.session_state["recommended_question"]:
    prompt = st.session_state["recommended_question"]

# Display history of messages
if "agent_messages" in st.session_state:
    show_history_chat()

# React to user input
if prompt:
    # Display user message in chat message container
    with st.chat_message("user", avatar=AVATAR_PERSON):
        st.markdown(prompt)

    # Display assistant response in chat message container
    with st.chat_message("assistant"):
        # Notify user that agent is working
        # st.toast(
        #     f"**Agent working**: `{st.session_state['expert_config']['model_name']}`...",
        #     icon=":material/smart_toy:",
        # )
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

        response_time = time.time() - start_time
        print("finished asking LLM...")

        # Show usage and reasoning details
        container_summary = st.container(border=True)
        ## Show cost and other details
        with container_summary:
            show_usage(response_time=response_time)

        if "agent_result" in st.session_state:
            ## Agent execution details
            with container_summary:
                container_buttons = st.container(
                    horizontal=True, vertical_alignment="bottom", horizontal_alignment="distribute"
                )
                with container_buttons:
                    with st.popover(
                        "**Reasoning details**",
                        icon=":material/auto_awesome:",
                        width="stretch",
                    ):
                        show_reasoning_details_dialog()
                    show_debugging_details()
            ## Add messages to history
            register_message_history()

    # Get recommendations
    try:
        show_suggestions()
    except Exception as _:
        pass

    st.session_state["recommended_question"] = None
