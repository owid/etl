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

# PAGE CONFIG
## Title/subtitle
st.title(":material/smart_toy: Expert :small[:rainbow[agent mode]]")

## Load variables
load_env()

# SESSION STATE
st.session_state.setdefault("expert_config", {})
st.session_state.setdefault("agent_messages", [])

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


def sample_question() -> str:
    """Sample a random question from SAMPLE_QUESTIONS."""
    import random

    return random.choice(SAMPLE_QUESTIONS)


# Reset chat history
def reset_messages() -> None:
    """Reset messages to default."""
    st.session_state["agent_messages"] = []


def config_model():
    # Model
    model_name = st.selectbox(
        label=":material/memory: Select model",
        options=MODELS_AVAILABLE_LIST,
        format_func=lambda x: MODELS_DISPLAY[x],
        index=MODELS_AVAILABLE_LIST.index(MODEL_DEFAULT),
        help="[Pricing](https://openai.com/api/pricing) | [Model list](https://platform.openai.com/docs/models/)",
    )

    # Add to session state
    st.session_state["expert_config"]["model_name"] = model_name
    # st.session_state["expert_config"]["max_tokens"] = max_tokens
    st.session_state["expert_config"]["temperature"] = 1
    # st.session_state["expert_config"]["use_reduced_context"] = use_reduced_context


def config_others():
    st.button(
        label=":material/restart_alt: Clear chat",
        on_click=reset_messages,
    )


##################################################################
# UI starts here
##################################################################
# Allocate container for chat
container_chat = st.container()

### LLM CONFIG
with st.expander("**Model config**", icon=":material/settings:"):
    # 2/ Model
    st.session_state.analytics = st.session_state.get("analytics", True)
    with st.container(horizontal=True, vertical_alignment="bottom"):
        config_model()
    # 3/ Others
    with st.container(horizontal=True, vertical_alignment="bottom"):
        config_others()


# CHAT INTERFACE
with container_chat:
    # Initialize chat history
    # if "messages" not in st.session_state:
    #     reset_messages()

    # Display chat messages from history on app rerun
    # for message in st.session_state.messages:
    #     if message["role"] != "system":
    #         avatar = None
    #         if message["role"] == "user":
    #             avatar = ":material/person:"
    #         with st.chat_message(message["role"], avatar=avatar):
    #             st.markdown(message["content"])

    # Initialise session state
    st.session_state.response = st.session_state.get("response", None)
    st.session_state.prompt = st.session_state.get("prompt", None)
    st.session_state.feedback_key = st.session_state.get("feedback_key", 0)
    st.session_state.cost_last = st.session_state.get("cost_last", 0)

    # Placeholder for response
    container_response = st.container()

    # React to user input
    # sample_question()
    if prompt := st.chat_input(placeholder="Ask me anything"):
        st.session_state.feedback_key += 1
        # Display user message in chat message container
        with container_response:
            with st.chat_message("user", avatar=":material/person:"):
                st.markdown(prompt)

        # Add user message to chat history
        # st.session_state.messages.append({"role": "user", "content": prompt})

        # Build GPT query (only use the system prompt and latest user input)
        # if st.session_state["expert_config"]["use_reduced_context"]:
        #     messages = [{"role": "system", "content": get_system_prompt()}, {"role": "user", "content": prompt}]
        # else:
        #     messages = st.session_state.messages

        # Display assistant response in chat message container
        with container_response:
            with st.chat_message("assistant"):
                # Start timer
                start_time = time.time()

                # Put agent to work
                st.toast(
                    f"Agent working, model {st.session_state["expert_config"]["model_name"]}...",
                    icon=":material/smart_toy:",
                )

                # Stream the agent response
                # Option 1: Keep existing behavior (uses st.session_state internally)
                stream = agent_stream2(
                    prompt,
                    model_name=st.session_state["expert_config"]["model_name"],
                    message_history=st.session_state["agent_messages"],
                )

                # Option 2: Avoid st.session_state by passing parameters explicitly
                # model_name = st.session_state["expert_config"]["model_name"]
                # def usage_handler(usage_info):
                #     st.session_state["last_usage"] = usage_info
                # stream = agent_stream2(prompt, model_name=model_name, usage_callback=usage_handler)
                st.session_state.response = cast(
                    str,
                    st.write_stream(stream),
                )

                # End timer and store duration
                end_time = time.time()
                st.session_state.response_time = end_time - start_time

                # Show cost and other details
                if "last_usage" in st.session_state:
                    # st.markdown(st.session_state.last_usage)
                    cost = estimate_llm_cost(
                        model_name=st.session_state["expert_config"]["model_name"],
                        input_tokens=st.session_state.last_usage.request_tokens,
                        output_tokens=st.session_state.last_usage.response_tokens,
                    )

                    # Build message
                    response_time = getattr(st.session_state, "response_time", 0)
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

                # Agent execution details
                if "agent_result" in st.session_state:
                    with st.expander("**Agent reasoning details**", expanded=False, icon=":material/auto_awesome:"):
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

                    # Add messages to history
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

            # Add new response by the System
            # st.session_state.messages.append({"role": "assistant", "content": st.session_state.response})

            # Add prompt to session state
            # st.session_state.prompt = prompt

            print("finished asking LLM...")

    # if st.session_state.response:
    #     # Get cost & tokens
    #     text_in = "\n".join([m["content"] for m in st.session_state.messages])
    #     cost, num_tokens = get_cost_and_tokens(
    #         text_in, st.session_state.response, cast(str, st.session_state["expert_config"]["model_name"])
    #     )

    #     # Format response time
    #     response_time = getattr(st.session_state, "response_time", 0)
    #     time_msg = f"⏱️ {response_time:.2f}s"

    #     cost_msg = f"≥{cost:.4f} USD (≥{num_tokens:,} tokens), {time_msg}, using **{MODELS_AVAILABLE[st.session_state['model_name']]}**"
    #     st.session_state.cost_last = cost

    #     # Show cost below feedback
    #     with container_response:
    #         with st.container(horizontal=True, horizontal_alignment="right"):
    #             st.markdown(f":blue-badge[:small[:material/paid: {cost_msg}]]")
