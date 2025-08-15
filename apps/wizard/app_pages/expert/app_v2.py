"""Ask chat GPT questions about our our docs.

references:
- https://docs.streamlit.io/knowledge-base/tutorials/build-conversational-apps#build-a-chatgpt-like-app
"""

import time
from typing import cast

import streamlit as st
from pydantic_ai import Agent
from structlog import get_logger

from apps.wizard.app_pages.expert.agent import agent
from apps.wizard.app_pages.expert.model_settings import CHAT_CATEGORIES
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

# Models
## See all of them in https://github.com/pydantic/pydantic-ai/blob/master/pydantic_ai_slim/pydantic_ai/models/__init__.py
MODEL_DEFAULT = "openai:gpt-5-mini"
MODELS_AVAILABLE = {
    "openai:gpt-5-mini": "GPT-5 mini",
    "openai:gpt-5": "GPT-5",
    "openai:gpt-4o": "GPT-4o",
    "openai:o3": "GPT o3",
    "anthropic:claude-sonnet-4-0": "Claude Sonnet 4.0",
    "google-gla:gemini-2.5-flash": "Gemini 2.5 Flash",
}
MODELS_AVAILABLE_LIST = list(MODELS_AVAILABLE.keys())

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
    st.exception(Exception("This hasn't been implemented yet"))


# Configuration functions
def config_category():
    # CONFIG part 1: category
    st.segmented_control(
        label="Choose a category for the question",
        options=CHAT_CATEGORIES,
        default=CHAT_CATEGORIES[1],
        help="Choosing a specific domain reduces the cost of the query to chatGPT, because only a subset of the documentation (i.e. fewer tokens used) will be used in the query.",
        key="category_gpt",
        on_change=reset_messages,
        width="stretch",
    )


def config_model():
    # Model
    model_name = st.selectbox(
        label=":material/memory: Select model",
        options=MODELS_AVAILABLE_LIST,
        format_func=lambda x: MODELS_AVAILABLE[x],
        index=MODELS_AVAILABLE_LIST.index(MODEL_DEFAULT),
        help="[Pricing](https://openai.com/api/pricing) | [Model list](https://platform.openai.com/docs/models/)",
    )
    # Max tokens
    # max_tokens = int(
    #     st.number_input(
    #         "Max tokens",
    #         min_value=32,
    #         max_value=4 * 4096,
    #         value=4096,
    #         step=32,
    #         help="The maximum number of tokens in the response.",
    #     )
    # )
    # # Reduced context
    # use_reduced_context = st.toggle(
    #     "Low context",
    #     value=False,
    #     help="If checked, only the last user message will be accounted (i.e less tokens and therefore cheaper).",
    # )

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
    # 1/ Category
    # with st.container(horizontal=True, vertical_alignment="bottom"):
    #     config_category()
    # 2/ Model
    st.session_state.analytics = st.session_state.get("analytics", True)
    with st.container(horizontal=True, vertical_alignment="bottom"):
        config_model()
    # 3/ Others
    # with st.container(horizontal=True, vertical_alignment="bottom"):
    #     config_others()

from pydantic_ai import Agent, RunContext
from pydantic_ai.agent import CallToolsNode
from pydantic_ai.messages import PartDeltaEvent, TextPartDelta
from pydantic_graph.nodes import BaseNode, End

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
                    icon=":material/robot:",
                )

                async def agent_stream():
                    async with agent.run_stream(
                        prompt,
                        model=st.session_state["expert_config"]["model_name"],
                    ) as result:
                        # """Stream agent response."""
                        # Yield each message from the stream
                        async for message in result.stream_text(delta=True):
                            if message is not None:
                                yield message

                    # At the very end, after the streaming is complete
                    # Capture the usage information in session state
                    if hasattr(result, "usage"):
                        st.session_state["last_usage"] = result.usage()

                async def agent_stream2():
                    # ref: https://github.com/pydantic/pydantic-ai/issues/1007#issuecomment-2963469441
                    async with agent.iter(
                        prompt,
                        model=st.session_state["expert_config"]["model_name"],
                    ) as run:
                        nodes = []
                        # """Stream agent response."""
                        # Yield each message from the stream
                        async for node in run:
                            nodes.append(node)
                            if Agent.is_model_request_node(node):
                                is_final_synthesis_node = any(
                                    isinstance(prev_node, CallToolsNode) for prev_node in nodes
                                )  # Heuristic: check if tools were called before
                                print(f"--- ModelRequestNode (Is Final Synthesis? {is_final_synthesis_node}) ---")
                                async with node.stream(run.ctx) as request_stream:
                                    async for event in request_stream:
                                        print(f"Request Event: Data: {event!r}")
                                        # Specifically track TextPartDelta for the final node
                                        if (
                                            is_final_synthesis_node
                                            and isinstance(event, PartDeltaEvent)
                                            and isinstance(event.delta, TextPartDelta)
                                        ):
                                            yield event.delta.content_delta

                            elif Agent.is_call_tools_node(node):
                                print("--- CallToolsNode ---")
                                async with node.stream(run.ctx) as handle_stream:
                                    async for event in handle_stream:
                                        print(f"Call Event: Data: {event!r}")

                    # At the very end, after the streaming is complete
                    # Capture the usage information in session state
                    if hasattr(run, "usage"):
                        st.session_state["last_usage"] = run.usage()

                    # if not yielded:
                    #     raise exceptions.AgentRunError("Agent run finished without producing a final result")

                # text = agent.run_sync(prompt)

                # st.text(text)
                st.session_state.response = cast(str, st.write_stream(agent_stream))

                if "last_usage" in st.session_state:
                    st.info(st.session_state["last_usage"])
                    # st.info(agent.model)
                # We'll gather partial text to show incrementally
                # partial_text = ""
                # message_placeholder = st.empty()

                # # Render partial text as it arrives
                # async for chunk in result.stream_text(delta=True):
                #     partial_text += chunk
                #     message_placeholder.markdown(partial_text)

                # End timer and store duration
                end_time = time.time()
                st.session_state.response_time = end_time - start_time

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
