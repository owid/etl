import asyncio
import queue
import threading
from typing import AsyncGenerator, List

import streamlit as st
from pydantic_ai import Agent
from pydantic_ai.messages import (
    PartDeltaEvent,
    PartStartEvent,
    TextPart,
    TextPartDelta,
)


def _get_model_from_name(model_name: str):
    """Simple pre-processing of the model name.

    Generally it does nothing but return the input. But, exceptionally, it might return special model objects for certain keywords.
    """
    if model_name == "llama3.2":
        from pydantic_ai.models.openai import OpenAIChatModel
        from pydantic_ai.providers.ollama import OllamaProvider

        model = OpenAIChatModel(
            model_name="llama3.2",
            provider=OllamaProvider(base_url="http://localhost:11434/v1"),
        )

    else:
        model = model_name
    return model


def agent_stream_sync(agent, prompt: str, model_name: str, message_history, session_updates_callback=None):
    """Synchronous wrapper for agent_stream that works with st.write_stream.

    This bridges the async generator to a sync generator using a queue and thread,
    allowing real-time streaming while maintaining compatibility with Streamlit.

    It includes a function `session_updates_callback` that is called with session state updates originated in a separate thread.

    Args:
        prompt: User prompt
        model_name: Model to use
        message_history: Chat history
        session_updates_callback: Function to call with session state updates
    """

    text_q: "queue.Queue[str | None]" = queue.Queue()
    updates_q: "queue.Queue[dict | None]" = queue.Queue()

    async def async_worker():
        try:
            # Create a custom agent_stream that captures session updates
            async for chunk in agent_stream_with_updates(
                agent,
                prompt,
                model_name,
                message_history,
                updates_q,
            ):
                text_q.put(chunk)
        except Exception as e:
            text_q.put(f"Error: {str(e)}")
        finally:
            text_q.put(None)  # Signal completion
            updates_q.put(None)

    def worker():
        asyncio.run(async_worker())

    # Start the async worker in a daemon thread
    thread = threading.Thread(target=worker, daemon=True)
    thread.start()

    # Synchronously yield chunks and handle updates as they become available
    while True:
        # Check for session updates (non-blocking)
        try:
            update = updates_q.get_nowait()
            if update is not None and session_updates_callback:
                session_updates_callback(update)
        except queue.Empty:
            pass

        # Get text chunks (blocking)
        chunk = text_q.get()
        if chunk is None:
            break
        yield chunk


async def agent_stream_with_updates(agent, prompt: str, model_name: str, message_history, updates_queue):
    """Stream output  that can send session updates through a queue."""
    # Get model
    model = _get_model_from_name(model_name)

    # Run agent
    async with agent.run_stream(
        prompt,
        model=model,  # type: ignore
        message_history=message_history,
        # toolsets=toolsets,
    ) as result:
        # Send agent result info through updates queue
        updates_queue.put({"agent_result": result})

        # Yield each message from the stream
        async for message in result.stream_text(delta=True):
            if message is not None:
                yield message

        # Send final updates (usage info, etc.)
        if hasattr(result, "usage"):
            updates_queue.put({"last_usage": result.usage()})


async def agent_stream(agent, prompt: str, model_name: str, message_history) -> AsyncGenerator[str, None]:
    """Stream agent response using run_stream.

    Args:
        prompt: The user prompt to process
        model_name: The model to use.

    Yields:
        str: Text chunks from the agent response
    """
    with st.status("Talking with the expert...", expanded=False) as status:
        st.markdown(
            f"**:material/smart_toy: Agent working**: `{model_name}`",
        )
        model = _get_model_from_name(model_name)

        async with agent.run_stream(
            prompt,
            model=model,  # type: ignore
            message_history=message_history,
            # toolsets=toolsets,
        ) as result:
            # Yield each message from the stream
            async for message in result.stream_text(delta=True):
                if message is not None:
                    yield message

        # At the very end, after the streaming is complete
        # Capture the usage information in session state
        if hasattr(result, "usage"):
            st.session_state["last_usage"] = result.usage()

        status.update(label="Got the answer!", state="complete", expanded=False)


async def _collect_agent_stream2(agent, prompt: str, model_name: str, message_history) -> List[str]:
    """Collect all chunks from agent_stream2 in one async context to avoid task switching issues."""
    chunks = []
    model = _get_model_from_name(model_name)

    async with agent.iter(
        prompt,
        model=model,
        message_history=message_history,
        # toolsets=toolsets,
    ) as run:
        nodes = []
        async for node in run:
            # print(f"--------------------------")
            # print(node)
            nodes.append(node)
            if Agent.is_model_request_node(node):
                # is_final_synthesis_node = any(isinstance(prev_node, CallToolsNode) for prev_node in nodes)
                # print(f"--- ModelRequestNode (Is Final Synthesis? {is_final_synthesis_node}) ---")
                async with node.stream(run.ctx) as request_stream:
                    async for event in request_stream:
                        # print(f"Request Event: Data: {event!r}")
                        if isinstance(event, PartDeltaEvent) and isinstance(event.delta, TextPartDelta):
                            chunks.append(event.delta.content_delta)
                        elif isinstance(event, PartStartEvent) and isinstance(event.part, TextPart):
                            chunks.append(event.part.content)

            elif Agent.is_call_tools_node(node):
                # print("--- CallToolsNode ---")
                async with node.stream(run.ctx) as handle_stream:
                    async for event in handle_stream:
                        pass
                        # print(f"Call Event Data: {event!r}")

        # Capture usage and result info
        if hasattr(run, "usage"):
            st.session_state["last_usage"] = run.usage()
        if hasattr(run, "result"):
            st.session_state["agent_result"] = run.result

    return chunks


async def agent_stream2(agent, prompt: str, model_name: str, message_history) -> AsyncGenerator[str, None]:
    """Stream agent response using iter method with Streamlit-compatible wrapper.

    This version collects all chunks in one async context first, then yields them
    to avoid async context manager issues with Streamlit's task switching.

    Args:
        prompt: The user prompt to process
        model_name: The model to use.
    Yields:
        str: Text chunks from the agent response
    """
    # Collect all chunks first to avoid async context issues with Streamlit
    # with st.spinner("Asking LLM...", show_time=True):
    with st.status("Talking with the expert...", expanded=False) as status:
        st.markdown(
            f"**:material/smart_toy: Agent working**: `{st.session_state['expert_config']['model_name']}`",
        )
        chunks = await _collect_agent_stream2(
            agent,
            prompt,
            model_name,
            message_history,
        )
        status.update(label="Got the answer!", state="complete", expanded=False)

    # Yield chunks one by one
    for chunk in chunks:
        if chunk:  # Only yield non-empty chunks
            yield chunk
