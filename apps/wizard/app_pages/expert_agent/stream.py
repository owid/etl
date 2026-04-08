import asyncio
import queue
import threading
from collections.abc import AsyncIterable
from typing import AsyncGenerator, List, Literal

import streamlit as st
from pydantic import BaseModel, Field
from pydantic_ai import Agent, RunContext
from pydantic_ai.messages import (
    AgentStreamEvent,
    FinalResultEvent,
    FunctionToolCallEvent,
    FunctionToolResultEvent,
    PartDeltaEvent,
    PartStartEvent,
    TextPart,
    TextPartDelta,
    # ThinkingPartDelta,
    # ToolCallPartDelta,
)


########################
# OTHERS               #
########################
class UIStateManager:
    """Manages UI state for cross-thread communication in a thread-safe manner."""

    def __init__(self):
        self._ui_message_queue = None
        self._status_container = None

    def set_ui_message_queue(self, queue_obj):
        """Set the UI message queue for tool messages."""
        self._ui_message_queue = queue_obj

    def set_status_container(self, status_obj):
        """Set the status container for tool messages."""
        self._status_container = status_obj

    def send_ui_message(self, message_type: str, text: str, **kwargs):
        """Send a UI message from a tool function to be displayed in the main thread.

        Args:
            message_type: Type of message ('markdown', 'info', 'warning', 'error', 'success')
            text: The message text
            **kwargs: Additional arguments to pass to the Streamlit function
        """
        if self._ui_message_queue is not None:
            self._ui_message_queue.put({"type": message_type, "text": text, "args": (), "kwargs": kwargs})

    def display_ui_message(self, ui_msg):
        """Display a UI message captured from the worker thread."""
        msg_type = ui_msg["type"]
        text = ui_msg["text"]
        args = ui_msg.get("args", ())
        kwargs = ui_msg.get("kwargs", {})

        # If we have a status container, display messages inside it
        if self._status_container is not None:
            # Write directly to the status container without using context manager
            if msg_type == "markdown":
                self._status_container.markdown(text, *args, **kwargs)
            elif msg_type == "info":
                self._status_container.info(text, *args, **kwargs)
            elif msg_type == "warning":
                self._status_container.warning(text, *args, **kwargs)
            elif msg_type == "error":
                self._status_container.error(text, *args, **kwargs)
            elif msg_type == "success":
                self._status_container.success(text, *args, **kwargs)
        else:
            # Fallback to regular display
            if msg_type == "markdown":
                st.markdown(text, *args, **kwargs)
            elif msg_type == "info":
                st.info(text, *args, **kwargs)
            elif msg_type == "warning":
                st.warning(text, *args, **kwargs)
            elif msg_type == "error":
                st.error(text, *args, **kwargs)
            elif msg_type == "success":
                st.success(text, *args, **kwargs)


# Global instance for backward compatibility
_ui_state_manager = UIStateManager()


# Backward compatibility functions
def tool_ui_message(message_type: str, text: str, **kwargs):
    """Send a UI message from a tool function to be displayed in the main thread."""
    _ui_state_manager.send_ui_message(message_type, text, **kwargs)


def set_status_container(status_obj):
    """Set the global status container for tool messages."""
    _ui_state_manager.set_status_container(status_obj)


def display_ui_message(ui_msg):
    """Display a UI message captured from the worker thread."""
    _ui_state_manager.display_ui_message(ui_msg)


def set_ui_message_queue(queue_obj):
    """Set the global UI message queue."""
    _ui_state_manager.set_ui_message_queue(queue_obj)


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


########################
# CORE STREAMING       #
########################


def _agent_stream_sync(
    agent,
    prompt: str,
    model_name: str,
    message_history,
    func_stream,
    session_updates_callback=None,
    question_id: str | None = None,
):
    text_q: "queue.Queue[AnswerChunk | str | None]" = queue.Queue()
    updates_q: "queue.Queue[dict | None]" = queue.Queue()
    ui_messages_q: "queue.Queue[dict | None]" = queue.Queue()

    async def async_worker():
        try:
            # Set up the UI message queue for tools to use
            _ui_state_manager.set_ui_message_queue(ui_messages_q)

            # Create a custom agent_stream that captures session updates
            async for chunk in func_stream(
                agent,
                prompt,
                model_name,
                message_history,
                updates_q,
                question_id,
            ):
                text_q.put(chunk)
        except Exception as e:
            text_q.put(f"Error: {str(e)}")
        finally:
            text_q.put(None)  # Signal completion
            updates_q.put(None)
            ui_messages_q.put(None)

    def worker():
        asyncio.run(async_worker())

    # Start the async worker in a daemon thread
    thread = threading.Thread(target=worker, daemon=True)
    thread.start()

    # Synchronously yield chunks and handle updates as they become available
    while True:
        # Process all available UI messages first (non-blocking)
        while True:
            try:
                ui_msg = ui_messages_q.get_nowait()
                if ui_msg is not None:
                    display_ui_message(ui_msg)
                else:
                    break
            except queue.Empty:
                break

        # Check for session updates (non-blocking)
        try:
            update = updates_q.get_nowait()
            if update is not None and session_updates_callback:
                session_updates_callback(update)
        except queue.Empty:
            pass

        # Get chunks with a short timeout to allow UI message processing
        try:
            chunk = text_q.get(timeout=0.1)  # 100ms timeout
            if chunk is None:
                break
            yield chunk
        except queue.Empty:
            # No text chunk available yet, continue to check for UI messages
            continue


########################
# REGULAR STREAMING    #
########################


def agent_stream_sync(
    agent, prompt: str, model_name: str, message_history, session_updates_callback=None, question_id: str | None = None
):
    """Synchronous wrapper for agent_stream that works with st.write_stream.

    This bridges the async generator to a sync generator using a queue and thread,
    allowing real-time streaming while maintaining compatibility with Streamlit.

    It includes a function `session_updates_callback` that is called with session state updates originated in a separate thread.

    Args:
        prompt: User prompt
        model_name: Model to use
        message_history: Chat history
        session_updates_callback: Function to call with session state updates
        question_id: Unique identifier for this question/conversation turn
    """

    yield _agent_stream_sync(
        agent=agent,
        prompt=prompt,
        model_name=model_name,
        message_history=message_history,
        func_stream=agent_stream_with_updates,
        session_updates_callback=session_updates_callback,
        question_id=question_id,
    )


async def agent_stream_with_updates(
    agent,
    prompt: str,
    model_name: str,
    message_history,
    updates_queue,
    question_id: str | None = None,
):
    """Stream output that can send session updates through a queue."""
    model = _get_model_from_name(model_name)

    # Prepare deps for tools
    deps = {"question_id": question_id} if question_id else {}

    # Run agent with text output (original behavior)
    async with agent.run_stream(
        prompt,
        model=model,  # type: ignore
        message_history=message_history,
        deps=deps,
        event_stream_handler=event_stream_handler,
    ) as result:
        # Send agent result info through updates queue
        updates_queue.put({"agent_result": result})

        # Yield text chunks
        async for message in result.stream_text(delta=True):
            if message is not None:
                yield message

        # Send final updates (usage info, etc.)
        if hasattr(result, "usage"):
            updates_queue.put({"last_usage": result.usage()})


async def event_stream_handler(
    ctx: RunContext,
    event_stream: AsyncIterable[AgentStreamEvent],
):
    async for event in event_stream:
        if isinstance(event, FunctionToolCallEvent):
            # Show tool call in Streamlit UI
            tool_ui_message(
                "markdown",
                f"**:material/construction: :green[:material/arrow_outward:] Tool call**: `{event.part.tool_name}`, with args {event.part.args}",
            )

        elif isinstance(event, FunctionToolResultEvent):
            # Show tool result in Streamlit UI
            tool_ui_message(
                "markdown",
                f"**:material/construction: :gray[:material/call_received:] Tool return**:  Tool `{event.result.tool_name}` completed",
            )

        elif isinstance(event, FinalResultEvent):
            # Show final result start in Streamlit UI
            tool_ui_message("markdown", "**:material/check: Final Result**: Starting to generate response")


########################
# STRUCTURED STREAMING #
########################
class AnswerChunk(BaseModel):
    """A chunk of structured response content that can be streamed to the user.

    This enables the AI to return rich, mixed content types in a single streaming response.
    Each chunk represents one logical piece of content that will be displayed sequentially.

    GUIDELINES FOR AI USAGE:
    - Break responses into logical chunks for better streaming UX
    - Mix content types naturally (text explanations + supporting Metabase links)
    - Always provide context before sharing Metabase links
    - Use text chunks for analysis, explanations, and conclusions
    - Use metabase_question chunks for links to Metabase questions

    STREAMING BEHAVIOR:
    As the AI generates the response, each chunk will be displayed immediately,
    allowing users to see content appear progressively rather than waiting for
    the complete response.

    Examples:
        Text chunk: AnswerChunk(type="text", content="GDP growth analysis shows...")
        Metabase Question chunk: AnswerChunk(type="metabase_question", content="https://metabase.owid.io/question/123-...")
    """

    type: Literal["text", "metabase_question"] = Field(
        description="Content type:\n"
        "• 'text' - Explanations, analysis, conclusions (supports markdown)\n"
        "• 'metabase_question' - URLs to relevant metabase questions"
    )
    content: str = Field(
        description="The actual content:\n"
        "• For 'text': Written content with optional markdown formatting\n"
        "• For 'metabase_question': Valid URL that adds value to the response"
    )

    @classmethod
    def text(cls, content: str) -> "AnswerChunk":
        """Create a text chunk. Use for explanations, analysis, and conclusions."""
        return cls(type="text", content=content)

    @classmethod
    def link(cls, url: str) -> "AnswerChunk":
        """Create a link chunk. Use for relevant datasets, charts, and resources."""
        return cls(type="metabase_question", content=url)


def agent_stream_sync_structured(
    agent, prompt: str, model_name: str, message_history, session_updates_callback=None, question_id: str | None = None
):
    """Synchronous wrapper for structured output streaming with st.write_stream compatibility.

    Returns structured chunks (AnswerChunk objects) instead of plain text.
    """

    yield _agent_stream_sync(
        agent=agent,
        prompt=prompt,
        model_name=model_name,
        message_history=message_history,
        func_stream=agent_stream_with_updates_structured,
        session_updates_callback=session_updates_callback,
        question_id=question_id,
    )


async def agent_stream_with_updates_structured(
    agent,
    prompt: str,
    model_name: str,
    message_history,
    updates_queue,
    question_id: str | None = None,
):
    """Stream output that can send session updates through a queue. Uses structured output."""
    # Get model
    model = _get_model_from_name(model_name)

    # Prepare deps for tools
    deps = {"question_id": question_id} if question_id else {}

    # Run agent with structured output
    async with agent.run_stream(
        prompt,
        model=model,  # type: ignore
        message_history=message_history,
        output_type=list[AnswerChunk],
        deps=deps,
        # toolsets=toolsets,
    ) as result:
        # Send agent result info through updates queue
        updates_queue.put({"agent_result": result})

        # Yield structured output chunks
        async for chunk in result.stream_structured():
            if chunk is not None:
                yield chunk

        # Send final updates (usage info, etc.)
        if hasattr(result, "usage"):
            updates_queue.put({"last_usage": result.usage()})


########################
# DEPRECATED STREAMING #
########################
async def agent_stream_iter_with_updates(
    agent,
    prompt: str,
    model_name: str,
    message_history,
    updates_queue,
):
    """Stream output that can send session updates through a queue."""
    with st.status("Talking with the expert...", expanded=False) as _:
        st.markdown(
            f"**:material/smart_toy: Agent working**: `{model_name}`",
        )

        model = _get_model_from_name(model_name)

        async with agent.iter(
            prompt,
            model=model,
            message_history=message_history,
            # toolsets=toolsets,
        ) as run:
            updates_queue.put({"agent_result": run.result})

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
                                yield event.delta.content_delta
                            elif isinstance(event, PartStartEvent) and isinstance(event.part, TextPart):
                                yield event.part.content

                elif Agent.is_call_tools_node(node):
                    # print("--- CallToolsNode ---")
                    async with node.stream(run.ctx) as handle_stream:
                        async for event in handle_stream:
                            pass
                            # print(f"Call Event Data: {event!r}")

            # Capture usage and result info
            if hasattr(run, "usage"):
                updates_queue.put({"last_usage": run.usage()})


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
