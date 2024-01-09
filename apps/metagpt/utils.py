"""Auxiliary classes, functions and variables."""
from dataclasses import dataclass
from pathlib import Path

import structlog
from openai import OpenAI
from openai.types.chat import ChatCompletion
from openai.types.completion_usage import CompletionUsage
from typing_extensions import Self

log = structlog.get_logger()


class Channels:
    """Channels for metadata files.

    Using this to avoid hardcoding strings."""

    SNAPSHOT = "snapshot"
    GRAPHER = "grapher"
    GARDEN = "garden"


# GPT
GPT_MODEL = "gpt-3.5-turbo"
RATE_PER_1000_TOKENS = 0.0015


# Interface with GPT
@dataclass
class GPTResult:
    """Dataclass with chat GPT result."""

    message_content: str
    cost: float


class OpenAIWrapper(OpenAI):
    """Wrapper for OpenAI API."""

    def __init__(self: Self, **kwargs) -> None:
        """Initialize OpenAI API wrapper."""
        super().__init__(**kwargs)
        self.model = kwargs.get("model", GPT_MODEL)

    def query_gpt(self: Self, **kwargs) -> GPTResult | None:
        """Query Chat GPT to get message content from the chat completion."""
        # Get chat completion
        _ = kwargs.pop("model", None)
        chat_completion = self.chat.completions.create(model=self.model, **kwargs)  # type: ignore
        # Return value only if message content is not None
        if isinstance(chat_completion, ChatCompletion) and isinstance(chat_completion.usage, CompletionUsage):
            chat_completion_tokens = chat_completion.usage.total_tokens
            cost = (chat_completion_tokens / 1000) * RATE_PER_1000_TOKENS
            message_content = chat_completion.choices[0].message.content
            # Log cost
            log.info(f"Chat completion {self.model} cost: {cost}")
            # Build return object
            return GPTResult(
                message_content=message_content,
                cost=cost,
            )
        else:
            raise ValueError("Chat completion is not a ChatCompletion object.")


def _read_metadata_file(path_to_file: str | Path) -> str:
    """Read a metadata file and returns its content."""
    with open(path_to_file, "r") as file:
        return file.read()
