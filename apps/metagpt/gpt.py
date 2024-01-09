"""Auxiliary classes, functions and variables."""
from dataclasses import asdict, dataclass
from typing import Any, Dict, List

import structlog
import yaml
from openai import OpenAI
from openai.types.chat import ChatCompletion
from openai.types.completion_usage import CompletionUsage
from typing_extensions import Self

log = structlog.get_logger()


# GPT
GPT_MODEL = "gpt-3.5-turbo"
RATE_PER_1000_TOKENS = 0.0015  # Approximate average cost per 1000 tokens from here - https://openai.com/pricing


# Interface with GPT
@dataclass
class GPTResponse:
    """Dataclass with chat GPT response."""

    message_content: str
    cost: float
    _message_contnent_dix: Dict[str, Any] | None = None

    @property
    def message_content_as_dict(self: Self) -> Dict[str, Any]:
        """Message content from GPT as dictionary."""
        if self._message_contnent_dix is None:
            self._message_contnent_dix = yaml.safe_load(self.message_content)
        else:
            raise ValueError("`message_content` is empty!")
        return self._message_contnent_dix


@dataclass
class GPTQuery:
    """Fields for GPT query."""

    messages: List[Dict[str, str]]
    temperature: float = 0

    @property
    def estimated_cost(self: Self) -> float:
        """
        Calculate the cost of using GPT based on the number of characters of the message content.

        This function estimates the cost of using GPT by converting the number of characters into tokens,
        rounding up to the nearest thousand tokens, and then multiplying by the rate per thousand tokens.

        Returns:
            float: The estimated cost of using GPT for the given number of characters.
        """
        char_count = len(self.messages[0]["content"])  # type: ignore
        tokens = char_count / 4  # Average size of a token is 4 characters
        tokens_rounded_up = -(-tokens // 1000) * 1000  # Round up to the nearest 1000 tokens
        estimated_cost = (tokens_rounded_up / 1000) * RATE_PER_1000_TOKENS
        return estimated_cost

    def to_dict(self: Self) -> Dict[str, Any]:
        """Class as dictionary."""
        return {k: v for k, v in asdict(self).items()}


class OpenAIWrapper(OpenAI):
    """Wrapper for OpenAI API."""

    def __init__(self: Self, **kwargs) -> None:
        """Initialize OpenAI API wrapper."""
        super().__init__(**kwargs)
        self.model = kwargs.get("model", GPT_MODEL)

    def query_gpt(self: Self, query: GPTQuery | None = None, **kwargs) -> GPTResponse | None:
        """Query Chat GPT to get message content from the chat completion."""
        # Get chat completion
        _ = kwargs.pop("model", None)
        if query:
            kwargs = {
                "model": self.model,
                **query.to_dict(),
            }
        else:
            kwargs = {
                "model": self.model,
                **kwargs,
            }
        chat_completion = self.chat.completions.create(**kwargs)  # type: ignore
        # Return value only if message content is not None
        if isinstance(chat_completion, ChatCompletion) and isinstance(chat_completion.usage, CompletionUsage):
            chat_completion_tokens = chat_completion.usage.total_tokens
            cost = (chat_completion_tokens / 1000) * RATE_PER_1000_TOKENS
            message_content = chat_completion.choices[0].message.content
            # Log cost
            log.info(f"Chat completion {self.model} cost: {cost}")
            # Build return object
            return GPTResponse(
                message_content=message_content,
                cost=cost,
            )
        else:
            raise ValueError("Chat completion is not a ChatCompletion object.")
