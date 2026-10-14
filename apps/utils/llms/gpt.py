"""Auxiliary classes, functions and variables.

This is mainly focused on GPT interaction. Please use pydantic-ai for LLMs now.
"""

from dataclasses import asdict, dataclass, field
from typing import Any

import structlog
import tiktoken
import yaml
from openai import OpenAI
from openai.types.chat import ChatCompletion
from typing_extensions import Self

from apps.utils.llms.costs import estimate_llm_cost

log = structlog.get_logger()


# GPT
MODEL_DEFAULT = "gpt-4.1-mini"

# HACK
# ref: https://github.com/openai/tiktoken/issues/395
MODELS_TOKEN_ENCODINGS = {
    "gpt-4.1": "o200k_base",
    "gpt-4.1-mini": "o200k_base",
    "gpt-4.1-nano": "o200k_base",
    "o1": "o200k_base",
    "o3": "o200k_base",
    "o4-mini": "o200k_base",
    "gpt-5": "o200k_base",
    "gpt-5-mini": "o200k_base",
    "gpt-5-nano": "o200k_base",
}


# Interface with GPT
class GPTResponse(ChatCompletion):
    """GPT response."""

    message_content_dix: dict[str, Any] | None = field(default_factory=dict)

    def __init__(self: Self, chat_completion_instance: ChatCompletion | None = None, **kwargs) -> None:  # ty: ignore
        """Initialize OpenAI API wrapper."""
        if chat_completion_instance:
            super().__init__(**chat_completion_instance.dict())  # ty: ignore
        else:
            super().__init__(**kwargs)

        # Check
        if not isinstance(self.message_content, str):
            raise ValueError("Chat completion is not a ChatCompletion object.")

    @property
    def message_content(self) -> str:
        """Get message content from first choice."""
        content = self.choices[0].message.content
        if content:
            return content
        else:
            raise ValueError("`message_content` is empty!")

    @property
    def message_content_as_dict(self: Self) -> dict[str, Any]:
        """Message content from GPT as dictionary."""
        if self.message_content is None:
            raise ValueError("`message_content` is empty!")
        else:
            if self.message_content_dix == {}:
                self.message_content_dix = yaml.safe_load(self.message_content)
            else:
                raise ValueError("`message_content` is empty!")
        return self.message_content_dix  # ty: ignore[invalid-return-type]


@dataclass
class GPTQuery:
    """Fields for GPT query."""

    messages: list[dict[str, str]]
    temperature: float = 0

    def get_number_tokens(self, model_name: str) -> int:
        """Get number of tokens of the message content."""
        token_count = sum([get_number_tokens(message["content"], model_name) for message in self.messages])
        return token_count

    def to_dict(self: Self) -> dict[str, Any]:
        """Class as dictionary."""
        return {k: v for k, v in asdict(self).items()}


class OpenAIWrapper(OpenAI):
    """Wrapper for OpenAI API."""

    def __init__(self: Self, **kwargs) -> None:  # ty: ignore
        """Initialize OpenAI API wrapper."""
        super().__init__(**kwargs)

    def query_gpt(
        self: Self, query: GPTQuery | None = None, model: str = MODEL_DEFAULT, **kwargs
    ) -> GPTResponse | None:
        """Query Chat GPT to get message content from the chat completion."""
        # Build query from query object + model name
        if query:
            kwargs = {
                "model": model,
                **query.to_dict(),
            }
        else:
            kwargs = {
                "model": model,
                **kwargs,
            }

        # Hotfix for temperature values
        if model in {"o3"} or model.startswith("gpt-5"):
            if "temperature" in kwargs:
                kwargs["temperature"] = 1

        # Get chat completion
        chat_completion = self.chat.completions.create(**kwargs)  # ty: ignore

        # Build response
        if isinstance(chat_completion, ChatCompletion):
            response = GPTResponse(chat_completion)
            return response
        else:
            raise ValueError("message_content is expected to be a string!")

    def query_gpt_fast(self, user_prompt: str, system_prompt: str, model: str = MODEL_DEFAULT) -> str:
        """Query Chat GPT to get message content from the chat completion."""
        query = GPTQuery(
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ]
        )
        response = self.query_gpt(query=query, model=model)

        if isinstance(response, GPTResponse):
            return response.message_content
        else:
            raise ValueError("message_content is expected to be a string!")


def get_number_tokens(text: str, model_name: str) -> int:
    """Get number of tokens of text.

    Note that tokens here is not equivalent to words. The number of tokens changes depending
    on the model used.

    More info: https://openai.com/pricing#language-models
    """
    if model_name in MODELS_TOKEN_ENCODINGS:
        encoding = tiktoken.get_encoding(MODELS_TOKEN_ENCODINGS[model_name])
    else:
        encoding = tiktoken.encoding_for_model(model_name)
    token_count = len(encoding.encode(text))
    return token_count


def get_cost_and_tokens(text_in: str, text_out: str, model_name: str) -> tuple[float, float]:
    """Get cost using tiktoken tokenisation."""
    tokens_in = get_number_tokens(text_in, model_name)
    tokens_out = get_number_tokens(text_out, model_name)
    cost = estimate_llm_cost(f"openai:{model_name}", input_tokens=tokens_in, output_tokens=tokens_out)
    return cost, tokens_in + tokens_out
