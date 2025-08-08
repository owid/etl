"""Auxiliary classes, functions and variables."""

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import structlog
import tiktoken
import yaml
from openai import OpenAI
from openai.types.chat import ChatCompletion
from typing_extensions import Self

log = structlog.get_logger()


# GPT
MODEL_DEFAULT = "gpt-4.1-mini"

# PRICING (per 1,000 tokens)
## See pricing list: https://openai.com/api/pricing/ (USD) // Detailed: https://platform.openai.com/docs/pricing
## See model list: https://platform.openai.com/docs/models
RATE_DEFAULT_IN = 0.005
MODEL_EQUIVALENCES = {
    "gpt-3.5-turbo": "gpt-3.5-turbo-0125",
    "gpt-4-turbo-preview": "gpt-4-0125-preview",
    "gpt-4-turbo": "gpt-4-turbo-2024-04-09",
    "gpt-4o": "gpt-4o-2024-08-06",
    "gpt-4o-mini": "gpt-4o-mini-2024-07-18",
    "gpt-4.1": "gpt-4.1-2025-04-14",
    "gpt-4.1-mini": "gpt-4.1-mini-2025-04-14",
    "gpt-4.1-nano": "gpt-4.1-nano-2025-04-14",
    "o1": "o1-2024-12-17",
    "o3": "o3-2025-04-16",
    "o4-mini": "o4-mini-2025-04-16",
    "gpt-5": "gpt-5-2025-08-07",
    "gpt-5-mini": "gpt-5-mini-2025-08-07",
    "gpt-5-nano": "gpt-5-nano-2025-08-07",
}
MODEL_RATES_1000_TOKEN = {
    # GPT 3.5
    "gpt-3.5-turbo-0125": {
        "in": 0.5 / 1000,
        "out": 1.5 / 1000,
    },
    # GPT 4
    "gpt-4-0125-preview": {
        "in": 10 / 1000,
        "out": 30 / 1000,
    },
    "gpt-4": {
        "in": 30 / 1000,
        "out": 60 / 1000,
    },
    "gpt-4-32k": {
        "in": 60 / 1000,
        "out": 120 / 1000,
    },
    # GPT 4 Turbo
    "gpt-4-turbo-2024-04-09": {
        "in": 10 / 1000,
        "out": 30 / 1000,
    },
    # GPT 4o
    "gpt-4o-2024-08-06": {
        "in": 2.5 / 1000,
        "out": 10 / 1000,
    },
    # GPT 4o mini
    "gpt-4o-mini-2024-07-18": {
        "in": 0.150 / 1000,
        "out": 0.600 / 1000,
    },
    # GPT o1
    "o1-2024-12-17": {
        "in": 15 / 1000,
        "out": 60 / 1000,
    },
    # GPT o3
    "o3-2025-04-16": {
        "in": 10 / 1000,
        "out": 40 / 1000,
    },
    # GPT o4-mini
    "o4-mini-2025-04-16": {
        "in": 1.1 / 1000,
        "out": 4.4 / 1000,
    },
    # GPT 4.1
    "gpt-4.1-2025-04-14": {
        "in": 2 / 1000,
        "out": 8 / 1000,
    },
    # GPT 4.1 mini
    "gpt-4.1-mini-2025-04-14": {
        "in": 0.4 / 1000,
        "out": 1.6 / 1000,
    },
    # GPT 4.1 nano
    "gpt-4.1-nano-2025-04-14": {
        "in": 0.1,
        "out": 0.4,
    },
    # GPT 5
    "gpt-5-2025-08-07": {
        "in": 1.25 / 1000,
        "out": 10 / 1000,
    },
    "gpt-5-mini-2025-08-07": {
        "in": 0.25 / 1000,
        "out": 2 / 1000,
    },
    "gpt-5-nano-2025-08-07": {
        "in": 0.05 / 1000,
        "out": 0.4 / 1000,
    },
}
MODEL_RATES_1000_TOKEN = {
    **MODEL_RATES_1000_TOKEN,
    **{new: MODEL_RATES_1000_TOKEN[equivalent] for new, equivalent in MODEL_EQUIVALENCES.items()},
}

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

    message_content_dix: Optional[Dict[str, Any]] = field(default_factory=dict)

    def __init__(self: Self, chat_completion_instance: ChatCompletion | None = None, **kwargs) -> None:  # type: ignore[reportInvalidTypeVarUse]
        """Initialize OpenAI API wrapper."""
        if chat_completion_instance:
            super().__init__(**chat_completion_instance.dict())
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
    def message_content_as_dict(self: Self) -> Dict[str, Any]:
        """Message content from GPT as dictionary."""
        if self.message_content is None:
            raise ValueError("`message_content` is empty!")
        else:
            if self.message_content_dix == {}:
                self.message_content_dix = yaml.safe_load(self.message_content)
            else:
                raise ValueError("`message_content` is empty!")
        return self.message_content_dix  # type: ignore[reportReturnType]

    @property
    def cost(self) -> float | None:
        """Get cost of the complete request (including input and output).

        The cost is given in USD.

        If model is not in MODEL_RATES_1000_TOKEN, None is returned
        """
        if self.usage is None:
            raise ValueError("`usage` is empty!")
        elif self.model not in MODEL_RATES_1000_TOKEN:
            log.info(f"Model {self.model} not registered in MODEL_RATES_1000_TOKEN!")
            return None
        else:
            tokens_in = self.usage.prompt_tokens
            tokens_out = self.usage.completion_tokens
            cost = (
                tokens_in / 1000 * MODEL_RATES_1000_TOKEN[self.model]["in"]
                + tokens_out / 1000 * MODEL_RATES_1000_TOKEN[self.model]["out"]
            )
            return cost


@dataclass
class GPTQuery:
    """Fields for GPT query."""

    messages: List[Dict[str, str]]
    temperature: float = 0

    def estimated_cost(self: Self, model_name: str) -> float:
        """
        Calculate the cost of using GPT based on the number of characters of the message content.

        Note that it only considers the input messages (and not the GPT response).

        Returns:
            float: The estimated cost of using GPT for the given number of tokens (only input).
        """
        # Check that model_name is registered. Else, use the default model
        if model_name not in MODEL_RATES_1000_TOKEN:
            raise ValueError(f"Model {model_name} not registered in MODEL_RATES_1000_TOKEN.")
        # Get rate per 1,000 tokens (USD) for the model
        rate = MODEL_RATES_1000_TOKEN[model_name]["in"]
        # Get the token count
        tokens_count = self.get_number_tokens(model_name)
        # Estimate the cost as token_count x rate
        estimated_cost = tokens_count / 1000 * rate
        return estimated_cost

    def get_number_tokens(self, model_name: str) -> int:
        """Get number of tokens of the message content."""
        token_count = sum([get_number_tokens(message["content"], model_name) for message in self.messages])
        return token_count

    def to_dict(self: Self) -> Dict[str, Any]:
        """Class as dictionary."""
        return {k: v for k, v in asdict(self).items()}


class OpenAIWrapper(OpenAI):
    """Wrapper for OpenAI API."""

    def __init__(self: Self, **kwargs) -> None:  # type: ignore[reportInvalidTypeVarUse]
        """Initialize OpenAI API wrapper."""
        super().__init__(**kwargs)

    def query_gpt(
        self: Self, query: Optional[GPTQuery] = None, model: str = MODEL_DEFAULT, **kwargs
    ) -> GPTResponse | None:
        """Query Chat GPT to get message content from the chat completion."""
        # Get model to be used (+ sanity checks)
        if model not in MODEL_RATES_1000_TOKEN:
            raise ValueError(f"Model {model} not registered in MODEL_RATES_1000_TOKEN!")

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
        if model in ("o3"):
            if "temperature" in kwargs:
                kwargs["temperature"] = 1

        # Get chat completion
        chat_completion = self.chat.completions.create(**kwargs)  # type: ignore

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


def get_cost_and_tokens(text_in: str, text_out: str, model_name: str) -> Tuple[float, float]:
    """Get cost using tiktoken tokenisation."""
    if model_name not in MODEL_RATES_1000_TOKEN:
        raise ValueError(f"Model {model_name} not registered in MODEL_RATES_1000_TOKEN.")
    tokens_in = get_number_tokens(text_in, model_name)
    tokens_out = get_number_tokens(text_out, model_name)
    cost = (
        tokens_in / 1000 * MODEL_RATES_1000_TOKEN[model_name]["in"]
        + tokens_out / 1000 * MODEL_RATES_1000_TOKEN[model_name]["out"]
    )
    return cost, tokens_in + tokens_out
