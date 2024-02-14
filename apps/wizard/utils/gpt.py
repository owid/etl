"""Auxiliary classes, functions and variables."""
from dataclasses import asdict, dataclass
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

import structlog
import tiktoken
import yaml
from openai import OpenAI
from openai.types.chat import ChatCompletion
from typing_extensions import Self

log = structlog.get_logger()


# GPT
MODEL_DEFAULT = "gpt-3.5-turbo"

# PRICING (per 1,000 tokens)
## See pricing list: https://openai.com/pricing (USD)
## See model list: https://platform.openai.com/docs/models/gpt-4-and-gpt-4-turbo
RATE_DEFAULT_IN = 0.005
MODEL_EQUIVALENCES = {
    # "gpt-3.5-turbo": "gpt-3.5-turbo-0125",
    "gpt-3.5-turbo": "gpt-3.5-turbo-0613" if date.today() >= date(2024, 2, 16) else "gpt-3.5-turbo-0125",
    "gpt-4-turbo-preview": "gpt-4-0125-preview",
}
MODEL_RATES_1000_TOKEN = {
    "gpt-3.5-turbo-0613": {
        "in": 0.0015,
        "out": 0.0020,
    },
    "gpt-3.5-turbo-0125": {
        "in": 0.0005,
        "out": 0.0015,
    },
    "gpt-4-0125-preview": {
        "in": 0.01,
        "out": 0.03,
    },
    "gpt-4": {
        "in": 0.03,
        "out": 0.06,
    },
    "gpt-4-32k": {
        "in": 0.06,
        "out": 0.12,
    },
}
MODEL_RATES_1000_TOKEN = {
    **MODEL_RATES_1000_TOKEN,
    **{new: MODEL_RATES_1000_TOKEN[equivalent] for new, equivalent in MODEL_EQUIVALENCES.items()},
}


# Interface with GPT
@dataclass
class GPTResponse(ChatCompletion):
    """GPT response."""

    _message_content_dix: Dict[str, Any] | None = None

    def __init__(self: Self, chat_completion_instance: ChatCompletion | None = None, **kwargs) -> None:
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
            if self._message_content_dix is None:
                self._message_content_dix = yaml.safe_load(self.message_content)
            else:
                raise ValueError("`message_content` is empty!")
        return self._message_content_dix

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

    def __init__(self: Self, **kwargs) -> None:
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

        # Get chat completion
        chat_completion = self.chat.completions.create(**kwargs)  # type: ignore

        # Build response
        if isinstance(chat_completion, ChatCompletion):
            response = GPTResponse(chat_completion)
            return response
        else:
            raise ValueError("message_content is expected to be a string!")


def get_number_tokens(text: str, model_name: str) -> int:
    """Get number of tokens of text.

    Note that tokens here is not equivalent to words. The number of tokens changes depending
    on the model used.

    More info: https://openai.com/pricing#language-models
    """
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
