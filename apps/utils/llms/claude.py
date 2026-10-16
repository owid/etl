"""Small helpers for one-off Claude calls (wizard summaries, `etl pr` branch names, scripts).

The key is ANTHROPIC_API_KEY. Responses can begin with thinking blocks, so the answer is read
from the text blocks, and `max_tokens` leaves room for the thinking that counts toward it.
"""

from collections.abc import Iterator
from typing import Any

import anthropic

from etl import config


def has_api_key() -> bool:
    return bool(config.ANTHROPIC_API_KEY)


def text_of(message: anthropic.types.Message) -> str:
    """The answer in a response, skipping any thinking blocks before it."""
    return "".join(block.text for block in message.content if block.type == "text").strip()


def _request(model: str, system: str, user: str, max_tokens: int, effort: str | None) -> dict[str, Any]:
    return {
        "model": model,
        "max_tokens": max_tokens,
        "system": system,
        "messages": [{"role": "user", "content": user}],
        **({"output_config": {"effort": effort}} if effort else {}),
    }


class TextStream:
    """Stream a Claude answer as text chunks (e.g. into `st.write_stream`), then read its cost.

    Iterate it once; `usage`, `cost` and `summary` are filled in when the stream ends.
    """

    def __init__(self, model: str, system: str, user: str, max_tokens: int = 16000, effort: str | None = None):
        self._request = _request(model, system, user, max_tokens, effort)
        self.usage: anthropic.types.Usage | None = None

    def __iter__(self) -> Iterator[str]:
        client = anthropic.Anthropic(api_key=config.ANTHROPIC_API_KEY)
        with client.messages.stream(**self._request) as stream:
            yield from stream.text_stream
            self.usage = stream.get_final_message().usage

    @property
    def cost(self) -> float | None:
        """Cost in USD, or None before the stream ends or for a model genai-prices doesn't know."""
        # Imported here: the pricing libraries are slow to load, and only this needs them.
        from apps.utils.llms.costs import estimate_llm_cost

        if self.usage is None:
            return None
        try:
            return estimate_llm_cost(
                f"anthropic:{self._request['model']}",
                input_tokens=self.usage.input_tokens,
                output_tokens=self.usage.output_tokens,
            )
        except LookupError:
            return None

    @property
    def summary(self) -> str:
        """One line with the cost and token counts, for showing under the answer."""
        cost_usd = self.cost
        cost = f"{cost_usd:.4f} USD" if cost_usd is not None else "unknown"
        tokens = f"{self.usage.input_tokens} in, {self.usage.output_tokens} out" if self.usage else "unknown"
        return f"Cost: {cost}. Tokens: {tokens}."


def ask(model: str, system: str, user: str, max_tokens: int = 4000, effort: str | None = None) -> str:
    """A short, non-streamed answer."""
    client = anthropic.Anthropic(api_key=config.ANTHROPIC_API_KEY)
    return text_of(client.messages.create(**_request(model, system, user, max_tokens, effort)))
