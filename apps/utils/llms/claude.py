"""Small helpers for one-off Claude calls (wizard summaries, `etl pr` branch names).

The key is ANTHROPIC_API_KEY. Responses can begin with thinking blocks, so the answer is read
from the text blocks, and `max_tokens` leaves room for the thinking that counts toward it.
"""

from collections.abc import Iterator

import anthropic

from apps.utils.llms.costs import estimate_llm_cost
from etl import config


def has_api_key() -> bool:
    return bool(config.ANTHROPIC_API_KEY)


class TextStream:
    """Stream a Claude answer as text chunks (e.g. into `st.write_stream`), then read its cost.

    Iterate it once; `usage` and `cost` are set when the stream ends.
    """

    def __init__(self, model: str, system: str, user: str, max_tokens: int = 16000, effort: str | None = None):
        self.model = model
        self._request = {
            "model": model,
            "max_tokens": max_tokens,
            "system": system,
            "messages": [{"role": "user", "content": user}],
            **({"output_config": {"effort": effort}} if effort else {}),
        }
        self.usage: anthropic.types.Usage | None = None

    def __iter__(self) -> Iterator[str]:
        client = anthropic.Anthropic(api_key=config.ANTHROPIC_API_KEY)
        with client.messages.stream(**self._request) as stream:
            yield from stream.text_stream
            self.usage = stream.get_final_message().usage

    @property
    def cost(self) -> float | None:
        """Cost in USD, or None before the stream ends or for a model genai-prices doesn't know."""
        if self.usage is None:
            return None
        try:
            return estimate_llm_cost(
                f"anthropic:{self.model}",
                input_tokens=self.usage.input_tokens,
                output_tokens=self.usage.output_tokens,
            )
        except LookupError:
            return None


def ask(model: str, system: str, user: str, max_tokens: int = 4000, effort: str | None = None) -> str:
    """A short, non-streamed answer."""
    client = anthropic.Anthropic(api_key=config.ANTHROPIC_API_KEY)
    message = client.messages.create(
        model=model,
        max_tokens=max_tokens,
        system=system,
        messages=[{"role": "user", "content": user}],
        **({"output_config": {"effort": effort}} if effort else {}),
    )
    return "".join(block.text for block in message.content if block.type == "text").strip()
