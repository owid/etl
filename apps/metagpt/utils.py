"""Auxiliary classes, functions and variables."""
from dataclasses import dataclass

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


# Additional instructions or configurations
ADDITIONAL_INSTRUCTIONS = """
Metadata Field Guidelines:

1. attribution (string, optional)
   - Capital letter start, except names like 'van Haasteren'.
   - No ending period.
   - Ends with year of date_published in parenthesis.
   - No semicolons.

2. attribution_short (string, recommended)
   - Use if producer name is long; shorten the producer name in an informative way.
   - Capital letter start, except names like 'van Haasteren'.
   - No ending period.
   - Refers to producer or well-known data product, not year.
   - Use acronym if well-known.

3. citation_full (string, required)
   - Capital letter start.
   - Ends with period.
   - Includes year of publication.
   - Match producer's format with minor edits.
   - List multiple sources for compilations.

4. date_accessed (string, required)
   - Format: YYYY-MM-DD.
   - Reflects access date of current version.

5. date_published (string, required)
   - Format: YYYY-MM-DD or YYYY.
   - Reflects publication date of current version.

6. description (string, recommended)
   - Capital letter start and period end.
   - Avoid other metadata fields unless crucial.
   - Succinct description of data product.

7. description_snapshot (string, recommended)
   - Capital letter start and period end.
   - Define if data product and snapshot differ.
   - Describe snapshot specifically.

8. license (string, required)
   - Standard license names or producer's custom text.
   - CC BY 4.0 if unspecified, pending confirmation.

9. license.url (string, required if existing)
   - Complete URL to license on producer's site.
   - Avoid generic license pages.

10. producer (string, required)
    - Capital letter start, except names like 'van Haasteren'.
    - No ending period, except 'et al.'.
    - Exclude dates, semicolons, OWID references.

11. title (string, required)
    - Capital letter start.
    - No ending period.
    - Identify data product, not snapshot.

12. title_snapshot (string, required if different)
    - Capital letter start.
    - No ending period.
    - Use if snapshot differs from data product.

13. url_download (string, required if existing)
    - Direct download URL or S3 URI.

14. url_main (string, required)
    - URL to data product's main site.

15. version_producer (string or number, recommended if existing)
    - Use producer's version naming.
"""
