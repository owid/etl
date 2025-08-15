from apps.wizard.app_pages.expert.prompts import (
    SYSTEM_PROMPT_DATASETTE,
    SYSTEM_PROMPT_GUIDES,
    SYSTEM_PROMPT_INTRO,
    SYSTEM_PROMPT_METADATA,
)
from apps.wizard.app_pages.expert.prompts_dynamic import (
    SYSTEM_PROMPT_DATABASE,
    SYSTEM_PROMPT_FULL,
)
from structlog import get_logger

# LOG
log = get_logger()


# CATEGORY FOR CHAT
# Chat category-switching
class ChatCategories:
    """Chat categories."""

    FULL = "**⭐️ All**"
    DATASETTE = "Datasette"
    DATABASE = "Analytics"
    METADATA = "ETL Metadata"
    INTRO = "Introduction"
    GUIDES = "Learn more"
    DEBUG = "Debug"


CHAT_CATEGORIES = [
    ChatCategories.FULL,
    ChatCategories.DATABASE,
    ChatCategories.METADATA,
    ChatCategories.INTRO,
    ChatCategories.GUIDES,
]


# Switch category function
def get_system_prompt(category: str) -> str:
    """Get appropriate system prompt."""
    # Choose context to provide to GPT
    match category:
        case ChatCategories.METADATA:
            log.info("Switching to 'Metadata' system prompt.")
            system_prompt = SYSTEM_PROMPT_METADATA
        case ChatCategories.INTRO:
            log.info("Switching to 'Getting started'/Design principles system prompt.")
            system_prompt = SYSTEM_PROMPT_INTRO
        case ChatCategories.GUIDES:
            log.info("Switching to 'Guides' system prompt.")
            system_prompt = SYSTEM_PROMPT_GUIDES
        case ChatCategories.FULL:
            log.warning("Switching to 'All' system prompt.")
            system_prompt = SYSTEM_PROMPT_FULL
        case ChatCategories.DATASETTE:
            log.warning("Switching to 'DATASETTE' system prompt.")
            system_prompt = SYSTEM_PROMPT_DATASETTE
        case ChatCategories.DATABASE:
            log.warning("Switching to 'DATABASE' system prompt.")
            system_prompt = SYSTEM_PROMPT_DATABASE
        case ChatCategories.DEBUG:
            log.warning("Switching to 'DEBUG' system prompt.")
            system_prompt = ""
        case _:
            log.info("Nothing selected. Switching to 'All' system prompt.")
            system_prompt = SYSTEM_PROMPT_FULL
    return system_prompt
