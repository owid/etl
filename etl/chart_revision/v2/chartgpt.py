import json
from typing import Any, Dict, List

import openai
from sqlmodel import Session
from structlog import get_logger

import etl.grapher_model as gm
from etl.db import get_engine

# ChatGPT model name
# details: https://platform.openai.com/docs/models
MODEL_NAME = "gpt-3.5-turbo"

# Prompt for chatGPT
SYSTEM_PROMPT = """
You are a researcher and science communicator that publishes charts on various topics. You are tasked to improve the title and subtitle of a chart.

To improve these fields, focus on the following:

- Make sure the title and subtitle are short and concise.
- Don't make the title or subtitle longer.
- Ideally, the title length should be less than 80 characters
- Ideally, the subtitle length should be less than 250 characters.
- Do not use title case for the title.
- Correct spelling and grammar mistakes.
- The subtitle should end with a period.

The title is given after the keyword "TITLE", and the subtitle after the keyword "SUBTITLE".

Return the title and subtitle in JSON format. For example, if the title is "This is the title" and the subtitle is "This is the subtitle", return {"title": "This is the title", "subtitle": "This is the subtitle"}.
"""

# Logger
log = get_logger()


def ask_gpt(question: str, system_prompt: str = "", model: str = "gpt-4", num_responses: int = 1) -> List[str]:
    """Ask chatGPT a question, with a system prompt.

    More on its API can be found at https://platform.openai.com/docs/api-reference.
    """
    response = openai.ChatCompletion.create(
        model=model,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": question},
        ],
        temperature=0.8,
        presence_penalty=1,
        frequency_penalty=1,
        n=num_responses,
    )
    return [c["message"]["content"] for c in response["choices"]]  # type: ignore


def suggest_new_config_fields(config: Dict[str, Any], num_suggestions: int) -> List[Dict[str, Any]]:
    """Obtain `num_suggestions` new configurations for a given chart configuration.

    Note that the new configurations only contain the affected fields (e.g. title and subtitle).
    """
    # Init configuration list
    configs = []

    # Create question
    title = config["title"]
    subtitle = config["subtitle"]
    question = f"""TITLE: '{title}'\nSUBTITLE: '{subtitle}'"""
    # Get response from chatGPT
    responses = ask_gpt(question, system_prompt=SYSTEM_PROMPT, model=MODEL_NAME, num_responses=num_suggestions)
    # Check if response is valid (should be a dictionary with two fields: title and subtitle)
    for i, response in enumerate(responses):
        try:
            response = json.loads(response)
        except ValueError:
            log.error(
                f"Could not parse new configuration #{i}! Returned response is not a JSON with fields `title` and `subtitle`: {response}."
            )
        else:
            if "title" not in response or "subtitle" not in response:
                log.error(
                    f"Could not parse new configuration #{i}! Returned response is not a JSON with fields `title` and `subtitle`: {response}."
                )
            else:
                configs.append(
                    {
                        "title": response["title"],
                        "subtitle": response["subtitle"],
                    }
                )
    return configs


if __name__ == "__main__":
    with Session(get_engine()) as session:
        # Get pending revisions
        log.info("Getting pending revisions...")
        revisions = gm.SuggestedChartRevisions.load_pending(session)

        # Create new configurations
        log.info("Getting new configurations from chatGPT...")
        num_suggestions = 3
        new_configs_all = []
        for i, revision in enumerate(revisions):
            log.info(f"Getting new configurations from chatGPT for revision #{i}...")
            new_configs = suggest_new_config_fields(revision.suggestedConfig, num_suggestions)

            # Add suggestions to object
            log.info(f"Pushing new configurations for revision #{i} to DB...")
            suggestion = {
                "model": MODEL_NAME,
                "suggestions": new_configs,
            }
            if not revision.experimental:
                revision.experimental = {
                    "gpt": suggestion,
                }
            else:
                revision.experimental["gpt"] = suggestion
            # Push to DB
            session.commit()
