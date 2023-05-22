"""Accesses the table `suggested_chart_revisions` from the database and produces alternative titles and subtitles using chatGPT.

The modified titles and subtitles are generated using the following a specific system prompt (see `etl.chart_revision.v2.SYSTEM_PROMPT`).
"""
import json
from typing import Any, Dict, List

import click
import openai
from rich_click.rich_command import RichCommand
from sqlmodel import Session
from structlog import get_logger

import etl.grapher_model as gm
from etl.config import GRAPHER_USER_ID
from etl.db import get_engine

# ChatGPT model name
# details: https://platform.openai.com/docs/models
MODELS_AVAILABLE = ["gpt-3.5-turbo", "gpt-4"]
MODEL_DEFAULT = "gpt-3.5-turbo"
GPT_SAMPLE_SIZE = 3

# Prompt for chatGPT
SYSTEM_PROMPT = """
You are a researcher and science communicator that publishes charts on various topics. You are tasked to improve the title and subtitle of a chart.

To improve these fields, focus on the following:

- Make sure the title and subtitle are short and concise.
- Don't make the title or subtitle longer.
- Ideally, the title length should be less than 80 characters
- Ideally, the subtitle length should be less than 250 characters.
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


def suggest_new_config_fields(config: Dict[str, Any], num_suggestions: int, model_name: str) -> List[Dict[str, Any]]:
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
    responses = ask_gpt(question, system_prompt=SYSTEM_PROMPT, model=model_name, num_responses=num_suggestions)
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
                        "title": response["title"].capitalize(),
                        "subtitle": response["subtitle"],
                    }
                )
    return configs


@click.command(cls=RichCommand, help=__doc__)
@click.option(
    "-u",
    "--userid",
    type=int,
    help="ID of the user. By default will get revisions from all users.",
)
@click.option(
    "-me",
    "--only-mine",
    is_flag=True,
    default=False,
    help="Use this to only modify those revisions created using your user id. Make sure that your environment variable `GPT_SAMPLE_SIZE` is properly set. If set, `userid` value will be ignored.",
)
@click.option(
    "-s",
    "--sample-size",
    type=int,
    default=GPT_SAMPLE_SIZE,
    help="Number of reviews sampled from chatGPT.",
)
@click.option(
    "-n",
    "--model-name",
    type=click.Choice(MODELS_AVAILABLE),
    default=MODEL_DEFAULT,
    help="Choose chart_revision backend version to use. By default uses latest version.",
)
def cli(userid: int, only_mine: bool, sample_size: int, model_name: str) -> None:
    """Add suggestions by chatGPT to pending revisions."""
    with Session(get_engine()) as session:
        # Get pending revisions
        log.info(f"Using {model_name}, sampling {sample_size} suggestions...")
        if only_mine:
            if GRAPHER_USER_ID is None:
                raise ValueError(
                    "Environment variable `GRAPHER_USER_ID` is not set. Please set it to your user ID, or use the `--userid` option."
                )
            userid = int(GRAPHER_USER_ID)
        if userid is not None:
            log.info(f"Only getting revisions from user with ID {userid}...")
        revisions = gm.SuggestedChartRevisions.load_pending(session, userid)

        # Create new configurations
        log.info(f"Found {len(revisions)} revisions pending!")
        if not revisions:
            log.info("No revisions pending! Exiting...")
        else:
            num_suggestions = sample_size
            for i, revision in enumerate(revisions):
                log.info(f"Getting new configurations from chatGPT for revision #{i}...")
                new_configs = suggest_new_config_fields(revision.suggestedConfig, num_suggestions, model_name)

                # Add suggestions to object
                log.info(f"Pushing new configurations for revision #{i} to DB...")
                suggestion = {
                    "model": model_name,
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
