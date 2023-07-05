"""Accesses the table `suggested_chart_revisions` from the database and produces alternative titles and subtitles using chatGPT.

The modified titles and subtitles are generated using the following a specific system prompt (see `etl.chart_revision.v2.SYSTEM_PROMPT`).
"""
import json
from typing import Any, Dict, List

import openai
import rich_click as click
from sqlmodel import Session, select
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
SYSTEM_PROMPT_TEXT = """
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

# Click options
click.rich_click.OPTION_GROUPS = {
    "etl-chartgpt": [
        {
            "name": "Basic usage",
            "options": ["-me"],
        },
        {
            "name": "Advanced options",
            "options": ["-f", "--user-id", "--revision-id"],
            # You can also set table styles at group-level instead of using globals if you want
            # "table_styles": {
            #     "row_styles": ["bold", "yellow", "cyan"],
            # },
        },
        {
            "name": "GPT options",
            "options": ["--model-name", "--sample-size", "--system-prompt"],
        },
    ],
}
# click.rich_click.COMMAND_GROUPS = {
#     "03_groups_sorting.py": [
#         {
#             "name": "Main usage",
#             "commands": ["sync", "download"],
#         },
#         {
#             "name": "Configuration",
#             "commands": ["config", "auth"],
#         },
#     ],
# }


@click.command(context_settings={"help_option_names": ["-h", "--help"]})
@click.option(
    "-u",
    "--user-id",
    type=int,
    help="ID of the user. Use this to only get the revisions created by this particular user. By default will get revisions from all users.",
)
@click.option(
    "-i",
    "--revision-id",
    type=int,
    help="ID of the revision. Use this to only get a specific review. By default will get revisions from all users.",
)
@click.option(
    "-me",
    "--only-mine",
    is_flag=True,
    default=False,
    help="Use this to only modify those revisions created using your user id. Make sure that your environment variable GPT_SAMPLE_SIZE is properly set. If set, userid value will be ignored.",
)
@click.option(
    "-f",
    "--overwrite",
    is_flag=True,
    default=False,
    help="Use this to overwrite existing suggestions. That is, existing gpt suggestions for the retrieved revisions will be replaced.",
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
@click.option(
    "-t",
    "--system-prompt",
    type=str,
    help="Path to a custom chatGPT system prompt.",
)
@click.version_option("0.1.0", prog_name="etl-chartgpt")
def cli(
    user_id: int,
    revision_id: int,
    only_mine: bool,
    overwrite: bool,
    sample_size: int,
    model_name: str,
    system_prompt: str,
) -> None:
    """Add suggestions by chatGPT to pending revisions."""
    if system_prompt:
        with open(system_prompt, "r") as f:
            system_prompt = f.read()
    else:
        system_prompt = SYSTEM_PROMPT_TEXT
    with Session(get_engine()) as session:
        # Get pending revisions
        log.info(f"Using {model_name}, sampling {sample_size} suggestions...")
        if only_mine:
            if GRAPHER_USER_ID is None:
                raise ValueError(
                    "Environment variable `GRAPHER_USER_ID` is not set. Please set it to your user ID, or use the `--user-id` option."
                )
            user_id = int(GRAPHER_USER_ID)
        if user_id is not None:
            log.info(f"Only getting revisions from user with ID {user_id}...")

        # Get revision for a specific ID
        if revision_id:
            revisions = session.exec(
                select(gm.SuggestedChartRevisions)
                .where(gm.SuggestedChartRevisions.status == "pending")
                .where(gm.SuggestedChartRevisions.id == revision_id)
            ).all()

        # Get revisions for a specific user
        elif user_id:
            revisions = session.exec(
                select(gm.SuggestedChartRevisions)
                .where(gm.SuggestedChartRevisions.status == "pending")
                .where(gm.SuggestedChartRevisions.createdBy == user_id)
            ).all()
        # Get all revisions
        else:
            revisions = session.exec(
                select(gm.SuggestedChartRevisions).where(gm.SuggestedChartRevisions.status == "pending")
            ).all()

        if not revisions:
            raise ValueError("No revisions found with the given parameters!")

        # Only consider those that do not have a suggestion
        if not overwrite:
            revisions = [r for r in revisions if r.experimental is None]

        # Create new config`urations
        log.info(f"Found {len(revisions)} revisions pending!")
        if not revisions:
            log.info("No revisions pending! Exiting...")
        else:
            num_suggestions = sample_size
            for revision in revisions:
                log.info(f"Getting new configurations from chatGPT for revision #{revision.id}...")
                new_configs = suggest_new_config_fields(
                    revision.suggestedConfig, system_prompt, num_suggestions, model_name
                )

                # Add suggestions to object
                log.info(f"Pushing new configurations for revision #{revision.id} to DB...")

                revision.experimental = {
                    "gpt": {
                        "model": model_name,
                        "suggestions": new_configs,
                    },
                }
                # Push to DB
                session.commit()


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


def suggest_new_config_fields(
    config: Dict[str, Any], system_prompt: str, num_suggestions: int, model_name: str
) -> List[Dict[str, Any]]:
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
    responses = ask_gpt(question, system_prompt=system_prompt, model=model_name, num_responses=num_suggestions)
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
