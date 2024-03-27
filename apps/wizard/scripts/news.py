"""Methods to update Wizard DB to have the necessary PR data."""
from typing import Tuple

import requests
from structlog import get_logger

from apps.wizard.pages.expert.prompts import SYSTEM_PROMPT_GENERIC
from apps.wizard.utils.db import WizardDB
from apps.wizard.utils.gpt import GPTQuery, OpenAIWrapper, get_cost_and_tokens

# Logger
log = get_logger()

# GPT model
MODEL_NAME = "gpt-4-turbo-preview"


def get_json_url(url: str):
    """Get JSON data from URL."""
    response = requests.get(url, timeout=10)
    if response.status_code == 200:
        data = response.json()
    else:
        raise requests.ConnectionError("Error fetching GitHub information! Refresh.")
    return data


def get_latest_pr_data():
    """Get latest PR data."""
    # Access repo and get latest PR data
    url = "https://api.github.com/repos/owid/etl/pulls?state=closed'"
    data = get_json_url(url)
    # Clean
    data = _clean_pr_data(data)
    # Get additional comment data
    return data


def _clean_pr_data(data):
    """Clean PR data to get necessary fields."""
    pr_data = []
    for pr in data:
        pr_data_ = {
            "id": pr["id"],
            "number": pr["number"],
            "title": pr["title"],
            "username": pr["user"]["login"],
            "date_created": pr["created_at"],
            "date_merged": pr["merged_at"],
            "labels": str([label["name"] for label in pr["labels"]]),
            "description": pr["body"],
            "merged": pr["merged_at"] is not None,
            "url_comments": pr["comments_url"],
            "url_review_comments": pr["review_comments_url"],
            "url_merge_commit": f"https://github.com/owid/etl/commit/{pr['merge_commit_sha']}",
            "url_diff": pr["diff_url"],
            "url_patch": pr["patch_url"],
            "url_html": pr["html_url"],
        }
        pr_data.append(pr_data_)
    return pr_data


def add_pr_data_to_db() -> None:
    """Get latest PR data and add to Wizard DB."""
    # Get data
    data = get_latest_pr_data()
    # Add to database
    WizardDB().add_pr(data)


def ask_gpt(df) -> Tuple[str, float, int]:
    """Ask GPT for news."""
    SYSTEM_PROMPT = f"""You will be given a markdown table with the pull requests merged in the etl repository in the last 7 days.

    Summarise the main updates and interesting points from the pull requests. Use markdown syntax in your reply if needed.

    To refer to users, use their `username`.

    {SYSTEM_PROMPT_GENERIC}
    """
    USER_PROMPT = f"{df.to_markdown()}"
    # Ask Chat GPT
    api = OpenAIWrapper()

    query = GPTQuery(
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": USER_PROMPT},
        ]
    )
    response = api.query_gpt(query=query, model=MODEL_NAME)

    summary = response.message_content  # type: ignore
    cost, num_tokens = get_cost_and_tokens(SYSTEM_PROMPT + USER_PROMPT, summary, MODEL_NAME)
    return summary, cost, num_tokens  # type: ignore


def add_news_data_to_db() -> None:
    """Get summaries of latest PRs and add to DB."""
    # 7days
    log.info("Getting summary of PRs (7 day)")
    df = WizardDB.get_pr(num_days=7)
    summary, cost, num_tokens = ask_gpt(df)
    WizardDB.add_news_summary(summary=summary, cost=cost, window_type="7d")
    # 24 hours
    log.info("Getting summary of PRs (24 hours)")
    df = WizardDB.get_pr(num_days=1)
    summary, cost, num_tokens = ask_gpt(df)
    WizardDB.add_news_summary(summary=summary, cost=cost, window_type="1d")


def main() -> None:
    """Add PR and news data to DB."""
    # Get list of latest PRs and add to DB
    log.info("Getting PRs into the database")
    add_pr_data_to_db()
    # Generate summaries of PRs and add to DB (must come after adding latest PRs to DB)
    add_news_data_to_db()


if __name__ == "__main__":
    main()
    print("Done!")
