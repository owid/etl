import requests

from apps.wizard.utils.db import WizardDB


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


def add_pr_data_to_db():
    # Get data
    data = get_latest_pr_data()
    # Add to database
    WizardDB().add_pr(data)
