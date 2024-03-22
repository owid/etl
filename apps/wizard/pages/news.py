"""Display news from ETL."""
import json
from datetime import datetime

import pytz
import requests
import streamlit as st
from st_pages import add_indentation

from etl.paths import BASE_DIR

st.set_page_config(page_title="Wizard: News", page_icon="🪄")
add_indentation()
# st.title("🏐 Metadata playground")
st.title("News 🗞️")


def _clean_date(dt_raw: str) -> str:
    return datetime.strptime(dt_raw, "%Y-%m-%dT%XZ").strftime("%c")


@st.cache_data()
def check_and_load_news():
    DATE = datetime.now(pytz.timezone("Europe/London")).strftime("%Y%m%d")
    path = BASE_DIR / "wizard-news" / f"{DATE}.json"

    if not path.parent.exists():
        path.parent.mkdir(parents=True, exist_ok=True)

    if path.exists():
        print("news exist")
    else:
        # Download GitHub PR data
        print("Getting GH info...")
        url = "https://api.github.com/repos/owid/etl/pulls?state=closed'"
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json()
        else:
            st.error("Error fetching GitHub information! Refresh.")

        # Clean pr data
        pr_data = _clean_pr_data(data)

        # Save data
        with open(path, "w") as f:
            json.dump(pr_data, f)

        # Process information


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
            "date_created": _clean_date(pr["created_at"]),
            "date_merged": _clean_date(pr["merged_at"]) if pr["merged_at"] is not None else None,
            "labels": str([label["name"] for label in pr["labels"]]),
            "description": pr["body"],
            "merged": pr["merged_at"] is not None,
            # "url_comments": pr["comments_url"],
            # "url_review_comments": pr["review_comments_url"],
            "url_merge_commit": f"https://github.com/owid/etl/commit/{pr['merge_commit_sha']}",
            "url_diff": pr["diff_url"],
            "url_patch": pr["patch_url"],
            "url_html": pr["html_url"],
        }
        pr_data.append(pr_data_)
    return pr_data
