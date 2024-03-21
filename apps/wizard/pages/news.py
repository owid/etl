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


def _clean_pr_data(data):
    pr_data = []
    for pr in data:
        pr_data_ = {
            "title": pr["title"],
            "id": pr["number"],
            "date_created": _clean_date(pr["created_at"]),
            "date_merged": _clean_date(pr["merged_at"]) if pr["merged_at"] is not None else None,
            "username": pr["user"]["login"],
            "description": pr["body"],
            "draft": pr["draft"],
            "comments_url": pr["comments_url"],
            "review_comments_url": pr["review_comments_url"],
            "merge_commit_url": f"https://github.com/owid/etl/commit/{pr['merge_commit_sha']}",
            "merged": pr["merged_at"] is not None,
        }
        pr_data.append(pr_data_)
        return pr_data


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
