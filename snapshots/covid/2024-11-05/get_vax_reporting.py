"""
Get dates of first reporting for each country.

python snapshot/covid/2024-11-05/get_vax_reporting.py
"""

import os
import re
from io import StringIO
from typing import Optional

import github
import github.PullRequest
import github.Repository
import pandas as pd
from github import Auth, Github

from etl import config


def get_repo(
    repo_name: str, access_token: Optional[str] = None, per_page: Optional[int] = None
) -> github.Repository.Repository:
    """Get repository."""
    if not access_token:
        assert config.OWIDBOT_ACCESS_TOKEN, "OWIDBOT_ACCESS_TOKEN is not set"
        access_token = config.OWIDBOT_ACCESS_TOKEN
    auth = Auth.Token(access_token)
    if per_page:
        g = Github(auth=auth, per_page=per_page)
    else:
        g = Github(auth=auth)
    return g.get_repo(f"owid/{repo_name}")


def get_country_file_paths(repo, folder_path):
    files = []
    contents = repo.get_contents(folder_path)
    while contents:
        file_content = contents.pop(0)
        if file_content.type == "dir":
            contents.extend(repo.get_contents(file_content.path))
        else:
            files.append(file_content.path)
    return files


def get_country_file_paths_old(repo, folder_path):
    country_files = set()
    commits = repo.get_commits(path=folder_path)

    def is_country_file(path):
        base_path = "scripts/scripts/vaccinations/output"
        return re.match(rf"^{base_path}/[^/]+\.csv$", path)

    for commit in commits:
        # Get the details of the commit
        commit_details = repo.get_commit(commit.sha)
        for file in commit_details.files:
            if file.status == "removed" and file.filename.startswith(folder_path):
                country_files.add(file.filename)
        break

    country_files = {c for c in country_files if is_country_file(c)}
    return country_files


def get_initial_version_of_file(repo, file_path):
    num_retries = 10
    commits = []
    for i in range(num_retries):
        commits = repo.get_commits(path=file_path)
        if commits.totalCount == 0:
            print(">> No commits found, retrying...")
            continue
            # return None
        else:
            break
    # The last commit in the list is the initial commit
    initial_commit = list(commits)[-1]
    # Retrieve the file content at the initial commit
    try:
        csv_content = repo.get_contents(file_path, ref=initial_commit.sha)
        csv_content = csv_content.decoded_content.decode("utf-8")
        df = pd.read_csv(StringIO(csv_content))
        try:
            date_reported = initial_commit.commit.author.date
            date_reported = date_reported.strftime("%Y-%m-%d")
        except Exception:
            date_reported = None

        return {
            "commit": initial_commit.sha,
            "date_first_value": df["date"].min(),
            "date_first_reported": date_reported,
        }
    except Exception as e:
        print(f"Error retrieving {file_path} at commit {initial_commit.sha}: {e}")
        return None


def combine_files_now_and_old(files, files_old):
    """Combine list of countries (keep old if available, otherwise new)."""
    file_dix = {}
    for file in files_old:
        key = os.path.basename(file)
        file_dix[key] = file
    for file in files:
        key = os.path.basename(file)
        if key not in file_dix:
            file_dix[key] = file
    files = list(file_dix.values())
    return files


# Get repository
repo = get_repo("covid-19-data", access_token=config.GITHUB_TOKEN)

# Get country file paths
# path_vax = "scripts/output/vaccinations/main_data"
# path_vax_old = "scripts/scripts/vaccinations/output"
path_vax = "public/data/vaccinations/country_data"

files = get_country_file_paths(repo, path_vax)
# files_old = get_country_file_paths_old(repo, path_vax_old)

# Get files
# files = combine_files_now_and_old(files, files_old)

######################################################
# GET FIRST FILE VERSIONS
######################################################
data = []
for i, file in enumerate(files):
    print(f"> {file}")
    data_ = get_initial_version_of_file(repo, file)
    if data_ is not None:
        data_["country"] = file
    data.append(data_)

    if i % 10 == 0:
        print(f">> {i} files processed")
        df = pd.DataFrame(data)
        df.to_csv(f"first_reporting_dates-{i}.csv", index=False)


# Create DataFrame
df = pd.DataFrame(data)
df.to_csv("first_reporting_dates.csv", index=False)
