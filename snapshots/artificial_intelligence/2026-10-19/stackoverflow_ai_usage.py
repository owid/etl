"""Script to create a snapshot of Stack Overflow Developer Survey responses on AI tool usage.

Downloads the public response data (one CSV per survey year) from Stack Overflow's survey archive on GitHub,
and keeps only the columns needed to compute the share of professional developers who use AI tools:
    - AISelect: the AI tool usage question.
    - MainBranch (2023-2025) or CodeMain (2026): the question used to identify professional developers.

The 2026 survey replaced MainBranch with CodeMain, and reworded AISelect (see the garden step).

The files are stored with Git LFS, so they are downloaded from media.githubusercontent.com rather than from the
raw GitHub URLs (which only return the LFS pointer).
"""

from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd
from structlog import get_logger

from etl.download_helpers import download
from etl.snapshot import Snapshot

log = get_logger()

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

# Base URL of the survey archive (Git LFS files).
BASE_URL = "https://media.githubusercontent.com/media/StackExchange/Survey/main/packages/archive"

# Public response data file for each survey year, and the columns to keep from it.
SURVEY_FILES = {
    2023: ("2023/results.csv", ["MainBranch", "AISelect"]),
    2024: ("2024/results.csv", ["MainBranch", "AISelect"]),
    2025: ("2025/results.csv", ["MainBranch", "AISelect"]),
    2026: ("2026/survey_results_public.csv", ["CodeMain", "AISelect"]),
}


def run(upload: bool = True) -> None:
    snap = Snapshot(f"artificial_intelligence/{SNAPSHOT_VERSION}/stackoverflow_ai_usage.parquet")

    # Download each year's response data and keep only the relevant columns.
    dfs = []
    with TemporaryDirectory() as temp_dir:
        for year, (path, columns) in SURVEY_FILES.items():
            local_file = Path(temp_dir) / f"{year}.csv"
            log.info("downloading", year=year, url=f"{BASE_URL}/{path}")
            download(url=f"{BASE_URL}/{path}", filename=str(local_file), quiet=True)
            df_year = pd.read_csv(local_file, usecols=columns, encoding="utf-8-sig", dtype="string")
            # Sanity check: a Git LFS pointer instead of the real file would have only a few lines.
            assert len(df_year) > 10_000, f"Unexpectedly few responses for {year}: {len(df_year)}"
            dfs.append(df_year.assign(year=year))

    # Combine all years into one table (columns missing in a given year are left empty).
    df = pd.concat(dfs, ignore_index=True)[["year", "MainBranch", "CodeMain", "AISelect"]]

    snap.create_snapshot(upload=upload, data=df)
