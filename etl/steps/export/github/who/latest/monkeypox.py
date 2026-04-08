"""Load a grapher dataset and create an explorer dataset with its tsv file."""

from etl import config
from etl.git_api_helpers import GithubApiRepo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    ds = paths.load_dataset("monkeypox")
    tb = ds.read("monkeypox")

    # Process it for backwards compatibility.
    tb = tb.rename(columns={"country": "location"}).drop(columns=["suspected_cases_cumulative", "annotation"])

    # Commit the data to the repository
    repo = GithubApiRepo(repo_name="monkeypox")
    repo.commit_file(
        tb.to_csv(index=False),
        file_path="owid-monkeypox-data.csv",
        commit_message="data(mpx): automated update",
        branch="main",
        dry_run=not config.MONKEYPOX_COMMIT,
    )
