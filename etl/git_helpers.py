#
#  git.py
#  etl
#

"""
Helpers for working with Git in an ETL flow.
"""

import time
from dataclasses import dataclass
from functools import wraps
from pathlib import Path
from typing import Any, Dict, Optional, Union, cast

import requests
import sh
from git import Repo
from structlog import get_logger

from etl.config import TLS_VERIFY
from etl.paths import BASE_DIR

CACHE_DIR = Path("~/.owid/git")
# Initialize logger.
log = get_logger()


class RepoAlreadyExists(Exception):
    pass


@dataclass
class GithubRepo:
    org: str
    repo: str

    @property
    def github_url(self) -> str:
        return f"https://github.com/{self.org}/{self.repo}"

    @property
    def cache_dir(self) -> Path:
        return Path(f"~/.owid/git/{self.org}/{self.repo}").expanduser()

    def ensure_cloned(self, shallow: bool = True) -> None:
        """
        Ensuret that a copy of this repo has been cloned and is up to date.
        """
        dest_dir = self.cache_dir
        if not dest_dir.is_dir():
            dest_dir.parent.mkdir(parents=True, exist_ok=True)
            if shallow:
                sh.git("clone", "--depth=1", self.github_url, dest_dir.as_posix(), _fg=True)  # type: ignore[reportCallIssue]
            else:
                sh.git("clone", self.github_url, dest_dir.as_posix(), _fg=True)  # type: ignore[reportCallIssue]
        else:
            self.update_and_reset()

    def update_and_reset(self) -> None:
        """
        Fetch new changes from origin and do a hard reset of the repo from
        origin/<current-branch>. The hard reset is to avoid the update breaking if
        there have been force pushes from master.
        """
        if not self.cache_dir.is_dir():
            raise Exception("cannot update repo until repo is cloned")

        self._git("fetch")
        self._git("reset", "--hard", f"origin/{self.branch_name}")

    @property
    def branch_name(self) -> str:
        "Return the current branch name of the checked out repo."
        # in newer versions of git we can do "git branch --show-current"
        # but these aren't available on Ubuntu 18.04 on live
        return self._git("symbolic-ref", "--short", "HEAD")

    @property
    def latest_sha(self) -> str:
        master_file = self.cache_dir / ".git/refs/heads/master"
        with open(master_file, "r") as f:
            sha = f.read().strip()

        return sha

    def _git(self, *args: str, **kwargs: Any) -> str:
        "Execute a git command in the context of this repo."
        return cast(
            str,
            sh.git("--no-pager", *args, _cwd=self.cache_dir.as_posix(), **kwargs).stdout.decode("utf8").strip(),  # type: ignore[reportCallIssue]
        )

    def is_up_to_date(self) -> bool:
        "Returns true if remote has no new changes, false otherwise."
        if not self.cache_dir.is_dir():
            return False

        return self.latest_remote_sha() == self.latest_sha

    def latest_remote_sha(self) -> str:
        "Return the latest commit SHA of the remote branch."
        # we rely on the smart HTTPS protocol for Git served by Github
        # https://www.git-scm.com/docs/http-protocol
        #
        # Responses look like this:
        #
        # S: 200 OK
        # S: Content-Type: application/x-git-upload-pack-advertisement
        # S: Cache-Control: no-cache
        # S:
        # S: 001e# service=git-upload-pack\n
        # S: 0000
        # S: 004895dcfa3633004da0049d3d0fa03f80589cbcaf31 refs/heads/maint\0multi_ack\n
        # S: 003fd049f6c27a2244e12041955e262a404c7faba355 refs/heads/master\n
        # S: 003c2cb58b79488a98d2721cea644875a8dd0026b115 refs/tags/v1.0\n
        # S: 003fa3c2e2402b99163d1d59756e5f207ae21cccba4c refs/tags/v1.0^{}\n
        # S: 0000

        uri = self.github_url + "/info/refs?service=git-upload-pack"
        resp = requests.get(uri, verify=TLS_VERIFY)
        resp.raise_for_status()
        lines = resp.content.decode("latin-1").splitlines()

        for line in lines:
            # XXX some repos now use "main" instead of "master"
            if line.count(" ") == 1 and line.endswith("refs/heads/master"):
                # the first four bytes are the line length, ignore them
                sha = line.split(" ")[0][4:]
                return cast(str, sha)

        raise Exception("Could not find latest remote SHA in response")


class GitError(Exception):
    pass


def log_time(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        log.info(f"{func.__name__}.start")
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        log.info(f"{func.__name__}.end", t=end_time - start_time)
        return result

    return wrapper


@log_time
def get_changed_files(
    current_branch: Optional[str] = None,
    base_branch: Optional[str] = None,
    repo_path: Union[Path, str] = BASE_DIR,
    only_committed: bool = False,
) -> Dict[str, Dict[str, str]]:
    """Return files that are different between the current branch and the specified base branch. This can
    be really slow if the number of files is large."""
    repo = Repo(repo_path)

    if current_branch is None:
        # If not specified, use the current branch.
        current_branch = repo.active_branch.name
    else:
        # Otherwise, switch to the given branch to compare from.
        repo.git.checkout(current_branch)

    if base_branch is None:
        # If not specified, use "master" branch.
        # However, if there is a "master-1" branch, that means we are on a staging server; if so, use "master-1".
        base_branch = "master-1" if "master-1" in [branch.name for branch in repo.branches] else "master"

    # Fetch the latest changes from the remote repository
    repo.remotes.origin.fetch()

    # Find the common ancestor of the remote base branch and the current local branch.
    # In other words, "merge_base" is the last common commit between those two branches.
    merge_base = repo.git.merge_base(f"origin/{base_branch}", f"{current_branch}").strip()

    # Create a dictionary {file_path: {"status": status, "diff": diff_content}}, where
    # * status is the change status, namely: 'M' if the file was modified, 'A' if appended, 'D' if deleted.
    # * diff_content shows the difference between files.
    changes = {}

    # Get the diff between the current branch and the base branch.
    diff_index = repo.git.diff(f"{merge_base}..{current_branch}", name_status=True, no_renames=True)
    if diff_index:
        for line in diff_index.splitlines():
            parts = line.split("\t")
            if len(parts) == 2:
                status, file_path = parts
                # Fetch diff content.
                diff_content = repo.git.diff(f"{merge_base}...{current_branch}", "--", file_path, p=True)
                changes[file_path] = {"status": status, "diff": diff_content}
            else:
                # Not sure if this could happen.
                log.error(f"Could not parse diff line: {line}")

    if not only_committed:
        # Include uncommitted changes
        uncommitted_diff = repo.git.diff(name_status=True, no_renames=True)
        if uncommitted_diff:
            for line in uncommitted_diff.splitlines():
                parts = line.split("\t")
                if len(parts) == 2:
                    status, file_path = parts
                    diff_content = repo.git.diff("--", file_path, p=True)
                    changes[file_path] = {"status": status, "diff": diff_content}

        # Add untracked files.
        changes.update({file_path: {"status": "A", "diff": ""} for file_path in repo.untracked_files})

    return changes
