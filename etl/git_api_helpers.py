"""
Helpers for working with Git through PyGithub library.
"""

import hashlib
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import jwt
import requests
from github import Auth, Github, GithubException, InputGitTreeElement
from github.PullRequest import PullRequest
from github.Repository import Repository
from structlog import get_logger

from etl.config import (
    GITHUB_TOKEN,
    OWIDBOT_ACCESS_TOKEN,
    OWIDBOT_APP_CLIENT_ID,
    OWIDBOT_APP_INSTALLATION_ID,
    OWIDBOT_APP_PRIVATE_KEY_PATH,
)
from etl.paths import BASE_DIR

# Initialize logger.
log = get_logger()


def get_github_instance(access_token: Optional[str] = None) -> Github:
    """Return a PyGithub instance authenticated with the token.

    Args:
        access_token: Optional access token. If None, uses GITHUB_TOKEN.
    """
    token = access_token or GITHUB_TOKEN
    if token:
        auth = Auth.Token(token)
        return Github(auth=auth)
    return Github()  # Anonymous access


def generate_jwt(client_id: str, private_key_path: str) -> str:
    """Generate a JWT token for GitHub App authentication.

    Args:
        client_id: GitHub App client ID
        private_key_path: Path to the private key file

    Returns:
        JWT token
    """
    now = int(time.time())
    payload = {
        "iat": now,
        "exp": now + (10 * 60),  # JWT expiration time (10 minutes)
        "iss": client_id,
    }
    with open(private_key_path, "r") as key_file:
        private_key = key_file.read()
    token = jwt.encode(payload, private_key, algorithm="RS256")
    return token


def github_app_access_token(max_retries=3) -> str:
    """Get a GitHub App installation access token.

    Args:
        max_retries: Maximum number of retries if the request fails

    Returns:
        Access token
    """
    assert OWIDBOT_APP_CLIENT_ID, "OWIDBOT_APP_CLIENT_ID is not set"
    assert OWIDBOT_APP_PRIVATE_KEY_PATH, "OWIDBOT_APP_PRIVATE_KEY_PATH is not set"
    assert OWIDBOT_APP_INSTALLATION_ID, "OWIDBOT_APP_INSTALLATION_ID is not set"

    jwt_token = generate_jwt(OWIDBOT_APP_CLIENT_ID, OWIDBOT_APP_PRIVATE_KEY_PATH)

    # Use the JWT to get an installation access token
    headers = {"Authorization": f"Bearer {jwt_token}", "Accept": "application/vnd.github.v3+json"}
    installation_access_token_url = (
        f"https://api.github.com/app/installations/{OWIDBOT_APP_INSTALLATION_ID}/access_tokens"
    )

    backoff = 2
    for attempt in range(1, max_retries + 1):
        response = requests.post(installation_access_token_url, headers=headers)
        if response.status_code not in (500, 504):
            response.raise_for_status()
            access_token = response.json()["token"]
            return access_token
        else:
            if attempt == max_retries:
                response.raise_for_status()
            else:
                time.sleep(backoff)
                backoff *= 2  # Exponential backoff

    raise AssertionError("Failed to get installation access token.")


def compute_git_blob_sha1(content: bytes) -> str:
    """Compute the SHA-1 hash of a file as Git would.

    Args:
        content: File content as bytes

    Returns:
        SHA-1 hash
    """
    # Calculate the blob header
    size = len(content)
    header = f"blob {size}\0".encode("utf-8")

    # Compute the SHA-1 hash of the header + content
    sha1 = hashlib.sha1()
    sha1.update(header + content)
    return sha1.hexdigest()


def _github_access_token() -> str:
    """Get the GitHub access token.

    Returns:
        GitHub access token
    """
    # Use GITHUB_TOKEN if set, otherwise use OWIDBOT_ACCESS_TOKEN
    if GITHUB_TOKEN:
        return GITHUB_TOKEN
    elif OWIDBOT_ACCESS_TOKEN:
        return OWIDBOT_ACCESS_TOKEN
    else:
        raise AssertionError("You need to set GITHUB_TOKEN or OWIDBOT_ACCESS_TOKEN in your .env file to commit.")


class GithubApiRepo:
    """Class for interacting with a GitHub repository via the GitHub API."""

    def __init__(
        self, org: str = "owid", repo_name: str = "etl", access_token: Optional[str] = None, use_app_auth: bool = False
    ):
        """Initialize with the organization and repository name.

        Args:
            org: GitHub organization name
            repo_name: Repository name
            access_token: Optional access token. If None, uses default token.
            use_app_auth: If True, uses GitHub App authentication
        """
        self.org = org
        self.repo_name = repo_name
        self.full_repo_name = f"{org}/{repo_name}"

        if use_app_auth:
            self.access_token = github_app_access_token()
        elif access_token:
            self.access_token = access_token
        else:
            # Try to get a token from environment variables
            try:
                self.access_token = _github_access_token()
            except AssertionError:
                self.access_token = None

        self.g = get_github_instance(self.access_token)
        self._repo = None

    @property
    def repo(self) -> Repository:
        """Get the PyGithub Repository object (lazily loaded)."""
        if self._repo is None:
            if self.full_repo_name.count("/") == 1:
                # Get by org/repo format
                self._repo = self.g.get_repo(self.full_repo_name)
            else:
                # Get by org and repo separately
                self._repo = self.g.get_organization(self.org).get_repo(self.repo_name)
        return self._repo

    def fetch_file_content(self, file_path: str, branch: str) -> str:
        """Fetch file content from GitHub.

        Args:
            file_path: The path to the file in the repository
            branch: The branch name

        Returns:
            The decoded content of the file
        """
        try:
            content_file = self.repo.get_contents(file_path, ref=branch)
            if isinstance(content_file, list):
                # This means we got a directory instead of a file
                raise ValueError(f"Path {file_path} is a directory, not a file")

            # PyGithub's decoded_content is bytes, decode it to a string
            content_bytes = content_file.decoded_content
            return str(content_bytes.decode("utf-8") if isinstance(content_bytes, bytes) else content_bytes)
        except GithubException as e:
            if e.status == 404:
                raise FileNotFoundError(f"File {file_path} not found on branch {branch}")
            raise

    def merge_with_master(self, branch_name: str) -> bool:
        """Merge master into the specified branch.

        Args:
            branch_name: The name of the branch to merge master into

        Returns:
            bool: True if merge was successful, False otherwise
        """
        try:
            # Get the branch object
            branch = self.repo.get_branch(branch_name)
            master = self.repo.get_branch("master")

            # If branch is already up to date with master, no need to merge
            if branch.commit.sha == master.commit.sha:
                log.info(f"Branch {branch_name} is already up to date with master")
                return True

            # Create a merge commit
            self.repo.merge(base=branch_name, head="master", commit_message=f"Merge master into {branch_name}")

            log.info(f"Successfully merged master into {branch_name}")
            return True

        except GithubException as e:
            # Handle merge conflicts or other errors
            log.error(f"Failed to merge master into {branch_name}: {e}")
            return False

    def merge_with_master_resolve_conflicts(self, branch_name: str) -> None:
        """Merge master into the specified branch, creating a merge commit even with conflicts."""
        # Get the current branch and master SHAs
        branch = self.repo.get_branch(branch_name)
        master = self.repo.get_branch("master")

        # If branch is already up to date with master, no need to merge
        if branch.commit.sha == master.commit.sha:
            log.info(f"Branch {branch_name} is already up to date with master")
            return

        # Try normal merge first
        try:
            self.repo.merge(base=branch_name, head="master", commit_message=f"Merge master into {branch_name}")
            log.info(f"Successfully merged master into {branch_name}")
            return
        except GithubException:
            # If merge fails due to conflicts, create a merge commit manually
            log.warning(
                "Merge conflicts detected, creating merge commit using master's tree (conflicts will be resolved by file commits)"
            )

            branch_commit = self.repo.get_git_commit(branch.commit.sha)
            master_commit = self.repo.get_git_commit(master.commit.sha)

            # Use master's tree as the base - this brings in all the master changes
            # Our subsequent file commits will override any conflicted files
            master_tree_sha = master_commit.tree.sha

            # Create merge commit with both parents but using master's tree
            merge_commit = self.repo.create_git_commit(
                message=f"Merge master into {branch_name} (conflicts to be resolved by file commits)",
                tree=self.repo.get_git_tree(master_tree_sha),
                parents=[branch_commit, master_commit],
            )

            # Update the branch reference
            self.update_branch_reference(branch_name, merge_commit.sha)

            log.info(
                f"Created merge commit for {branch_name} using master's tree, conflicts will be resolved by file commits"
            )

    def get_open_prs(self, branch_name: str) -> List[PullRequest]:
        """Get open pull requests for a specific branch.

        Args:
            branch_name: The name of the branch

        Returns:
            List of pull request data
        """
        return list(self.repo.get_pulls(state="open", head=f"{self.org}:{branch_name}"))

    def get_pr(self, branch_name: str) -> Optional[PullRequest]:
        """Get a pull request for a branch.

        Args:
            branch_name: The name of the branch

        Returns:
            Pull request object or None if not found
        """
        # Find pull requests for the branch (assuming you're looking for open PRs)
        pulls = self.repo.get_pulls(state="open", sort="created", head=f"{self.org}:{branch_name}")
        pulls = list(pulls)

        if len(pulls) == 0:
            return None
        elif len(pulls) > 1:
            log.warning(f"More than one open PR found for branch {branch_name}. Taking the most recent one.")
            return pulls[-1]

        return pulls[0]

    def get_all_prs(self, exclude_dependabot: bool = True) -> list[str]:
        """Get all open PRs from the repository.

        Args:
            exclude_dependabot: If True, exclude dependabot PRs

        Returns:
            List of PR branches
        """
        # Get all open PRs using PyGithub's get_pulls method
        pulls = self.repo.get_pulls(state="open", sort="created")

        active_prs = []
        for pr in pulls:
            # Only include PRs from the same org
            if pr.head.repo and pr.head.repo.owner.login == self.org:
                active_prs.append(pr.head.ref)

        # Exclude dependabot PRs if requested
        if exclude_dependabot:
            active_prs = [pr for pr in active_prs if "dependabot" not in pr]

        return active_prs

    def get_comment_from_pr(self, pr: PullRequest, username: str = "owidbot") -> Optional[Any]:
        """Get a comment from a PR by username.

        Args:
            pr: Pull request object
            username: Username of the commenter

        Returns:
            Comment object or None
        """
        comments = pr.get_issue_comments()
        matching_comments = [comment for comment in comments if comment.user.login == username]

        if len(matching_comments) == 0:
            return None
        elif len(matching_comments) == 1:
            return matching_comments[0]
        else:
            raise AssertionError(f"More than one {username} comment found.")

    def get_master_commit_sha(self) -> str:
        """Get the latest commit SHA on master branch.

        Returns:
            The SHA of the latest commit on master
        """
        master = self.repo.get_branch("master")
        return master.commit.sha

    def check_branch_exists(self, branch_name: str) -> tuple[bool, dict]:
        """Check if a branch exists.

        Args:
            branch_name: The name of the branch

        Returns:
            Tuple of (exists, branch_data)
        """
        try:
            branch = self.repo.get_branch(branch_name)
            return True, {"object": {"sha": branch.commit.sha}}
        except GithubException:
            return False, {}

    def create_branch(self, branch_name: str, base_sha: str) -> bool:
        """Create a new branch pointing to the specified commit.

        Args:
            branch_name: The name of the new branch
            base_sha: The commit SHA to point to

        Returns:
            True if successful
        """
        # Create a reference (branch)
        self.repo.create_git_ref(f"refs/heads/{branch_name}", base_sha)
        return True

    def create_branch_if_not_exists(self, branch_name: str, dry_run: bool = False) -> bool:
        """Create a branch if it doesn't exist.

        Args:
            branch_name: The name of the branch
            dry_run: If True, don't actually create the branch

        Returns:
            True if branch exists or was created
        """
        try:
            self.repo.get_branch(branch_name)
            return True  # Branch already exists
        except GithubException as e:
            if e.status == 404:
                if not dry_run:
                    try:
                        # Try main first, then master
                        try:
                            master_ref = self.repo.get_branch("main").commit.sha
                            log.info(f"Using 'main' branch as reference for creating {branch_name}.")
                        except GithubException:
                            master_ref = self.repo.get_branch("master").commit.sha
                            log.info(f"Using 'master' branch as reference for creating {branch_name}.")

                        log.info(f"Creating branch {branch_name} with reference {master_ref}.")
                        self.create_branch(branch_name, master_ref)

                    except Exception as e:
                        log.error(f"Failed to create branch {branch_name}: {e}")
                        return False

                log.info(f"Branch {branch_name} {'would be' if dry_run else 'was'} created in {self.full_repo_name}.")
                return not dry_run  # True if not dry_run, False otherwise
            else:
                raise e

    def delete_branch(self, branch_name: str) -> bool:
        """Delete a branch.

        Args:
            branch_name: The name of the branch to delete

        Returns:
            True if successful
        """
        # Get the reference and delete it
        ref = self.repo.get_git_ref(f"heads/{branch_name}")
        ref.delete()

        log.info(f"Deleted branch {branch_name}")
        return True

    def create_tree(self, base_tree_sha: str, tree_items: List[Dict[str, str]]) -> str:
        """Create a git tree.

        Args:
            base_tree_sha: The SHA of the base tree
            tree_items: List of items to include in the tree

        Returns:
            The SHA of the created tree
        """
        # Convert our tree items to InputGitTreeElements
        git_tree_elements = [
            InputGitTreeElement(path=item["path"], mode=item["mode"], type=item["type"], content=item["content"])
            for item in tree_items
        ]

        # Create the tree
        tree = self.repo.create_git_tree(git_tree_elements, base_tree=self.repo.get_git_tree(base_tree_sha))
        return tree.sha

    def create_commit(self, message: str, tree_sha: str, parent_sha: str) -> str:
        """Create a git commit.

        Args:
            message: The commit message
            tree_sha: The SHA of the tree for this commit
            parent_sha: The SHA of the parent commit

        Returns:
            The SHA of the created commit
        """
        # Get the parent commit
        parent = self.repo.get_git_commit(parent_sha)

        # Create the commit
        commit = self.repo.create_git_commit(message=message, tree=self.repo.get_git_tree(tree_sha), parents=[parent])

        return commit.sha

    def update_branch_reference(self, branch_name: str, commit_sha: str, force: bool = True) -> bool:
        """Update a branch reference to point to a commit.

        Args:
            branch_name: The name of the branch to update
            commit_sha: The SHA of the commit to point to
            force: If True, allows non-fast-forward updates (default: True)

        Returns:
            True if successful
        """
        # Get the reference
        ref = self.repo.get_git_ref(f"heads/{branch_name}")

        # Update it
        ref.edit(commit_sha, force=force)
        return True

    def create_pull_request(self, title: str, branch_name: str, body: str = "") -> str:
        """Create a pull request.

        Args:
            title: The title of the pull request
            branch_name: The name of the branch for this PR
            body: The body text for the PR

        Returns:
            The URL of the created pull request
        """
        # Create the PR
        pr = self.repo.create_pull(
            title=title,
            body=body,
            head=branch_name,  # PyGithub will handle prefixing with owner
            base="master",
        )

        return pr.html_url

    def create_commit_with_files(
        self,
        files: List[Path],
        branch_name: str,
        commit_message: str,
        base_dir: Path = BASE_DIR,
        parent_sha: Optional[str] = None,
        base_tree_sha: Optional[str] = None,
    ) -> tuple[bool, str]:
        """Create a commit with the provided files.

        This function:
        1. Reads the content of each file
        2. Compares with the remote version to skip unchanged files
        3. Creates a tree with the changed files
        4. Creates a commit
        5. Updates the branch reference

        Args:
            files: List of Path objects to the files to include
            branch_name: Name of the branch to commit to
            commit_message: Commit message
            base_dir: Base directory for calculating relative paths (default: BASE_DIR)
            parent_sha: SHA of the parent commit (if None, uses master_sha)
            base_tree_sha: SHA of the base tree (if None, uses parent_sha)

        Returns:
            Tuple of (has_changes, commit_sha)
        """
        # If parent_sha is not provided, use master SHA
        if parent_sha is None:
            parent_sha = self.get_master_commit_sha()

        # If base_tree_sha is not provided, use parent_sha
        if base_tree_sha is None:
            base_tree_sha = parent_sha

        # Gather files in snapshot_dir
        tree_items = []
        for filepath in files:
            with filepath.open("r", encoding="utf-8") as fp:
                content = fp.read()

            # Skip if the remote content is the same
            try:
                remote_content = self.fetch_file_content(str(filepath.relative_to(base_dir)), branch_name)
                if remote_content == content:
                    continue
            except FileNotFoundError:
                # If the file doesn't exist remotely, include it
                pass
            except ValueError as e:
                # If it's a directory, skip it
                if "directory" in str(e):
                    continue
                raise

            # Build the tree structure
            repo_path = str(filepath.relative_to(base_dir))
            tree_items.append(
                {
                    "path": repo_path,
                    "mode": "100644",
                    "type": "blob",
                    "content": content,
                }
            )

        # Don't update if there are no changes
        has_changes = len(tree_items) > 0
        if not has_changes:
            return False, ""

        # Create a tree for all files
        tree_sha = self.create_tree(base_tree_sha, tree_items)

        # Create a commit
        new_commit_sha = self.create_commit(commit_message, tree_sha, parent_sha)

        # Update the branch to point to the new commit
        self.update_branch_reference(branch_name, new_commit_sha)

        return True, new_commit_sha

    def commit_file(
        self,
        content: str,
        file_path: str,
        commit_message: str,
        branch: str,
        dry_run: bool = True,
    ) -> bool:
        """Commit a file to GitHub.

        Args:
            content: File content
            file_path: Path to the file in the repository
            commit_message: Commit message
            branch: Branch name
            dry_run: If True, don't actually commit the file

        Returns:
            True if the file was committed
        """
        new_content_checksum = compute_git_blob_sha1(content.encode("utf-8"))

        try:
            # Check if the file already exists
            contents = self.repo.get_contents(file_path, ref=branch)
            assert not isinstance(contents, list)

            # Compare the existing content with the new content
            if contents.sha == new_content_checksum:
                log.info(
                    f"File {file_path} is identical to the current version in {self.full_repo_name} on branch {branch}. No commit will be made."
                )
                return False

            # Update the file
            if not dry_run:
                self.repo.update_file(contents.path, commit_message, content, contents.sha, branch=branch)
        except Exception as e:
            # If the file doesn't exist, create a new file
            if "404" in str(e):
                if not dry_run:
                    self.repo.create_file(file_path, commit_message, content, branch=branch)
            else:
                raise e

        if dry_run:
            log.info(f"Would have committed {file_path} to {self.full_repo_name} on branch {branch}.")
        else:
            log.info(f"Committed {file_path} to {self.full_repo_name} on branch {branch}.")

        return not dry_run  # True if not dry_run, False otherwise

    def get_git_branch_from_commit_sha(self, commit_sha: str) -> str:
        """Get the branch name from a merged pull request commit sha.

        This is useful for Buildkite jobs where we only have the commit sha.

        Args:
            commit_sha: Commit SHA

        Returns:
            Branch name
        """
        # First verify that the commit exists
        self.repo.get_commit(commit_sha)

        # Use PyGithub's internal requester to make the API call
        # This endpoint isn't directly exposed in PyGithub's public API
        endpoint = f"/repos/{self.org}/{self.repo_name}/commits/{commit_sha}/pulls"
        _, data = self.repo._requester.requestJsonAndCheck("GET", endpoint)

        # Filter the closed ones
        closed_pull_requests = [pr for pr in data if pr["state"] == "closed"]

        # Get the branch of the most recent one
        if closed_pull_requests:
            return closed_pull_requests[0]["head"]["ref"]
        else:
            raise ValueError(f"No closed pull requests found for commit {commit_sha}")

    def create_check_run(
        self,
        head_sha: str,
        name: str,
        conclusion: str,
        title: str,
        summary: str,
        text: Optional[str] = None,
    ) -> Any:
        """Create a check run for a commit.

        Args:
            head_sha: SHA of the commit
            name: Name of the check run
            conclusion: Conclusion of the check run (success, failure, neutral, etc.)
            title: Title of the check run
            summary: Summary of the check run
            text: Additional text for the check run

        Returns:
            Check run object
        """
        output = {
            "title": title,
            "summary": summary,
        }
        if text:
            output["text"] = text

        check_run = self.repo.create_check_run(
            name=name,
            head_sha=head_sha,
            status="completed",
            conclusion=conclusion,
            output=output,  # type: ignore
        )

        return check_run
