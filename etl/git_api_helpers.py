"""
Helpers for working with Git through PyGithub library.
"""

import base64
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

from github import Github, GithubException, InputGitTreeElement
from github.Repository import Repository
from structlog import get_logger

from etl.config import GITHUB_TOKEN
from etl.paths import BASE_DIR

# Initialize logger.
log = get_logger()


def get_github_instance() -> Github:
    """Return a PyGithub instance authenticated with the token."""
    return Github(GITHUB_TOKEN)


class GithubApiRepo:
    """Class for interacting with a GitHub repository via the GitHub API."""

    def __init__(self, org: str = "owid", repo_name: str = "etl"):
        """Initialize with the organization and repository name.

        Args:
            org: GitHub organization name
            repo_name: Repository name
        """
        self.org = org
        self.repo_name = repo_name
        self.g = get_github_instance()
        self._repo = None

    @property
    def repo(self) -> Repository:
        """Get the PyGithub Repository object (lazily loaded)."""
        if self._repo is None:
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
            return content_bytes.decode("utf-8") if isinstance(content_bytes, bytes) else content_bytes
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

    def get_open_prs(self, branch_name: str) -> List[Dict[str, Any]]:
        """Get open pull requests for a specific branch.

        Args:
            branch_name: The name of the branch

        Returns:
            List of pull request data
        """
        pulls = self.repo.get_pulls(state="open", head=f"{self.org}:{branch_name}")

        # Convert PyGithub objects to dictionaries to maintain compatibility
        return [{"html_url": pr.html_url, "number": pr.number, "title": pr.title, "state": pr.state} for pr in pulls]

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

    def update_branch_reference(self, branch_name: str, commit_sha: str) -> bool:
        """Update a branch reference to point to a commit.

        Args:
            branch_name: The name of the branch to update
            commit_sha: The SHA of the commit to point to

        Returns:
            True if successful
        """
        # Get the reference
        ref = self.repo.get_git_ref(f"heads/{branch_name}")

        # Update it
        ref.edit(commit_sha, force=False)
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
