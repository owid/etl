"""
Helpers for working with Git through their REST API.
"""

import base64
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from structlog import get_logger

from etl.config import GITHUB_API_BASE, GITHUB_API_URL, GITHUB_TOKEN
from etl.paths import BASE_DIR

# Initialize logger.
log = get_logger()


def get_github_auth_headers() -> Dict[str, str]:
    """Return GitHub API authentication headers."""
    return {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json",
    }


def fetch_file_from_github(file_path: str, branch: str) -> str:
    """Fetch file content from GitHub.

    Args:
        file_path: The path to the file in the repository
        branch: The branch name

    Returns:
        The decoded content of the file
    """
    headers = get_github_auth_headers()

    url = f"{GITHUB_API_BASE}/contents/{file_path}?ref={branch}"
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    data = resp.json()

    content_base64 = data["content"]  # Base64-encoded
    return base64.b64decode(content_base64).decode("utf-8")


def merge_branch_with_master(branch_name: str) -> bool:
    """Merge master into the specified branch via GitHub API.

    Args:
        branch_name: The name of the branch to merge master into

    Returns:
        bool: True if merge was successful, False otherwise
    """
    headers = get_github_auth_headers()

    # Get the latest commit SHA on master
    ref_resp = requests.get(f"{GITHUB_API_BASE}/git/ref/heads/master", headers=headers)
    ref_resp.raise_for_status()
    master_sha = ref_resp.json()["object"]["sha"]

    # Get the latest commit SHA on the branch
    branch_resp = requests.get(f"{GITHUB_API_BASE}/git/ref/heads/{branch_name}", headers=headers)
    if branch_resp.status_code != 200:
        log.warning(f"Branch {branch_name} does not exist")
        return False
    branch_sha = branch_resp.json()["object"]["sha"]

    # If branch is already up to date with master, no need to merge
    if master_sha == branch_sha:
        log.info(f"Branch {branch_name} is already up to date with master")
        return True

    # Create a merge commit
    merge_data = {"base": branch_name, "head": "master", "commit_message": f"Merge master into {branch_name}"}

    merge_url = f"{GITHUB_API_BASE}/merges"
    merge_resp = requests.post(merge_url, json=merge_data, headers=headers)

    if merge_resp.status_code == 204:
        # 204 means branch is up to date, no merge needed
        log.info(f"Branch {branch_name} is up to date with master, no merge needed")
        return True
    elif merge_resp.status_code == 201:
        # 201 means merge was created successfully
        log.info(f"Successfully merged master into {branch_name}")
        return True
    else:
        # Handle merge conflicts or other errors
        log.error(f"Failed to merge master into {branch_name}: {merge_resp.status_code} - {merge_resp.text}")
        return False


def get_open_prs_for_branch(branch_name: str) -> List[Dict[str, Any]]:
    """Get open pull requests for a specific branch.

    Args:
        branch_name: The name of the branch

    Returns:
        List of pull request data
    """
    headers = get_github_auth_headers()

    # Check for an existing pull request
    pr_search_resp = requests.get(
        GITHUB_API_URL, headers=headers, params={"state": "open", "head": f"owid:{branch_name}"}
    )
    pr_search_resp.raise_for_status()
    return pr_search_resp.json()


def get_master_commit_sha() -> str:
    """Get the latest commit SHA on master branch.

    Returns:
        The SHA of the latest commit on master
    """
    headers = get_github_auth_headers()
    ref_resp = requests.get(f"{GITHUB_API_BASE}/git/ref/heads/master", headers=headers)
    ref_resp.raise_for_status()
    return ref_resp.json()["object"]["sha"]


def check_branch_exists(branch_name: str) -> tuple[bool, dict]:
    """Check if a branch exists.

    Args:
        branch_name: The name of the branch

    Returns:
        Tuple of (exists, branch_data)
    """
    headers = get_github_auth_headers()
    branch_url = f"{GITHUB_API_BASE}/git/ref/heads/{branch_name}"
    branch_resp = requests.get(branch_url, headers=headers)

    if branch_resp.status_code == 200:
        return True, branch_resp.json()
    return False, {}


def create_branch(branch_name: str, base_sha: str) -> bool:
    """Create a new branch pointing to the specified commit.

    Args:
        branch_name: The name of the new branch
        base_sha: The commit SHA to point to

    Returns:
        True if successful
    """
    headers = get_github_auth_headers()
    create_ref_data = {"ref": f"refs/heads/{branch_name}", "sha": base_sha}
    create_ref_resp = requests.post(f"{GITHUB_API_BASE}/git/refs", json=create_ref_data, headers=headers)
    create_ref_resp.raise_for_status()
    return True


def delete_branch(branch_name: str) -> bool:
    """Delete a branch.

    Args:
        branch_name: The name of the branch to delete

    Returns:
        True if successful
    """
    headers = get_github_auth_headers()
    delete_ref_resp = requests.delete(f"{GITHUB_API_BASE}/git/refs/heads/{branch_name}", headers=headers)
    delete_ref_resp.raise_for_status()
    log.info(f"Deleted branch {branch_name}")
    return True


def create_tree(base_tree_sha: str, tree_items: List[Dict[str, str]]) -> str:
    """Create a git tree.

    Args:
        base_tree_sha: The SHA of the base tree
        tree_items: List of items to include in the tree

    Returns:
        The SHA of the created tree
    """
    headers = get_github_auth_headers()
    tree_data = {"base_tree": base_tree_sha, "tree": tree_items}
    create_tree_resp = requests.post(f"{GITHUB_API_BASE}/git/trees", json=tree_data, headers=headers)
    create_tree_resp.raise_for_status()
    return create_tree_resp.json()["sha"]


def create_commit(message: str, tree_sha: str, parent_sha: str) -> str:
    """Create a git commit.

    Args:
        message: The commit message
        tree_sha: The SHA of the tree for this commit
        parent_sha: The SHA of the parent commit

    Returns:
        The SHA of the created commit
    """
    headers = get_github_auth_headers()
    commit_data = {
        "message": message,
        "tree": tree_sha,
        "parents": [parent_sha],
    }
    create_commit_resp = requests.post(f"{GITHUB_API_BASE}/git/commits", json=commit_data, headers=headers)
    create_commit_resp.raise_for_status()
    return create_commit_resp.json()["sha"]


def update_branch_reference(branch_name: str, commit_sha: str) -> bool:
    """Update a branch reference to point to a commit.

    Args:
        branch_name: The name of the branch to update
        commit_sha: The SHA of the commit to point to

    Returns:
        True if successful
    """
    headers = get_github_auth_headers()
    update_ref_data = {"sha": commit_sha}
    update_ref_resp = requests.patch(
        f"{GITHUB_API_BASE}/git/refs/heads/{branch_name}", json=update_ref_data, headers=headers
    )
    update_ref_resp.raise_for_status()
    return True


def create_pull_request(title: str, branch_name: str, body: str = "") -> str:
    """Create a pull request.

    Args:
        title: The title of the pull request
        branch_name: The name of the branch for this PR
        body: The body text for the PR

    Returns:
        The URL of the created pull request
    """
    headers = get_github_auth_headers()
    pr_data = {
        "title": title,
        "head": branch_name,
        "base": "master",
        "body": body,
    }
    pr_resp = requests.post(GITHUB_API_URL, json=pr_data, headers=headers)
    pr_resp.raise_for_status()
    return pr_resp.json()["html_url"]


def create_commit_with_files(
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
        parent_sha = get_master_commit_sha()

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
            remote_content = fetch_file_from_github(str(filepath.relative_to(base_dir)), branch_name)
            if remote_content == content:
                continue
        except requests.exceptions.HTTPError as e:
            # If file doesn't exist remotely (404), include it
            if e.response.status_code != 404:
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
    tree_sha = create_tree(base_tree_sha, tree_items)

    # Create a commit
    new_commit_sha = create_commit(commit_message, tree_sha, parent_sha)

    # Update the branch to point to the new commit
    update_branch_reference(branch_name, new_commit_sha)

    return True, new_commit_sha
