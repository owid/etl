"""Utility functions for fetching and processing OpenAPI specs from GitHub."""

from pathlib import Path
from typing import Any, Dict
import re
import yaml
from github import Github
from etl.git_api_helpers import GithubApiRepo
from .openapi_to_markdown import generate_markdown


def get_current_branch() -> str:
    """Get the current git branch name using PyGithub."""
    try:
        # Get the git repository from the current directory
        import git
        repo = git.Repo(search_parent_directories=True)
        return repo.active_branch.name
    except Exception:
        # Fall back to master if we can't detect the branch
        return "master"


def load_openapi_spec_from_github(
    org: str,
    repo: str,
    file_path: str,
    branch: str | None = None,
) -> Dict[str, Any]:
    """Load OpenAPI specification from GitHub repository.

    Args:
        org: GitHub organization name
        repo: Repository name
        file_path: Path to the OpenAPI spec file in the repository
        branch: Branch to fetch from. If None, auto-detects current branch with fallback to master.

    Returns:
        Parsed OpenAPI specification
    """
    if branch is None:
        # Auto-detect current branch
        current_branch = get_current_branch()
        
        # Try current branch first (if it's not master)
        if current_branch != "master":
            try:
                print(f"  Trying branch '{current_branch}'...")
                github_repo = GithubApiRepo(org=org, repo_name=repo)
                content = github_repo.fetch_file_content(file_path, current_branch)
                return yaml.safe_load(content)
            except Exception:
                print(f"  Branch '{current_branch}' not found, falling back to master...")
        
        # Fall back to master
        branch = "master"
    
    github_repo = GithubApiRepo(org=org, repo_name=repo)
    content = github_repo.fetch_file_content(file_path, branch)
    return yaml.safe_load(content)


def load_text_from_github(
    org: str,
    repo: str,
    file_path: str,
    branch: str | None = None,
) -> str:
    """Load text file from GitHub repository.

    Args:
        org: GitHub organization name
        repo: Repository name
        file_path: Path to the file in the repository
        branch: Branch to fetch from. If None, auto-detects current branch with fallback to master.

    Returns:
        File content as string
    """
    if branch is None:
        # Auto-detect current branch
        current_branch = get_current_branch()
        
        # Try current branch first (if it's not master)
        if current_branch != "master":
            try:
                print(f"  Trying branch '{current_branch}'...")
                github_repo = GithubApiRepo(org=org, repo_name=repo)
                return github_repo.fetch_file_content(file_path, current_branch)
            except Exception:
                print(f"  Branch '{current_branch}' not found, falling back to master...")
        
        # Fall back to master
        branch = "master"
    
    github_repo = GithubApiRepo(org=org, repo_name=repo)
    return github_repo.fetch_file_content(file_path, branch)


def resolve_parameter_refs(spec: dict) -> dict:
    """Resolve $ref references in parameters."""
    components = spec.get("components", {})
    parameters = components.get("parameters", {})
    
    for path, path_item in spec.get("paths", {}).items():
        for method in ["get", "post", "put", "delete", "patch"]:
            if method in path_item:
                operation = path_item[method]
                if "parameters" in operation:
                    resolved_params = []
                    for param in operation["parameters"]:
                        if "$ref" in param:
                            # Extract the reference name
                            ref_path = param["$ref"].split("/")
                            if ref_path[0] == "#" and ref_path[1] == "components" and ref_path[2] == "parameters":
                                param_name = ref_path[-1]
                                if param_name in parameters:
                                    resolved_params.append(parameters[param_name])
                                else:
                                    resolved_params.append(param)
                            else:
                                resolved_params.append(param)
                        else:
                            resolved_params.append(param)
                    operation["parameters"] = resolved_params
    
    return spec


def extract_frontmatter(content: str) -> tuple[dict, str]:
    """Extract YAML frontmatter from markdown content."""
    pattern = r'^---\s*\n(.*?)\n---\s*\n(.*)$'
    match = re.match(pattern, content, re.DOTALL)
    
    if match:
        frontmatter_str = match.group(1)
        body = match.group(2)
        frontmatter = yaml.safe_load(frontmatter_str)
        return frontmatter, body
    
    return {}, content


def strip_frontmatter(content: str) -> str:
    """Remove YAML frontmatter from markdown content."""
    pattern = r'^---\s*\n.*?\n---\s*\n'
    return re.sub(pattern, '', content, count=1, flags=re.DOTALL)
