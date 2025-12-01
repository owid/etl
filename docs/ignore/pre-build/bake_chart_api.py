#!/usr/bin/env python
"""
Generate Chart API documentation from OpenAPI specification and description file.

Called from: make docs.pre
Input: Fetched from owid/owid-grapher repository on GitHub (docs/chart-api.openapi.yaml and docs/chart-api.md)
Output: docs/api/chart-api.md
"""

from pathlib import Path
import yaml
import re
from etl.git_api_helpers import GithubApiRepo
from .openapi_to_markdown import generate_markdown

def load_openapi_spec_from_github(org: str, repo: str, file_path: str, branch: str = "master") -> dict:
    github_repo = GithubApiRepo(org=org, repo_name=repo)
    content = github_repo.fetch_file_content(file_path, branch)
    return yaml.safe_load(content)

def load_text_from_github(org: str, repo: str, file_path: str, branch: str = "master") -> str:
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

def main():
    repo_root = Path(__file__).parent.parent.parent.parent
    output_path = repo_root / "docs" / "api" / "chart-api.md"

    print("Fetching OpenAPI spec from GitHub (owid/owid-grapher)...")
    spec = load_openapi_spec_from_github(
        org="owid", repo="owid-grapher", file_path="docs/chart-api.openapi.yaml", branch="feat/chart-api-docs"
    )

    print("Fetching description from GitHub (owid/owid-grapher)...")
    description = load_text_from_github(
        org="owid", repo="owid-grapher", file_path="docs/chart-api.md", branch="feat/chart-api-docs"
    )

    print("Resolving parameter references...")
    spec = resolve_parameter_refs(spec)

    print("Generating markdown documentation...")
    api_docs = generate_markdown(spec)

    print("Extracting frontmatter...")
    # Extract frontmatter from description
    desc_frontmatter, desc_body = extract_frontmatter(description)
    
    # Strip frontmatter from generated API docs
    api_docs_body = strip_frontmatter(api_docs)

    print(f"Writing documentation to {output_path}...")
    # Reconstruct with proper frontmatter
    frontmatter_yaml = yaml.dump(desc_frontmatter, default_flow_style=False, sort_keys=False)
    full_docs = f"---\n{frontmatter_yaml}---\n\n{desc_body}\n\n{api_docs_body}"
    output_path.write_text(full_docs)

    print("✓ Chart API documentation generated successfully!")

if __name__ == "__main__":
    main()
