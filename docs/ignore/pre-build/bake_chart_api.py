#!/usr/bin/env python
"""
Generate Chart API documentation from OpenAPI specification and description file.

Called from: make docs.pre
Input: Fetched from owid/owid-grapher repository on GitHub (docs/chart-api.openapi.yaml and docs/chart-api.md)
Output: docs/api/chart-api.md
"""

from pathlib import Path
import yaml
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

    print(f"Writing documentation to {output_path}...")
    # Combine description and API docs
    full_docs = description + "\n\n" + api_docs
    output_path.write_text(full_docs)

    print("✓ Chart API documentation generated successfully!")

if __name__ == "__main__":
    main()
