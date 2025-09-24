#!/usr/bin/env python3
"""
Concurrent version for faster YAML file reading from analytics repository.
"""

from typing import cast

import yaml

from etl.git_api_helpers import GithubApiRepo

def get_metabase_db_docs():
    """Get documentation of all databases in Metabase.

    Documentation is obtained from a daily exported YAML file in the analytics repository.

    Returns:
        List of dictionaries, where each dictionary is the parsed content of a YAML file
    """
    from github import ContentFile
    repo = GithubApiRepo(org="owid", repo_name="analytics")
    docs_file = "docs/db/db_docs.yml"
    branch = "main"

    # Get file reference
    file_ref = repo.repo.get_contents(docs_file, ref=branch)
    if not isinstance(file_ref, ContentFile.ContentFile):
        raise ValueError(f"Expected file but got directory: {docs_file}")
    file_ref = cast(ContentFile.ContentFile, file_ref)

    # Read file content
    try:
        # Try decoded_content first (faster)
        file_content = file_ref.decoded_content.decode("utf-8")
    except Exception:
        # Fallback to API call
        file_content = repo.fetch_file_content(file_ref.path, branch)

    # Parse YAML content
    try:
        yaml_data = yaml.safe_load(file_content)
    except yaml.YAMLError as e:
        raise RuntimeError(f"Failed to parse YAML file {file_ref.name}: {e}")

    # Extract semantic layer
    assert "metabase" in yaml_data, "YAML data must contain 'metabase' key"
    semantic_db = next((item for item in yaml_data["metabase"] if "semantic" in item.get("database_name", "").lower()), None)
    if semantic_db is None:
        raise ValueError("No semantic database found in metabase data")
    assert "tables" in semantic_db, "Semantic database must contain 'tables' key"
    return semantic_db["tables"]
