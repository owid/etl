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

    # Extract semantic-layer tables.
    # The analytics "semantic layer" used to be a dedicated DuckDB database exposed in Metabase, but it
    # was migrated to the `prod_semantic` dataset on BigQuery (owid/analytics#735). In db_docs.yml these
    # tables now live under the top-level `bigquery:` key (the `metabase:` key is empty), each qualified
    # as `owid-analytics:prod_semantic.<table>`. We keep only the semantic-layer tables and present them
    # with their dataset-qualified name (`prod_semantic.<table>`), which is how queries must reference
    # them on BigQuery (see etl/analytics/data.py).
    assert "bigquery" in yaml_data, "YAML data must contain 'bigquery' key"
    project_prefix = "owid-analytics:"
    semantic_prefix = f"{project_prefix}prod_semantic."
    tables = [
        {**item, "table": item["table"][len(project_prefix) :], "description": item.get("description", "")}
        for item in yaml_data["bigquery"]
        if str(item.get("table", "")).startswith(semantic_prefix)
    ]
    if not tables:
        raise ValueError("No semantic-layer (prod_semantic) tables found in bigquery section of analytics docs")
    return tables
