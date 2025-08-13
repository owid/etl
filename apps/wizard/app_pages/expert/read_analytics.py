#!/usr/bin/env python3
"""
Concurrent version for faster YAML file reading from analytics repository.
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List

import yaml

from etl.git_api_helpers import GithubApiRepo


def _transform_table_docs_yaml(yaml_data: Dict[str, Any], tb_name: str) -> Dict[str, Any]:
    """
    Transform YAML structure to standardized format with name, description, and fields.

    Args:
        yaml_data: Raw YAML data dictionary

    Returns:
        Transformed dictionary with keys: name, description, fields
    """
    # Extract table name and description
    assert "metadata" in yaml_data, "YAML data must contain 'metadata' key"
    table_description = yaml_data["metadata"].get("description", "")

    # Transform columns/fields
    columns = []
    assert "fields" in yaml_data, "YAML data must contain 'fields' key"
    for key, value in yaml_data["fields"].items():
        field = {"name": key, "description": value.get("description", "")}
        columns.append(field)

    return {
        "name": tb_name,
        "description": table_description,
        "columns": columns,
    }


def get_analytics_db_docs(max_workers: int = 10) -> List[Dict[str, Any]]:
    """
    Read all YAML files from analytics repository using concurrent requests.

    Args:
        max_workers: Maximum number of concurrent threads for API requests

    Returns:
        List of dictionaries, where each dictionary is the parsed content of a YAML file
    """
    repo = GithubApiRepo(org="owid", repo_name="analytics")
    semantic_dir = "analytics/duckdb/semantic/docs"
    branch = "main"

    # Get directory contents
    contents = repo.repo.get_contents(semantic_dir, ref=branch)
    if not isinstance(contents, list):
        raise ValueError(f"Expected directory but got file: {semantic_dir}")

    # Filter for YAML files
    yaml_files = [f for f in contents if f.name.endswith((".yml", ".yaml"))]

    def fetch_and_parse_yaml(file_info):
        """Helper function to fetch and parse a single YAML file."""
        tb_name = Path(file_info.path).stem
        try:
            # Try decoded_content first (faster)
            file_content = file_info.decoded_content.decode("utf-8")
        except:
            # Fallback to API call
            file_content = repo.fetch_file_content(file_info.path, branch)

        try:
            yaml_data = yaml.safe_load(file_content)
            return _transform_table_docs_yaml(yaml_data, tb_name=tb_name)
        except yaml.YAMLError as e:
            raise RuntimeError(f"Failed to parse YAML file {file_info.name}: {e}")

    yaml_contents = []

    # Use ThreadPoolExecutor for concurrent processing
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_file = {executor.submit(fetch_and_parse_yaml, file_info): file_info for file_info in yaml_files}

        # Collect results as they complete
        for future in as_completed(future_to_file):
            try:
                yaml_data = future.result()
                yaml_contents.append(yaml_data)
            except Exception as e:
                file_info = future_to_file[future]
                raise RuntimeError(f"Failed to process {file_info.name}: {e}")

    return yaml_contents


def get_analytics_db_docs_as_text() -> str:
    """
    Read analytics semantic YAML files and return them as a single YAML string.

    Returns:
        YAML string containing all table documentation
    """
    # Get the transformed data
    tables_data = get_analytics_db_docs()

    # Convert back to YAML string
    return yaml.dump(tables_data, default_flow_style=False, sort_keys=False, indent=2)
