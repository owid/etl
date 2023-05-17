"""Client for version 1 of chart_revision."""
import json
from typing import Optional

from etl.chart_revision.v1.revision import create_and_submit_charts_revisions


def main(mapping_file: str, revision_reason: Optional[str] = None) -> None:
    """Execute chart_revision version 1."""
    # Load mapping
    with open(mapping_file, "r") as f:
        variable_mapping = json.load(f)
        variable_mapping = {int(k): int(v) for k, v in variable_mapping.items()}
    create_and_submit_charts_revisions(variable_mapping, revision_reason)
