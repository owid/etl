from pathlib import Path
from typing import List

from owid import catalog

CURRENT_DIR = Path(__file__).parent
VERSION = CURRENT_DIR.name


def gather_sources_from_tables(
    tables: List[catalog.Table],
) -> List[catalog.meta.Source]:
    # Initialise list that will gather all unique metadata sources from the tables.
    known_sources: List[catalog.meta.Source] = []
    for table in tables:
        # Get list of sources of the dataset of current table.
        table_sources = table.metadata.dataset.sources
        for source in table_sources:
            # Go source by source of current table, and check if its name is not already in the list of known sources.
            if all(
                [source.name != known_source.name for known_source in known_sources]
            ):
                # Add the new source to the list.
                known_sources.append(source)

    return known_sources
