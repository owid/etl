#
#  filters.py
#  Filter parsing and matching for browser UI
#

import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple


@dataclass
class StepPath:
    """Minimal step path parser for filter matching.

    Parses step URIs like "data://garden/who/2024-01-15/gho" into components.
    This is a lightweight alternative to CatalogPath that avoids heavy imports.
    """

    channel: Optional[str] = None
    namespace: Optional[str] = None
    version: Optional[str] = None
    dataset: Optional[str] = None

    @classmethod
    def from_uri(cls, uri: str) -> Optional["StepPath"]:
        """Parse a step URI into components.

        Args:
            uri: Step URI like "data://garden/who/2024-01-15/gho"

        Returns:
            StepPath with parsed components, or None if parsing fails.
        """
        # Strip protocol prefix
        if "://" in uri:
            path_str = uri.split("://", 1)[1]
        else:
            path_str = uri

        parts = path_str.split("/")
        if len(parts) < 4:
            return None

        return cls(
            channel=parts[0] if len(parts) > 0 else None,
            namespace=parts[1] if len(parts) > 1 else None,
            version=parts[2] if len(parts) > 2 else None,
            dataset=parts[3] if len(parts) > 3 else None,
        )


# Filter prefix patterns - short and long forms
FILTER_PREFIXES = {
    "n": "namespace",
    "namespace": "namespace",
    "c": "channel",
    "channel": "channel",
    "v": "version",
    "version": "version",
    "d": "dataset",
    "dataset": "dataset",
}

# Pattern to match filter tokens like "n:who" or "namespace:who" or just "n:"
# Uses word boundary (\b) to avoid matching "d:foo" in "invalid:foo"
# Value is optional (\S*) so "v:" highlights immediately while typing
FILTER_PATTERN = re.compile(r"\b(n|namespace|c|channel|v|version|d|dataset):(\S*)", re.IGNORECASE)


@dataclass
class ParsedInput:
    """Parsed user input with filters and search terms."""

    filters: Dict[str, str] = field(default_factory=dict)  # {attribute: value}
    search_terms: List[str] = field(default_factory=list)  # remaining search terms
    filter_spans: List[Tuple[int, int]] = field(default_factory=list)  # (start, end) for highlighting


def parse_filters(text: str) -> ParsedInput:
    """Parse filter prefixes from input text.

    Args:
        text: User input text like "n:who v:2024 population"

    Returns:
        ParsedInput with extracted filters, remaining search terms, and spans for highlighting.

    Examples:
        >>> parse_filters("n:who population")
        ParsedInput(filters={"namespace": "who"}, search_terms=["population"], ...)

        >>> parse_filters("c:garden v:2024 energy")
        ParsedInput(filters={"channel": "garden", "version": "2024"}, search_terms=["energy"], ...)
    """
    filters: Dict[str, str] = {}
    filter_spans: List[Tuple[int, int]] = []

    for match in FILTER_PATTERN.finditer(text):
        prefix = match.group(1).lower()
        value = match.group(2)
        # Always record span for highlighting (even "v:" with no value)
        filter_spans.append((match.start(), match.end()))
        # Only add to filters if value is non-empty
        if value:
            attr = FILTER_PREFIXES[prefix]
            filters[attr] = value

    # Remove filter tokens from text to get search terms
    remaining = FILTER_PATTERN.sub("", text).strip()
    search_terms = remaining.split() if remaining else []

    return ParsedInput(filters=filters, search_terms=search_terms, filter_spans=filter_spans)


def matches_filters(path: StepPath, filters: Dict[str, str]) -> bool:
    """Check if a StepPath matches all filters.

    Args:
        path: StepPath to check
        filters: Dict of {attribute: value} to match against

    Returns:
        True if path matches all filters

    Notes:
        - Version uses prefix matching (v:2024 matches 2024-01-15)
        - Other attributes use case-insensitive exact matching
    """
    for attr, value in filters.items():
        path_value = getattr(path, attr, None)
        if path_value is None:
            return False

        # Version uses prefix matching for flexibility
        if attr == "version":
            if not path_value.startswith(value):
                return False
        else:
            # Case-insensitive exact match for other attributes
            if path_value.lower() != value.lower():
                return False

    return True


def get_step_segment_positions(item: str) -> Dict[str, Tuple[int, int]]:
    """Get start/end positions for each segment of a step URI.

    Args:
        item: Step URI like "data://garden/who/2024-01-15/gho"

    Returns:
        Dict mapping segment names to (start, end) positions:
        {"channel": (7, 13), "namespace": (14, 17), "version": (18, 28), "dataset": (29, 32)}
    """
    # Format: protocol://channel/namespace/version/dataset
    if "://" not in item:
        return {}

    protocol_end = item.index("://") + 3
    rest = item[protocol_end:]
    parts = rest.split("/")

    if len(parts) < 4:
        return {}

    segments: Dict[str, Tuple[int, int]] = {}
    pos = protocol_end

    segment_names = ["channel", "namespace", "version", "dataset"]
    for i, name in enumerate(segment_names):
        if i < len(parts):
            start = pos
            end = pos + len(parts[i])
            segments[name] = (start, end)
            pos = end + 1  # +1 for the slash

    return segments


def find_filter_match_spans(item: str, filters: Dict[str, str]) -> List[Tuple[int, int]]:
    """Find positions of filter-matched segments in a step URI.

    Args:
        item: Step URI like "data://garden/who/2024-01-15/gho"
        filters: Dict of {attribute: value} that were used to filter

    Returns:
        List of (start, end) positions for segments that match filters.
    """
    if not filters:
        return []

    segments = get_step_segment_positions(item)
    if not segments:
        return []

    spans: List[Tuple[int, int]] = []

    for attr, filter_value in filters.items():
        if attr not in segments:
            continue

        start, end = segments[attr]
        segment_value = item[start:end]

        # Check if this segment matches the filter
        if attr == "version":
            # Version uses prefix matching
            if segment_value.startswith(filter_value):
                spans.append((start, end))
        else:
            # Other attributes use case-insensitive exact matching
            if segment_value.lower() == filter_value.lower():
                spans.append((start, end))

    return spans


def apply_filters(items: List[str], filters: Dict[str, str]) -> List[str]:
    """Filter items by step path attributes.

    Args:
        items: List of step URIs (e.g., "data://grapher/who/2024/gho")
        filters: Dict of {attribute: value} to filter by

    Returns:
        Items that match all filters
    """
    if not filters:
        return items

    result = []
    for item in items:
        path = StepPath.from_uri(item)
        if path is None:
            # Invalid path format - skip this item when filters are active
            continue

        if matches_filters(path, filters):
            result.append(item)

    return result
