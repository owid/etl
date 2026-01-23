#
#  filters.py
#  Filter parsing and matching for browser UI
#

import re
from dataclasses import dataclass, field


@dataclass
class StepPath:
    """Minimal step path parser for filter matching.

    Parses step URIs like "data://garden/who/2024-01-15/gho" into components.
    This is a lightweight alternative to CatalogPath that avoids heavy imports.
    """

    channel: str | None = None
    namespace: str | None = None
    version: str | None = None
    dataset: str | None = None

    @classmethod
    def from_uri(cls, uri: str) -> "StepPath | None":
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

    filters: dict[str, str] = field(default_factory=dict)  # {attribute: value}
    search_terms: list[str] = field(default_factory=list)  # remaining search terms
    filter_spans: list[tuple[int, int]] = field(default_factory=list)  # (start, end) for highlighting


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
    filters: dict[str, str] = {}
    filter_spans: list[tuple[int, int]] = []

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


def matches_filters(path: StepPath, filters: dict[str, str]) -> bool:
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


def get_step_segment_positions(item: str) -> dict[str, tuple[int, int]]:
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

    segments: dict[str, tuple[int, int]] = {}
    pos = protocol_end

    segment_names = ["channel", "namespace", "version", "dataset"]
    for i, name in enumerate(segment_names):
        if i < len(parts):
            start = pos
            end = pos + len(parts[i])
            segments[name] = (start, end)
            pos = end + 1  # +1 for the slash

    return segments


def find_filter_match_spans(item: str, filters: dict[str, str]) -> list[tuple[int, int]]:
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

    spans: list[tuple[int, int]] = []

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


def apply_filters(items: list[str], filters: dict[str, str]) -> list[str]:
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


@dataclass
class FilterOptions:
    """Cached unique values for each filterable attribute."""

    channels: list[str] = field(default_factory=list)
    namespaces: list[str] = field(default_factory=list)
    versions: list[str] = field(default_factory=list)
    datasets: list[str] = field(default_factory=list)

    def get(self, attr: str) -> list[str]:
        """Get options for a given attribute name."""
        mapping = {
            "channel": self.channels,
            "namespace": self.namespaces,
            "version": self.versions,
            "dataset": self.datasets,
        }
        return mapping.get(attr, [])


def extract_filter_options(items: list[str]) -> FilterOptions:
    """Extract unique values for each filterable attribute from items.

    Args:
        items: List of step URIs

    Returns:
        FilterOptions with sorted unique values for each attribute.
    """
    channels: dict[str, int] = {}
    namespaces: dict[str, int] = {}
    versions: dict[str, int] = {}
    datasets: dict[str, int] = {}

    for item in items:
        path = StepPath.from_uri(item)
        if path is None:
            continue

        if path.channel:
            channels[path.channel] = channels.get(path.channel, 0) + 1
        if path.namespace:
            namespaces[path.namespace] = namespaces.get(path.namespace, 0) + 1
        if path.version:
            versions[path.version] = versions.get(path.version, 0) + 1
        if path.dataset:
            datasets[path.dataset] = datasets.get(path.dataset, 0) + 1

    # Sort by frequency (most common first), then alphabetically
    def sort_by_freq(d: dict[str, int]) -> list[str]:
        return [k for k, _ in sorted(d.items(), key=lambda x: (-x[1], x[0]))]

    return FilterOptions(
        channels=sort_by_freq(channels),
        namespaces=sort_by_freq(namespaces),
        versions=sort_by_freq(versions),
        datasets=sort_by_freq(datasets),
    )


@dataclass
class ActiveFilter:
    """Information about the filter currently being typed."""

    attr: str  # e.g., "namespace"
    prefix: str  # e.g., "n"
    value: str  # e.g., "wh" (partial value typed so far)
    start: int  # position in input string
    end: int  # position in input string


def get_active_filter(text: str) -> ActiveFilter | None:
    """Detect if user is currently typing a filter value.

    Returns information about the active filter if the cursor is at the end
    of a filter token (e.g., "n:" or "n:wh").
    """
    # Check if text ends with a filter pattern
    # We want to detect: "n:", "n:wh", "namespace:who", etc.
    match = re.search(r"\b(n|namespace|c|channel|v|version|d|dataset):(\S*)$", text, re.IGNORECASE)
    if not match:
        return None

    prefix = match.group(1).lower()
    value = match.group(2)
    attr = FILTER_PREFIXES[prefix]

    return ActiveFilter(
        attr=attr,
        prefix=prefix,
        value=value,
        start=match.start(),
        end=match.end(),
    )
