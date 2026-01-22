#
#  scoring.py
#  Shared scoring utilities for browser ranking
#

import re
from typing import Callable, Dict, List, Optional, Tuple


def score_match_term(term: str, item: str) -> float:
    """Score how well a single search term matches an item.

    Scoring (higher is better):
    - Exact match of final segment (dataset name): 1.0
    - Term matches final segment exactly at boundary: 0.95
    - Term is substring of final segment: 0.85
    - Match at path boundary (after / or -): 0.7
    - Match anywhere: 0.5
    - No match: 0.0

    Args:
        term: A single search term (lowercase)
        item: The item to match against (lowercase)

    Returns:
        Match score between 0.0 and 1.0
    """
    term_lower = term.lower()
    item_lower = item.lower()

    if term_lower not in item_lower:
        return 0.0

    # Check matches in final segment (dataset name)
    # Final segment is after the last /
    final_segment = item_lower.rsplit("/", 1)[-1]

    if term_lower in final_segment:
        # Exact match of entire final segment
        if final_segment == term_lower:
            return 1.0

        # Term matches at boundary within final segment (e.g., "population" in "population_growth")
        if final_segment.startswith(term_lower + "_") or final_segment.startswith(term_lower + "-"):
            return 0.95

        # Term is suffix of final segment (e.g., "population" in "world_population")
        if final_segment.endswith("_" + term_lower) or final_segment.endswith("-" + term_lower):
            return 0.9

        # Term is substring in final segment
        return 0.85

    # Check for boundary match in path (after / or -)
    if f"/{term_lower}" in item_lower or f"-{term_lower}" in item_lower or item_lower.startswith(term_lower):
        return 0.7

    # Match anywhere
    return 0.5


def score_match(pattern: str, item: str) -> float:
    """Score how well a search pattern matches an item.

    For multi-term patterns, returns the average of per-term scores.

    Args:
        pattern: Search pattern (may contain multiple space-separated terms)
        item: The item to match against

    Returns:
        Match score between 0.0 and 1.0
    """
    if not pattern:
        return 0.0

    terms = pattern.split()
    if not terms:
        return 0.0

    scores = [score_match_term(term, item) for term in terms]
    return sum(scores) / len(scores)


def extract_version_from_uri(uri: str) -> Optional[str]:
    """Extract version from a step URI.

    Handles formats like:
    - data://grapher/namespace/2024-01-15/dataset
    - data-private://meadow/namespace/2024/dataset
    - grapher://grapher/namespace/latest/dataset

    Args:
        uri: A step URI

    Returns:
        Version string or None if not found
    """
    # Strip protocol prefix
    if "://" in uri:
        _, path = uri.split("://", 1)
    else:
        path = uri

    parts = path.split("/")

    # Format: channel/namespace/version/dataset[/...]
    # We expect at least 4 parts
    if len(parts) >= 4:
        # Version is the 3rd component (index 2)
        version = parts[2]
        # Validate it looks like a version (YYYY-MM-DD, YYYY, or "latest")
        if re.match(r"^(\d{4}-\d{2}-\d{2}|\d{4}|latest)$", version):
            return version

    return None


def extract_version_from_snapshot(snapshot_path: str) -> Optional[str]:
    """Extract version from a snapshot path.

    Handles formats like:
    - namespace/2024-01-15/short_name.ext

    Args:
        snapshot_path: A snapshot path (without snapshot:// prefix)

    Returns:
        Version string or None if not found
    """
    parts = snapshot_path.split("/")

    # Format: namespace/version/short_name
    if len(parts) >= 3:
        version = parts[1]
        # Validate it looks like a version
        if re.match(r"^(\d{4}-\d{2}-\d{2}|\d{4}|latest)$", version):
            return version

    return None


def create_ranker(
    popularity_data: Optional[Dict[str, float]] = None,
    slug_extractor: Optional[Callable[[str], Optional[str]]] = None,
    version_extractor: Callable[[str], Optional[str]] = extract_version_from_uri,
) -> Callable[[str, List[str]], List[str]]:
    """Create a ranker function for use with browse_items.

    Uses lexicographic sorting for deterministic, intuitive results:
    1. Primary: match quality (higher is better)
    2. Secondary: popularity (higher is better)
    3. Tertiary: version recency (newer is better)
    4. Final tiebreaker: alphabetical (shorter paths first)

    This ensures:
    - Better matches always beat worse matches
    - Popular datasets beat unpopular ones (when match is equal)
    - Version only matters as a tiebreaker

    Args:
        popularity_data: Dict mapping slugs to popularity (0.0-1.0)
        slug_extractor: Function to extract slug from URI for popularity lookup
        version_extractor: Function to extract version from URI

    Returns:
        A ranker function that takes (pattern, matches) and returns sorted matches
    """

    def rank_matches(pattern: str, matches: List[str]) -> List[str]:
        if not matches:
            return matches

        # Pre-compute pattern terms once (optimization)
        pattern_lower = pattern.lower()
        terms = pattern_lower.split()

        def score_match_fast(item: str) -> float:
            """Optimized match scoring inline."""
            item_lower = item.lower()
            final_segment = item_lower.rsplit("/", 1)[-1]

            total = 0.0
            for term in terms:
                if term not in item_lower:
                    continue  # Term doesn't match at all

                if term in final_segment:
                    if final_segment == term:
                        total += 1.0
                    elif final_segment.startswith(term + "_") or final_segment.startswith(term + "-"):
                        total += 0.95
                    elif final_segment.endswith("_" + term) or final_segment.endswith("-" + term):
                        total += 0.9
                    else:
                        total += 0.85
                elif f"/{term}" in item_lower or f"-{term}" in item_lower or item_lower.startswith(term):
                    total += 0.7
                else:
                    total += 0.5

            return total / len(terms) if terms else 0.0

        def sort_key(item: str) -> Tuple[float, float, int, int, str]:
            # Match score (negated for descending sort)
            match_score = -score_match_fast(item)

            # Popularity score (negated for descending sort)
            pop_score = 0.0
            if popularity_data and slug_extractor:
                slug = slug_extractor(item)
                if slug:
                    pop_score = -popularity_data.get(slug, 0.0)

            # Version as numeric value (negated for descending/newest-first sort)
            # "2024-07-15" -> -20240715, "2024" -> -20240000, "latest" -> -99999999
            version = version_extractor(item)
            if version is None:
                version_num = 0  # Unknown versions sort last
            elif version == "latest":
                version_num = -99999999  # "latest" sorts first (most negative)
            else:
                # Convert "YYYY-MM-DD" or "YYYY" to negative integer
                try:
                    digits = version.replace("-", "")
                    # Pad year-only versions: "2024" -> "20240000"
                    if len(digits) == 4:
                        digits += "0000"
                    version_num = -int(digits)
                except ValueError:
                    version_num = 0

            # Length (shorter paths first)
            length = len(item)

            return (match_score, pop_score, version_num, length, item)

        return sorted(matches, key=sort_key)

    return rank_matches
