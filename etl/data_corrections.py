#
#  data_corrections.py
#  etl
#
"""Declarative corrections for known upstream data errors.

A `.corrections.yml` file sits next to a step (exactly like `.excluded_countries.json`) and records
known errors in the *source* data together with the local fix we apply until the provider fixes them.
One entry per error. This replaces scattered inline `.loc[...]` / `.drop(...)` exceptions with a single
auditable record that carries the reason, provider and reporting status.

Format (a list of entries)::

    - id: gcp-panama-negative-consumption      # unique, kebab-case
      indicator: consumption_emissions          # the COLUMN to correct
      entity: Panama                            # country/entity
      years: [2006, 2008, 2016]                 # list | latest | {after/before/from/to: Y}
      action: drop                              # drop | override | flag
      reason: Negative consumption-based CO2 (physically impossible) in source data.
      provider: Global Carbon Project
      reported: 2024-11-13                       # optional — when we told the provider
      status: reported                           # open | reported | acknowledged | fixed_upstream

Rows can also be located by their current value instead of entity+years::

    - id: gfs-drinks-typo-4p5
      indicator: drinks
      match: {value: 4.5}                        # match by current value of the indicator column
      action: override
      value: 5
      ...

Actions:
- ``drop``     — remove the matched rows.
- ``override`` — replace the indicator value of the matched rows with ``value``.
- ``flag``     — no data change; logs that the (uncorrected) error is known.

This module is the mechanism only. The cross-dataset dashboard, per-provider report generator and
auto-expiry on re-ingestion are deliberately out of scope here.
"""

from pathlib import Path
from typing import Any

import numpy as np
import yaml
from owid.catalog import Table
from structlog import get_logger

log = get_logger()

# Default names of the columns (or index levels) used to locate rows by entity and year.
DEFAULT_COUNTRY_COL = "country"
DEFAULT_YEAR_COL = "year"

VALID_ACTIONS = {"drop", "override", "flag"}
VALID_STATUSES = {"open", "reported", "acknowledged", "fixed_upstream"}


def load_corrections(path: Path | str) -> list[dict[str, Any]]:
    """Load and validate the list of corrections from a `.corrections.yml` file."""
    with open(path) as istream:
        corrections = yaml.safe_load(istream)

    if corrections is None:
        return []

    if not isinstance(corrections, list):
        raise ValueError(f"Corrections file {path} must contain a list of entries, got {type(corrections).__name__}.")

    for correction in corrections:
        _validate_correction(correction, path)

    # Ensure ids are unique within the file.
    ids = [c["id"] for c in corrections]
    duplicated = sorted({i for i in ids if ids.count(i) > 1})
    assert not duplicated, f"Duplicate correction id(s) in {path}: {duplicated}"

    return corrections


def _validate_correction(correction: Any, path: Path | str) -> None:
    """Fail loudly if a correction entry is malformed."""
    if not isinstance(correction, dict):
        raise ValueError(f"Each correction in {path} must be a mapping, got {type(correction).__name__}.")

    cid = correction.get("id", "<missing id>")

    for field in ("id", "indicator", "action", "reason", "provider", "status"):
        assert correction.get(field) not in (None, ""), f"Correction '{cid}' in {path} is missing required '{field}'."

    action = correction["action"]
    assert action in VALID_ACTIONS, f"Correction '{cid}': invalid action '{action}' (expected one of {VALID_ACTIONS})."
    assert correction["status"] in VALID_STATUSES, (
        f"Correction '{cid}': invalid status '{correction['status']}' (expected one of {VALID_STATUSES})."
    )

    has_entity_years = "entity" in correction and "years" in correction
    has_match = "match" in correction
    assert has_entity_years ^ has_match, f"Correction '{cid}': specify exactly one of (entity + years) or match."

    if action == "override":
        assert "value" in correction, f"Correction '{cid}': action 'override' requires a 'value'."


def _column_values(tb: Table, col: str, index_names: list[str]) -> np.ndarray:
    """Return the values of `col` whether it lives in the table's columns or its index."""
    if col in tb.columns:
        return tb[col].to_numpy()
    if col in index_names:
        return tb.index.get_level_values(col).to_numpy()
    raise KeyError(f"Column '{col}' not found in table columns {list(tb.columns)} or index {index_names}.")


def _year_mask(years: Any, year_values: np.ndarray, entity_mask: np.ndarray, cid: str) -> np.ndarray:
    """Build a boolean mask over rows selected by the `years` grammar.

    Supports: a literal list of years, the keyword `latest` (the max year present among the entity's
    rows), and range dicts using `after` / `before` / `from` / `to`.
    """
    if years == "latest":
        present = year_values[entity_mask]
        assert len(present) > 0, f"Correction '{cid}': entity has no rows, cannot resolve 'latest' year."
        return year_values == present.max()
    if isinstance(years, list):
        return np.isin(year_values, years)
    if isinstance(years, dict):
        mask = np.ones(len(year_values), dtype=bool)
        unknown = set(years) - {"after", "before", "from", "to"}
        assert not unknown, f"Correction '{cid}': unknown year-range keys {sorted(unknown)}."
        if "after" in years:
            mask &= year_values > years["after"]
        if "before" in years:
            mask &= year_values < years["before"]
        if "from" in years:
            mask &= year_values >= years["from"]
        if "to" in years:
            mask &= year_values <= years["to"]
        return mask
    raise ValueError(
        f"Correction '{cid}': unsupported 'years' value {years!r} (expected list, 'latest' or a range dict)."
    )


def _row_mask(tb: Table, correction: dict[str, Any], country_col: str, year_col: str) -> np.ndarray:
    """Return a positional boolean mask over `tb` selecting the rows a correction targets."""
    index_names = [name for name in tb.index.names if name is not None]
    cid = correction["id"]

    if "match" in correction:
        mask = np.ones(len(tb), dtype=bool)
        for key, value in correction["match"].items():
            # `value` is shorthand for the indicator column; any other key matches that column by name.
            col = correction["indicator"] if key == "value" else key
            mask &= _column_values(tb, col, index_names) == value
        return mask

    entity_mask = _column_values(tb, country_col, index_names) == correction["entity"]
    year_values = _column_values(tb, year_col, index_names)
    return entity_mask & _year_mask(correction["years"], year_values, entity_mask, cid)


def apply_corrections(
    tb: Table,
    corrections: list[dict[str, Any]],
    *,
    country_col: str = DEFAULT_COUNTRY_COL,
    year_col: str = DEFAULT_YEAR_COL,
) -> Table:
    """Apply a list of corrections to a table and return the corrected copy.

    `country_col` / `year_col` name the entity and year fields, which may be either columns or index
    levels of `tb`. The original table is not mutated.
    """
    tb = tb.copy()

    # If entity/year are plain columns, the table carries an ordinary RangeIndex; after dropping rows we
    # renumber it so the result matches the long-standing `df.drop(...).reset_index(drop=True)` idiom.
    entity_year_in_columns = country_col in tb.columns and year_col in tb.columns
    dropped_any = False

    for correction in corrections:
        cid = correction["id"]
        action = correction["action"]
        mask = _row_mask(tb, correction, country_col, year_col)
        n_matched = int(mask.sum())

        if action == "flag":
            log.info("data_corrections.flag", id=cid, matched=n_matched, reason=correction["reason"])
            continue

        # A correction that no longer matches anything is a signal: the upstream error was likely fixed,
        # or the source schema changed. Fail loudly rather than silently doing nothing.
        assert n_matched > 0, (
            f"Correction '{cid}' matched no rows. The upstream error may have been fixed — "
            f"remove or update this correction."
        )

        if action == "drop":
            tb = tb[~mask]
            dropped_any = True
        elif action == "override":
            tb.loc[mask, correction["indicator"]] = correction["value"]

        log.info("data_corrections.apply", id=cid, action=action, matched=n_matched)

    if dropped_any and entity_year_in_columns:
        tb = tb.reset_index(drop=True)

    return tb
