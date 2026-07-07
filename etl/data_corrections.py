#
#  data_corrections.py
#  etl
#
"""Declarative corrections for known upstream data errors.

A `.corrections.yml` file sits next to a step (exactly like `.excluded_countries.json`) and records
known errors in the *source* data together with the local fix we apply until the provider fixes them.
One entry per error. This replaces scattered inline `.loc[...]` / `.drop(...)` exceptions with a single
auditable record that carries the reason, provider and reporting status.

The mechanism is channel-agnostic (it works wherever `PathFinder.apply_corrections` is called), but
**garden is the default home**: a correction is a judgment about the data, its `entity`/`year` locators
are only canonical after garden harmonization, and some corrections target derived/rescaled columns that
don't exist upstream. Keep meadow/snapshot a faithful mirror of the provider — reserve them for raw
transcription typos that are wrong before any OWID processing and shared across multiple consumers.

Format (a list of entries)::

    - indicator: consumption_emissions          # the COLUMN to correct
      entity: Panama                            # country/entity
      years: [2006, 2008, 2016]                 # list | all | latest | {after/before/from/to: Y}
      action: drop                              # drop | override | scale | flag
      reason: Negative consumption-based CO2 (physically impossible) in source data.
      provider: Global Carbon Project
      reported: 2024-11-13                       # optional — when we told the provider
      status: reported                           # open | reported | acknowledged | fixed_upstream

Rows can also be located by their current value instead of entity+years::

    - indicator: drinks
      match: {value: 4.5}                        # match by current value of the indicator column
      action: override
      value: 5
      ...

A `match` value can also reference another column of the *same row* instead of a literal, to express
a relationship between two columns rather than a fixed constant::

    - indicator: barn
      match: {country: Belgium, year: 2025, value: {column: free_range}}
      action: drop                                # drop the row where barn == free_range
      ...

Actions:
- ``drop``     — remove the matched rows (use for long/tidy tables, where a row *is* one data point).
- ``override`` — replace the indicator value of the matched rows with ``value``. Set ``value: null``
                 to blank a single bad cell in a wide table without dropping the whole row.
- ``scale``    — multiply the indicator value of the matched rows by ``factor`` (e.g. ``factor: 0.001``
                 to fix a value reported in units while the rest are in thousands).
- ``flag``     — no data change; logs that the (uncorrected) error is known.

Guard against silently re-applying a correction after the source fixes it (optional ``expect``)::

    - indicator: ..._guests
      entity: Taiwan
      years: all
      action: scale
      factor: 0.001
      expect: {gt: 1000000}                      # matched values must still be anomalously large
      ...

``expect`` is a mapping of comparison operator (``eq``/``ne``/``gt``/``ge``/``lt``/``le``) to a numeric
threshold; every operator must hold for *all* matched rows or the step fails loudly. ``drop`` is already
self-validating (the row vanishing trips the no-match assertion), but ``override``/``scale`` keep the row,
so ``expect`` is how they detect an upstream fix. It cannot be combined with ``flag``.

There is intentionally no ``id`` field — a stable identifier is derived from the entry's target
(provider / indicator / entity+years or match) for logs and error messages.

This module is the mechanism only. The cross-dataset dashboard, per-provider report generator and
auto-expiry on re-ingestion are deliberately out of scope here.
"""

import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import yaml
from owid.catalog import Table
from structlog import get_logger

from etl.paths import DATA_DIR, STEP_DIR

log = get_logger()

# Default names of the columns (or index levels) used to locate rows by entity and year.
DEFAULT_COUNTRY_COL = "country"
DEFAULT_YEAR_COL = "year"

VALID_ACTIONS = {"drop", "override", "scale", "flag"}
VALID_STATUSES = {"open", "reported", "acknowledged", "fixed_upstream"}

# Comparison operators allowed in an `expect:` guard, mapping to the function applied elementwise.
#
# `expect`'s threshold is deliberately literal-only (unlike `match`, which also accepts a
# `{column: <name>}` reference — see `_resolve`). No correction needs a cross-column `expect` yet, and
# wiring it up isn't free: `_resolve` returns a table-length array, but `_check_expectation` compares
# against `tb.loc[mask, indicator]` (already mask-narrowed), so the resolved array would need slicing
# by the same `mask` before the elementwise comparison — an extra step with no test coverage today. If
# this is ever needed: resolve the threshold via `_resolve(tb, threshold, index_names, label)`, then
# index it with `mask` before passing it to the operator lambda.
EXPECT_OPERATORS = {
    "eq": lambda values, threshold: values == threshold,
    "ne": lambda values, threshold: values != threshold,
    "gt": lambda values, threshold: values > threshold,
    "ge": lambda values, threshold: values >= threshold,
    "lt": lambda values, threshold: values < threshold,
    "le": lambda values, threshold: values <= threshold,
}


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

    return corrections


def _label(correction: dict[str, Any]) -> str:
    """A short human-readable identifier for a correction, used in logs and error messages.

    There is no `id` field to invent — we derive a label from what the correction targets.
    """
    target = (
        f"{correction.get('entity')} {correction.get('years')}"
        if "entity" in correction
        else f"match {correction.get('match')}"
    )
    return f"{correction.get('provider', '?')} / {correction.get('indicator', '?')} / {target}"


def _validate_correction(correction: Any, path: Path | str) -> None:
    """Fail loudly if a correction entry is malformed."""
    if not isinstance(correction, dict):
        raise ValueError(f"Each correction in {path} must be a mapping, got {type(correction).__name__}.")

    label = _label(correction)

    for field in ("indicator", "action", "reason", "provider", "status"):
        assert correction.get(field) not in (None, ""), f"Correction [{label}] in {path} is missing required '{field}'."

    action = correction["action"]
    assert action in VALID_ACTIONS, (
        f"Correction [{label}]: invalid action '{action}' (expected one of {VALID_ACTIONS})."
    )
    assert correction["status"] in VALID_STATUSES, (
        f"Correction [{label}]: invalid status '{correction['status']}' (expected one of {VALID_STATUSES})."
    )

    has_entity_years = "entity" in correction and "years" in correction
    has_match = "match" in correction
    assert has_entity_years ^ has_match, f"Correction [{label}]: specify exactly one of (entity + years) or match."

    if has_match:
        match = correction["match"]
        # An empty (or non-dict) match would build an all-true mask and silently apply the correction to
        # *every* row — wiping a table on drop, or overwriting a whole indicator on override. Reject it.
        assert isinstance(match, dict) and len(match) > 0, (
            f"Correction [{label}]: 'match' must be a non-empty mapping of column → value."
        )
        # `entity`/`years` only apply to the entity+years form; mixing them with `match` is a mistake
        # (they would be silently ignored), so reject the combination.
        mixed = {"entity", "years"} & set(correction)
        assert not mixed, f"Correction [{label}]: do not combine 'match' with {sorted(mixed)}."
        # A match value may reference another column of the same row (`{column: <name>}`) instead of a
        # literal. Reject any other dict shape now, rather than letting it silently build an all-false
        # mask (a literal `==` against a dict never matches) that only surfaces as a confusing
        # "matched no rows" at apply time.
        for key, value in match.items():
            if isinstance(value, dict):
                assert set(value) == {"column"} and isinstance(value.get("column"), str) and value["column"], (
                    f"Correction [{label}]: match.{key} must be a literal or {{'column': <name>}}, got {value!r}."
                )

    if action == "override":
        assert "value" in correction, f"Correction [{label}]: action 'override' requires a 'value'."
        # `{column: ...}` references are only resolved inside `match` (to compare two columns of the
        # same row); resolving one as the top-level override target isn't implemented, so a value
        # shaped that way would be assigned into every matched cell as a literal dict — reject it.
        value = correction["value"]
        assert not (isinstance(value, dict) and "column" in value), (
            f"Correction [{label}]: action 'override' does not support a {{'column': ...}} reference as "
            f"'value' — that form is only resolved inside 'match'."
        )

    if action == "scale":
        factor = correction.get("factor")
        assert isinstance(factor, (int, float)) and not isinstance(factor, bool), (
            f"Correction [{label}]: action 'scale' requires a numeric 'factor'."
        )

    if "expect" in correction:
        # `expect` only guards a data-changing action — there is nothing to guard for 'flag'.
        assert action != "flag", f"Correction [{label}]: 'expect' cannot be combined with action 'flag'."
        expect = correction["expect"]
        assert isinstance(expect, dict) and len(expect) > 0, (
            f"Correction [{label}]: 'expect' must be a non-empty mapping of operator → threshold."
        )
        unknown_ops = set(expect) - set(EXPECT_OPERATORS)
        assert not unknown_ops, (
            f"Correction [{label}]: unknown 'expect' operators {sorted(unknown_ops)} "
            f"(expected a subset of {sorted(EXPECT_OPERATORS)})."
        )
        for op, threshold in expect.items():
            assert isinstance(threshold, (int, float)) and not isinstance(threshold, bool), (
                f"Correction [{label}]: 'expect.{op}' threshold must be numeric, got {threshold!r}."
            )


def _column_values(tb: Table, col: str, index_names: list[str]) -> np.ndarray:
    """Return the values of `col` whether it lives in the table's columns or its index."""
    if col in tb.columns:
        return tb[col].to_numpy()
    if col in index_names:
        return tb.index.get_level_values(col).to_numpy()
    raise KeyError(f"Column '{col}' not found in table columns {list(tb.columns)} or index {index_names}.")


def _year_mask(years: Any, year_values: np.ndarray, label: str) -> np.ndarray:
    """Build a boolean mask over rows selected by the `years` grammar.

    Supports: a literal list of years, the keyword `all` (every year for the entity), the keyword
    `latest` (the max year present in the table, i.e. the dataset's most recent year), and range dicts
    using `after` / `before` / `from` / `to`.
    """
    if years == "all":
        return np.ones(len(year_values), dtype=bool)
    if years == "latest":
        assert len(year_values) > 0, f"Correction [{label}]: table has no rows, cannot resolve 'latest' year."
        return year_values == year_values.max()
    if isinstance(years, list):
        return np.isin(year_values, years)
    if isinstance(years, dict):
        mask = np.ones(len(year_values), dtype=bool)
        unknown = set(years) - {"after", "before", "from", "to"}
        assert not unknown, f"Correction [{label}]: unknown year-range keys {sorted(unknown)}."
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
        f"Correction [{label}]: unsupported 'years' value {years!r} (expected list, 'all', 'latest' or a range dict)."
    )


def _eq_mask(values: np.ndarray, target: Any) -> np.ndarray:
    """Elementwise equality that treats missing values as non-matching rather than raising.

    `values` may be an object array holding `pd.NA`/`None` (e.g. a nullable string column) — comparing
    those to a scalar with plain `==` yields `pd.NA` instead of `False`, which blows up a later `&=`
    with a `TypeError: boolean value of NA is ambiguous`. `target` may itself be an array (see
    `_resolve`), in which case this compares elementwise position-by-position, same as against a scalar.
    """
    return pd.Series(values).eq(target).fillna(False).to_numpy()


def _resolve(tb: Table, spec: Any, index_names: list[str], label: str) -> Any:
    """Resolve a `match` operand: `{"column": <name>}` resolves to that column's full array, so the
    caller ends up comparing two columns of the same row elementwise instead of a column against a
    fixed constant. Anything else is returned unchanged (the literal case).
    """
    if isinstance(spec, dict):
        assert "column" in spec, (
            f"Correction [{label}]: expected a column reference like {{'column': <name>}}, got {spec!r}."
        )
        return _column_values(tb, spec["column"], index_names)
    return spec


def _row_mask(tb: Table, correction: dict[str, Any], country_col: str, year_col: str) -> np.ndarray:
    """Return a positional boolean mask over `tb` selecting the rows a correction targets."""
    index_names = [name for name in tb.index.names if name is not None]
    label = _label(correction)

    if "match" in correction:
        mask = np.ones(len(tb), dtype=bool)
        for key, value in correction["match"].items():
            # `value` is shorthand for the indicator column; any other key matches that column by name.
            col = correction["indicator"] if key == "value" else key
            target = _resolve(tb, value, index_names, label)
            mask &= _eq_mask(_column_values(tb, col, index_names), target)
        return mask

    entity_mask = _eq_mask(_column_values(tb, country_col, index_names), correction["entity"])
    year_values = _column_values(tb, year_col, index_names)
    return entity_mask & _year_mask(correction["years"], year_values, label)


def _check_expectation(tb: Table, correction: dict[str, Any], mask: np.ndarray, label: str) -> None:
    """Assert the matched rows still satisfy the `expect` predicate (i.e. the anomaly is still present).

    This is the "raise if fixed upstream" guard: every operator in `expect` must hold for *all* matched
    values of the indicator column. If any fails, the upstream error was likely corrected and the
    correction should be removed.
    """
    values = tb.loc[mask, correction["indicator"]]
    for op, threshold in correction["expect"].items():
        satisfied = EXPECT_OPERATORS[op](values, threshold)
        assert satisfied.all(), (
            f"Correction [{label}]: expectation '{op} {threshold}' failed for {(~satisfied).sum()} of "
            f"{len(values)} matched rows (values: {sorted(set(values.tolist()))[:5]}). The upstream error "
            f"may have been fixed — remove or update this correction."
        )


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
        label = _label(correction)
        action = correction["action"]
        mask = _row_mask(tb, correction, country_col, year_col)
        n_matched = int(mask.sum())

        if action == "flag":
            log.info("data_corrections.flag", correction=label, matched=n_matched, reason=correction["reason"])
            continue

        # A correction that no longer matches anything is a signal: the upstream error was likely fixed,
        # or the source schema changed. Fail loudly rather than silently doing nothing.
        assert n_matched > 0, (
            f"Correction [{label}] matched no rows. The upstream error may have been fixed — "
            f"remove or update this correction."
        )

        # `drop` is self-validating (the row vanishing trips the assert above), but `override`/`scale`
        # keep the row, so a fixed-upstream value would be silently re-mangled. An optional `expect`
        # guard asserts the matched rows are *still* anomalous before we touch them.
        if "expect" in correction:
            _check_expectation(tb, correction, mask, label)

        if action == "drop":
            tb = tb[~mask]
            dropped_any = True
        elif action == "override":
            tb.loc[mask, correction["indicator"]] = correction["value"]
        elif action == "scale":
            tb.loc[mask, correction["indicator"]] *= correction["factor"]

        log.info("data_corrections.apply", correction=label, action=action, matched=n_matched)

    if dropped_any and entity_year_in_columns:
        tb = tb.reset_index(drop=True)

    return tb


# --------------------------------------------------------------------------------------------------
# Audit: capture the before/after of each correction so `etl corrections --charts` can show the
# *problematic* values (which are gone from the published data once the correction is applied).
# --------------------------------------------------------------------------------------------------


def _to_float(value: Any) -> float | None:
    """Coerce a value to float, or None if it's missing/non-numeric (e.g. a categorical entity)."""
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    return None if f != f else f  # drop NaN


def _to_int(value: Any) -> int | None:
    """Coerce a value to int, or None if it's not a whole-number label.

    Some tables carry non-integer year labels for rows unrelated to a given correction (e.g. a
    reporting-period range like "1872-6") — those points simply can't be placed on the audit's
    year axis, so they're dropped rather than raising.
    """
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def audit_path_for(corrections_path: Path | str) -> Path:
    """Where the audit JSON for a given `.corrections.yml` lives (under the gitignored data/ tree)."""
    rel = Path(corrections_path).resolve().relative_to(STEP_DIR.resolve())  # data/<channel>/.../x.corrections.yml
    parts = rel.parts[1:] if rel.parts and rel.parts[0] == "data" else rel.parts
    name = parts[-1].replace(".corrections.yml", ".audit.json")
    return DATA_DIR / "corrections_audit" / Path(*parts[:-1]) / name


def build_audit(
    tb: Table,
    corrections: list[dict[str, Any]],
    *,
    country_col: str = DEFAULT_COUNTRY_COL,
    year_col: str = DEFAULT_YEAR_COL,
) -> list[dict[str, Any]]:
    """Record each correction's affected entities: their full *pre-correction* series and the
    before/after of every affected point. Computed on `tb` before any correction is applied.
    """
    index_names = [name for name in tb.index.names if name is not None]
    records = []
    for correction in corrections:
        indicator = correction["indicator"]
        try:
            ind_vals = _column_values(tb, indicator, index_names)
            entity_vals = _column_values(tb, country_col, index_names).astype(str)
            year_vals = _column_values(tb, year_col, index_names)
        except KeyError:
            continue  # can't audit if the locator columns aren't present at this stage
        mask = _row_mask(tb, correction, country_col, year_col)
        action = correction["action"]
        factor = correction.get("factor")
        # `correction.get("value")` here is the top-level override target, not a `match`-nested
        # `{column: ...}` reference (those live inside `match["value"]`, a different dict) — the two
        # `"value"` keys never collide, so `_to_float` seeing a real reference dict can't happen for a
        # correction that's valid per `_validate_correction`.
        override_value = _to_float(correction.get("value"))

        any_numeric = False
        entities = []
        for ent in sorted(set(entity_vals[mask].tolist())):
            ent_mask = entity_vals == ent
            series = sorted(
                [y_int, fv]
                for y, v in zip(year_vals[ent_mask].tolist(), ind_vals[ent_mask].tolist())
                if (fv := _to_float(v)) is not None and (y_int := _to_int(y)) is not None
            )
            any_numeric = any_numeric or bool(series)
            affected = []
            for y, v in sorted(
                zip(year_vals[(ent_mask & mask)].tolist(), ind_vals[(ent_mask & mask)].tolist()),
                key=lambda pair: str(pair[0]),
            ):
                before = _to_float(v)
                y_int = _to_int(y)
                if before is None or y_int is None:
                    continue  # a matched-but-empty cell, or a non-integer year label, carries nothing to plot
                if action == "drop":
                    after = None
                elif action == "override":
                    after = override_value
                elif action == "scale":
                    after = before * float(factor) if before is not None else None
                else:
                    after = before
                affected.append([y_int, before, after])
            entities.append({"entity": ent, "series": series, "affected": affected})

        records.append(
            {
                "label": _label(correction),
                "indicator": indicator,
                "action": action,
                "reason": correction.get("reason"),
                "numeric": any_numeric,
                "entities": entities,
            }
        )
    return records


def write_audit(corrections_path: Path | str, records: list[dict[str, Any]]) -> None:
    """Write the audit JSON for a corrections file (best-effort; never raises)."""
    try:
        path = audit_path_for(corrections_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(records))
    except (OSError, TypeError, ValueError) as e:
        log.warning("data_corrections.audit_write_failed", path=str(corrections_path), error=str(e))


def read_audit(corrections_path: Path | str) -> list[dict[str, Any]] | None:
    """Read the audit JSON for a corrections file, or None if it hasn't been generated yet."""
    path = audit_path_for(corrections_path)
    if not path.exists():
        return None
    return json.loads(path.read_text())
