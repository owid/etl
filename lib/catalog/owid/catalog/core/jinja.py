from dataclasses import is_dataclass
from functools import cache
from typing import Any

import jinja2

from owid.catalog.core.utils import remove_details_on_demand

# Sentinel marking a Jinja-templated value that should be coerced to its
# native Python type (int, float) after rendering. Without this, Jinja always
# returns strings — which makes templated numeric fields (e.g. `yAxis.min`,
# `comparisonLines[].yEquals`) fail strict-typed schema validation. See the
# `as_value` Jinja filter below.
_AS_VALUE_MARKER = "__OWID_AS_VALUE__:"

# Sentinel returned by `_expand_jinja_text` when a Jinja template that uses
# the `| as_value` filter renders to an empty string. The dict/list walker in
# `_expand_jinja` drops keys/items carrying this sentinel, so authors can
# conditionally suppress strict-typed fields (e.g. `yAxis.min`,
# `comparisonLines[].yEquals`) without an `<% else %>` branch. Untyped fields
# (subtitle, note, description_*, etc.) preserve "" instead — see
# `_expand_jinja_text` for the split.
_REMOVE_KEY = object()

jinja_env = jinja2.Environment(
    block_start_string="<%",
    block_end_string="%>",
    variable_start_string="<<",
    variable_end_string=">>",
    comment_start_string="<#",
    comment_end_string="#>",
    trim_blocks=True,
    lstrip_blocks=True,
    undefined=jinja2.StrictUndefined,
)


# Helper function to raise an error with << raise("uh oh...") >>
def raise_helper(msg: str) -> None:
    raise Exception(msg)


def _as_value_filter(value: Any) -> str:
    """Mark a Jinja-templated value for type coercion at expansion time.

    Use on numeric fields where the schema requires a number, e.g.

        min: "<% if age == '0' %><< 90 | as_value >><% endif %>"

    Without the filter, Jinja returns the literal string "90", which fails
    strict-typed schema validation. With the filter, `_expand_jinja_text`
    detects the sentinel after rendering and coerces back to int/float.
    """
    return f"{_AS_VALUE_MARKER}{value}"


jinja_env.globals["raise"] = raise_helper  # ty: ignore[invalid-assignment]
jinja_env.filters["as_value"] = _as_value_filter


def _uses_jinja(text: str | None):
    """Check if a string uses Jinja templating."""
    if not text:
        return False
    return "<%" in text or "<<" in text


@cache
def _cached_jinja_template(text: str) -> jinja2.environment.Template:
    return jinja_env.from_string(text)


def _coerce_marked_value(text: str) -> Any:
    """Strip the as_value sentinel and coerce to int/float; fall back to string."""
    raw = text[len(_AS_VALUE_MARKER) :]
    try:
        return int(raw)
    except ValueError:
        pass
    try:
        return float(raw)
    except ValueError:
        pass
    return raw


def _expand_jinja_text(text: str, dim_dict: dict[str, str], remove_dods: bool = False) -> Any:
    if not _uses_jinja(text):
        out = text
    else:
        try:
            # NOTE: we're stripping the result to avoid trailing newlines
            out = _cached_jinja_template(text).render(dim_dict).strip()
            # Empty Jinja output: preserve "" by default so authors can force
            # empty string fields like `subtitle: ""` or `note: ""` via Jinja
            # (Grapher reads these as "render no subtitle" rather than falling
            # back to `description_short`). Drop the key/item only when the
            # template opts in via `| as_value` — those are typed numeric
            # fields (e.g. `yAxis.min`, `comparisonLines[].yEquals`) where ""
            # would fail strict-typed schema validation.
            if out == "":
                if "as_value" in text:
                    return _REMOVE_KEY
                return ""
            # Convert strings to booleans. Getting boolean directly from Jinja is not possible
            if out in ("false", "False", "FALSE"):
                return False
            elif out in ("true", "True", "TRUE"):
                return True
            # Coerce values explicitly marked with the `| as_value` filter.
            elif out.startswith(_AS_VALUE_MARKER):
                return _coerce_marked_value(out)
        except jinja2.exceptions.TemplateSyntaxError as e:
            new_message = f"{e.message}\n\nDimensions:\n{dim_dict}\n\nTemplate:\n{text}\n"
            raise e.__class__(new_message, e.lineno, e.name, e.filename) from e
        except jinja2.exceptions.UndefinedError as e:
            new_message = f"{e.message}\n\nDimensions:\n{dim_dict}\n\nTemplate:\n{text}\n"
            raise e.__class__(new_message) from e

    if remove_dods:
        out = remove_details_on_demand(out)

    return out


def _expand_jinja(obj: Any, dim_dict: dict[str, str], **kwargs: Any) -> Any:
    """Expand Jinja in all metadata fields. This modifies the original object in place."""
    if obj is None:
        return None
    elif isinstance(obj, str):
        return _expand_jinja_text(obj, dim_dict, **kwargs)
    elif is_dataclass(obj):
        for k, v in obj.__dict__.items():
            new_v = _expand_jinja(v, dim_dict, **kwargs)
            # Defensive: a dataclass scalar that opted into drop via `| as_value`
            # (unusual on a dataclass field) still becomes "" rather than None,
            # so downstream code that treats `unit: None` as a hard failure
            # keeps working.
            if new_v is _REMOVE_KEY:
                new_v = ""
            setattr(obj, k, new_v)
        return obj
    elif isinstance(obj, list):
        # Drop REMOVE_KEY items and dicts that emptied out via `| as_value`.
        # The latter handles the rare `comparisonLines: [{label: <as_value>,
        # yEquals: <as_value>}]` pattern where every numeric key in the entry
        # opts into drop and ends up with `{}`, which would break schema
        # validation. List items that render to "" via plain Jinja are
        # preserved (pre-PR behavior — author can add an `<% else %>` branch
        # if an empty entry isn't desired).
        result = []
        for v in obj:
            new_v = _expand_jinja(v, dim_dict, **kwargs)
            if new_v is _REMOVE_KEY:
                continue
            if isinstance(new_v, dict) and not new_v:
                continue
            result.append(new_v)
        return type(obj)(result)
    elif isinstance(obj, dict):
        return {
            k: new_v
            for k, new_v in ((k, _expand_jinja(v, dim_dict, **kwargs)) for k, v in obj.items())
            if new_v is not _REMOVE_KEY
        }
    else:
        return obj
