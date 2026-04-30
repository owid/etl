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

# Sentinel returned by `_expand_jinja_text` when a Jinja template renders to
# an empty string. The dict/list walker in `_expand_jinja` drops keys/items
# carrying this sentinel, which lets metadata authors conditionally suppress
# fields via `<% if … %>…<% endif %>` (no else branch needed).
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
            # Treat empty Jinja output as "remove the key/item" so authors can
            # write `<% if cond %>value<% endif %>` without an else branch.
            if out == "":
                return _REMOVE_KEY
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
            # Treat REMOVE_KEY as "reset to None" — dataclass fields are
            # declared at class level, so we can't `del` the attribute, but
            # None is the standard "absent" value across our metadata schemas.
            setattr(obj, k, None if new_v is _REMOVE_KEY else new_v)
        return obj
    elif isinstance(obj, list):
        # Drop REMOVE_KEY items, plus dicts that became empty after expansion
        # (e.g. a comparisonLines entry whose label and yEquals were both
        # conditionally suppressed). Without this, we'd ship `[{}]` and break
        # downstream schema validation.
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
