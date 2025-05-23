from dataclasses import is_dataclass
from functools import lru_cache
from typing import Any, Dict, Optional, Union

import jinja2
import structlog

from .utils import remove_details_on_demand

log = structlog.get_logger()

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
def raise_helper(msg):
    raise Exception(msg)


jinja_env.globals["raise"] = raise_helper


def _uses_jinja(text: Optional[str]):
    """Check if a string uses Jinja templating."""
    if not text:
        return False
    return "<%" in text or "<<" in text


@lru_cache(maxsize=None)
def _cached_jinja_template(text: str) -> jinja2.environment.Template:
    return jinja_env.from_string(text)


def _expand_jinja_text(text: str, dim_dict: Dict[str, str], remove_dods: bool = False) -> Union[str, bool]:
    if not _uses_jinja(text):
        out = text
    else:
        try:
            # NOTE: we're stripping the result to avoid trailing newlines
            out = _cached_jinja_template(text).render(dim_dict).strip()
            # Convert strings to booleans. Getting boolean directly from Jinja is not possible
            if out in ("false", "False", "FALSE"):
                return False
            elif out in ("true", "True", "TRUE"):
                return True
        except jinja2.exceptions.TemplateSyntaxError as e:
            new_message = f"{e.message}\n\nDimensions:\n{dim_dict}\n\nTemplate:\n{text}\n"
            raise e.__class__(new_message, e.lineno, e.name, e.filename) from e
        except jinja2.exceptions.UndefinedError as e:
            new_message = f"{e.message}\n\nDimensions:\n{dim_dict}\n\nTemplate:\n{text}\n"
            raise e.__class__(new_message) from e

    if remove_dods:
        out = remove_details_on_demand(out)

    return out


def _expand_jinja(obj: Any, dim_dict: Dict[str, str], **kwargs) -> Any:
    """Expand Jinja in all metadata fields. This modifies the original object in place."""
    if obj is None:
        return None
    elif isinstance(obj, str):
        return _expand_jinja_text(obj, dim_dict, **kwargs)
    elif is_dataclass(obj):
        for k, v in obj.__dict__.items():
            setattr(obj, k, _expand_jinja(v, dim_dict, **kwargs))
        return obj
    elif isinstance(obj, list):
        return type(obj)([_expand_jinja(v, dim_dict, **kwargs) for v in obj])
    elif isinstance(obj, dict):
        return {k: _expand_jinja(v, dim_dict, **kwargs) for k, v in obj.items()}
    else:
        return obj
