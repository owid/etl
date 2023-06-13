import re
from typing import List, Literal, Optional, overload

import numpy as np
import pandas as pd
from unidecode import unidecode

from .tables import Table
from .variables import Variable


@overload
def underscore(name: str, validate: bool = True) -> str:
    ...


@overload
def underscore(name: None, validate: bool = True) -> None:
    ...


def underscore(name: Optional[str], validate: bool = True) -> Optional[str]:
    """Convert arbitrary string to under_score. This was fine tuned on WDI bank column names.
    This function might evolve in the future, so make sure to have your use cases in tests
    or rather underscore your columns yourself.
    """
    if name is None:
        return None

    orig_name = name

    name = (
        name.replace(" ", "_")
        .replace("-", "_")
        .replace("—", "_")
        .replace("–", "_")
        .replace(",", "_")
        .replace(".", "_")
        .replace("\t", "_")
        .replace("?", "_")
        .replace('"', "")
        .replace("‘", "")
        .replace("\xa0", "_")
        .replace("’", "")
        .replace("`", "")
        .replace("−", "_")
        .replace("*", "_")
        .replace("“", "")
        .replace("”", "")
        .replace("#", "")
        .replace("^", "")
        .lower()
    )

    # replace special separators
    name = (
        name.replace("(", "__")
        .replace(")", "__")
        .replace(":", "__")
        .replace(";", "__")
        .replace("[", "__")
        .replace("]", "__")
    )

    # replace special symbols
    name = name.replace("/", "_")
    name = name.replace("|", "_")
    name = name.replace("=", "_")
    name = name.replace("%", "pct")
    name = name.replace("+", "plus")
    name = name.replace("us$", "usd")
    name = name.replace("$", "dollar")
    name = name.replace("&", "_and_")
    name = name.replace("<", "_lt_")
    name = name.replace(">", "_gt_")
    name = name.replace("≥", "_gte_")
    name = name.replace("≤", "_lte_")

    # replace quotes
    name = name.replace("'", "")

    # shrink triple underscore
    name = re.sub("__+", "__", name)

    # convert special characters to ASCII
    name = unidecode(name).lower()

    # strip leading and trailing underscores
    name = name.strip("_")

    # if the first letter is number, prefix it with underscore
    if re.match("^[0-9]", name):
        name = f"_{name}"

    # make sure it's under_score now, if not then raise NameError
    if validate:
        validate_underscore(name, f"`{orig_name}`")

    return name


def _resolve_collisions(
    orig_cols: pd.Index,
    new_cols: pd.Index,
    collision: Literal["raise", "rename", "ignore"],
) -> pd.Index:
    new_cols = new_cols.copy()
    vc = new_cols.value_counts()

    colliding_cols = list(vc[vc >= 2].index)
    for colliding_col in colliding_cols:
        ixs = np.where(new_cols == colliding_col)[0]
        if collision == "raise":
            raise NameError(
                f"Columns `{orig_cols[ixs[0]]}` and `{orig_cols[ixs[1]]}` are given the same name "
                f"`{colliding_cols[0]}` after underscoring`"
            )
        elif collision == "rename":
            # give each column numbered suffix
            for i, ix in enumerate(ixs):
                new_cols.values[ix] = f"{new_cols[ix]}_{i + 1}"
        elif collision == "ignore":
            pass
        else:
            raise NotImplementedError()
    return new_cols


def underscore_table(
    t: Table,
    collision: Literal["raise", "rename", "ignore"] = "raise",
    inplace: bool = False,
) -> Table:
    """Convert column and index names to underscore. In extremely rare cases
    two columns might have the same underscored version. Use `collision` param
    to control whether to raise an error or append numbered suffix."""
    orig_cols = t.columns

    # underscore columns and resolve collisions
    new_cols = pd.Index([underscore(c) for c in t.columns])
    new_cols = _resolve_collisions(orig_cols, new_cols, collision)

    columns_map = {c_old: c_new for c_old, c_new in zip(orig_cols, new_cols)}
    if inplace:
        t.rename(columns=columns_map, inplace=True)
    else:
        t = t.rename(columns=columns_map)

    t.index.names = [underscore(e) for e in t.index.names]
    t.metadata.primary_key = t.primary_key
    t.metadata.short_name = underscore(t.metadata.short_name)

    # put original names as titles into metadata by default
    for c_old, c_new in columns_map.items():
        if t[c_new].metadata.title is None:
            t[c_new].metadata.title = c_old

    return t


def validate_underscore(name: Optional[str], object_name: str = "Name") -> None:
    """Raise error if name is not snake_case."""
    if name is not None and not re.match("^[a-z_][a-z0-9_]*$", name):
        raise NameError(f"{object_name} must be snake_case. Change `{name}` to `{underscore(name, validate=False)}`")


def concat_variables(variables: List[Variable]) -> Table:
    """Concatenate variables into a single table keeping all metadata."""
    t = Table(pd.concat(variables, axis=1))
    for v in variables:
        if v.name:
            t._fields[v.name] = v.metadata
    return t
