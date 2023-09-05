import os
import random
import tempfile
import webbrowser
from contextlib import contextmanager
from dataclasses import dataclass
from functools import wraps
from typing import Any, Dict, List, Literal, Optional, Tuple

from dataclasses_json import dataclass_json

from .utils import pruned_json


def enabled() -> bool:
    """Is processing log enabled? The env can be changing during the execution of the program."""
    # Environment variable such that, if True, the processing log will be updated, if False, the log will always be empty.
    # If not defined, assume False.
    return os.getenv("PROCESSING_LOG", "") in ("True", "true", "1")


@contextmanager
def disable_processing_log():
    original_value = os.environ.get("PROCESSING_LOG", "0")
    os.environ["PROCESSING_LOG"] = "0"
    try:
        yield
    finally:
        os.environ["PROCESSING_LOG"] = original_value


@pruned_json
@dataclass_json
@dataclass(frozen=True)
class LogEntry:
    variable: str
    parents: Tuple[str, ...]
    operation: str
    target: str
    comment: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        ...

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "LogEntry":
        ...

    def clone(self, **kwargs):
        """Clone the log entry, optionally overriding some attributes."""
        d = self.to_dict()
        d.update(**kwargs)
        return LogEntry.from_dict(d)


class ProcessingLog(List[LogEntry]):
    # hack for dataclasses_json
    __args__ = (LogEntry,)

    # NOTE: calling this method `as_dict` is intentional, otherwise it gets called
    # by dataclass_json
    def as_dict(self) -> List[Dict[str, Any]]:
        return [r.to_dict() for r in self]

    def clear(self) -> "ProcessingLog":
        if enabled():
            super().clear()
        return self

    def _parse_parents(self, parents: List[Any], variable: str) -> List[str]:
        """Parents currently can be Variable, VariableMeta or str. Here we should ensure that they
        are strings. For example, we could extract the name of the parent if it is a variable."""
        new_parents = []
        for parent in parents:
            # Variable instance
            if hasattr(parent, "metadata"):
                parent = parent.metadata

            # VariableMeta instance
            if hasattr(parent, "processing_log"):
                if len(parent.processing_log) == 0:
                    new_parents.append(variable)
                else:
                    new_parents.append(parent.processing_log[-1].target)
            elif hasattr(parent, "name"):
                new_parents.append(parent.name)
            # owid.catalog.tables.ExcelFile
            elif hasattr(parent, "io"):
                from etl.paths import DATA_DIR

                new_parents.append(str(parent.io.relative_to(DATA_DIR)))
            else:
                new_parents.append(str(parent))

        return new_parents

    def add_entry(
        self,
        variable: str,
        parents: List[Any],
        operation: str,
        target: Optional[str] = None,
        comment: Optional[str] = None,
    ) -> None:
        if not enabled():
            # Avoid any processing
            return

        # Renaming has no effect, skip it.
        # TODO: is this right?
        if len(self) > 0 and self[-1].variable == variable and operation == "rename":
            return

        new_parents = self._parse_parents(parents, variable)

        if not target:
            target = f"{variable}#{random_hash()}"

        # Define new log entry.
        entry = LogEntry(
            variable=variable, parents=tuple(new_parents), operation=operation, target=target, comment=comment
        )

        # TODO: can this duplication happen?
        if entry in self:
            raise NotImplementedError("Fixme")

        self.append(entry)

    def display(self, output: Literal["text", "html"] = "html", show_upstream=True, auto_open=True):
        """Displays processing log as a Mermaid diagram in a browser or as a text.
        :param show_upstream: Show processing log for upstream channels too.
        """
        pl = self

        if show_upstream:
            pl = _add_upstream_channels(pl)

        pl = preprocess_log(pl)

        mermaid_lines = list(_mermaid_diagram(pl))

        if len(mermaid_lines) >= 100:
            print("WARNING: maximum lenght of processing log reached, showing incomplete diagram")
            mermaid_lines = mermaid_lines[:50]

        mermaid_diagram = "\n".join(mermaid_lines)

        if output == "text":
            return mermaid_diagram

        s = (
            """
<html>
<head>
    <title>Mermaid Diagram</title>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/mermaid/10.4.0/mermaid.min.js"></script>
    <style>
        .mermaid {
          margin: auto;
        }
    </style>
</head>
<body>
    <div class="mermaid">
        """
            + mermaid_diagram
            + """
    </div>

    <script>
        mermaid.initialize({startOnLoad:true});
    </script>
</body>
</html>
"""
        )

        with tempfile.NamedTemporaryFile(delete=False, suffix=".html") as temp:
            temp.write(s.encode())
            temp.seek(0)

        if auto_open:
            webbrowser.open("file://" + os.path.realpath(temp.name))


def wrap(operation: str, parents: List[str] = []):
    """Decorator that wraps function returning Table object. It disables
    processing log during the execution of the function and adds a clean new entry.
    """
    # TODO: this should work for functions taking Table as a argument and using columns
    #   as parents
    # TODO: make sure that the first argument `parent` is valid
    # TODO: validate that the function returns Table
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            with disable_processing_log():
                tb = func(*args, **kwargs)

            # TODO: fix circular imports
            from owid.catalog import Table
            from owid.catalog.variables import combine_variables_processing_logs

            if isinstance(args[0], Table):
                input_table = args[0]
                for col in tb.columns:
                    # if column is in the input table, use its processing log
                    if col in input_table.columns:
                        tb[col].m.processing_log = input_table[col].m.processing_log
                        tb[col].m.processing_log.add_entry(col, parents=[input_table[col]], operation=operation)
                    # if column is not there, use `parents` columns
                    else:
                        parent_variables = [input_table.loc[:, c] for c in parents]
                        tb[col].m.processing_log = combine_variables_processing_logs(variables=parent_variables)
                        tb[col].m.processing_log.add_entry(col, parents=parent_variables, operation=operation)

            else:
                # Use the first argument as a parent.
                parent = args[0]
                for col in tb.columns:
                    tb[col].m.processing_log.clear().add_entry(col, parents=[parent], operation=operation)

            return tb

        return wrapper

    return decorator


def random_hash():
    return random.randint(0, int(1e10))


def _sanitize_mermaid(s: str) -> str:
    return s.replace(" ", "_").replace(">", "").replace("<", "")


def _mermaid_diagram(pl: list[LogEntry]):
    yield "graph TB;"

    for r in pl:
        # TODO: multiple parents join with `&`
        for parent in r.parents:
            # constant or unknown column, add random hash to get unique vertices
            # if / is in parent, it means it's a path (TODO: make it more robust)
            if "#" not in parent and "/" not in parent:
                parent += f"#{random_hash()}"

            # some characters are not supported by Mermaid, replace them
            parent = _sanitize_mermaid(parent)
            target = _sanitize_mermaid(r.target)

            yield f"{parent}[{parent.split('#')[0]}] --> |{r.operation}| {target}[{target.split('#')[0]}]"


def preprocess_log(pl: ProcessingLog) -> ProcessingLog:
    # try to merge rename with previous operation
    new_pl = []
    last_r = None
    seen_r = set()
    for r in pl:
        # TODO: when is this happening??
        if str(r) in seen_r:
            continue
            # raise NotImplementedError("Fixme")

        if last_r and r.operation == "rename":
            # operation is just renaming, we can merge it with the previous one
            # exclude sort and load, these are nicer to keep
            if last_r.target == r.parents[0] and last_r.operation not in ("sort", "load", "pivot"):
                new_pl[-1] = last_r.clone(target=r.target, variable=r.variable)
                continue

        last_r = r
        new_pl.append(r)
        seen_r.add(str(r))

    return ProcessingLog(new_pl)


def _add_upstream_channels(pl: ProcessingLog) -> ProcessingLog:
    # TODO: get rid of this circular dependency
    from owid.catalog import Dataset

    from etl.paths import DATA_DIR

    # reverse processing log to traverse backwards
    pl: list = pl[::-1]
    new_pl = []

    seen_parents_variables = set()

    while pl:
        r = pl.pop(0)
        new_pl.append(r)

        # load upstream channels
        if r.operation == "load":
            assert len(r.parents) == 1
            parent = r.parents[0]

            if parent.startswith("snapshot"):
                continue

            if (parent, r.variable) in seen_parents_variables:
                continue

            dataset_name, table_name = parent.rsplit("/", 1)

            # TODO: this is inefficient, we're loading entire dataset to get a
            # single column
            tab = Dataset(DATA_DIR / dataset_name)[table_name]
            upstream_pl = tab[r.variable].m.processing_log

            # add reverted log to the queue
            pl = pl + upstream_pl[::-1]
            seen_parents_variables.add((parent, r.variable))

    return ProcessingLog(new_pl[::-1])
