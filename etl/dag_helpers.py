import re
from collections import defaultdict
from pathlib import Path
from typing import Any

import yaml
from structlog import get_logger

from etl import paths

log = get_logger()
Graph = dict[str, set[str]]


def _iter_nested_dep_items(deps: Any):
    """Yield every ``(step, sub_deps)`` pair nested under ``deps``.

    ``deps`` is the raw value from a ruamel ``steps:`` entry: either ``None``,
    an empty list, or a list whose items are strings or single-key mappings.
    Only the mapping items are yielded.
    """
    if not isinstance(deps, list):
        return
    for item in deps:
        if isinstance(item, dict):
            if len(item) != 1:
                raise ValueError(f"Nested dependency must be a single-key mapping, got keys {list(item)!r}.")
            sub_node, sub_deps = next(iter(item.items()))
            yield sub_node, sub_deps
            yield from _iter_nested_dep_items(sub_deps)


def _nested_deps_to_flat(deps: Any) -> list[str]:
    """Return the flat list of dep strings for a dep value that may contain nested mappings."""
    flat: list[str] = []
    if not isinstance(deps, list):
        return flat
    for item in deps:
        if isinstance(item, str):
            flat.append(item)
        elif isinstance(item, dict):
            sub_node, _ = next(iter(item.items()))
            flat.append(sub_node)
    return flat


_NESTED_LINE_RE = re.compile(r"^(?P<indent>\s*)- (?P<step>\S+):\s*(?:#.*)?$")
_FLAT_STEP_LINE_RE = re.compile(r"^(?P<indent>\s*)(?P<step>\S+):\s*(?:#.*)?$")


def flatten_dag_file(dag_file: Path) -> bool:
    """Rewrite ``dag_file`` so that every step is declared at the top level.

    This is a pure text rewrite driven by YAML-aware indentation tracking.
    Ruamel does a poor job of round-tripping comments that live *inside*
    nested dep sequences (it attaches them to item tails, which a structural
    mutation would drop), so we avoid ruamel here: comments, blank lines, and
    formatting survive verbatim.

    Returns ``True`` if the file contents changed, ``False`` otherwise.
    """
    dag_file = Path(dag_file)
    original = dag_file.read_text()
    flat = _flatten_dag_text(original)
    if flat is None or flat == original:
        return False
    dag_file.write_text(flat)
    return True


def _flatten_dag_text(original: str) -> str | None:
    """Return the flat-form DAG text for ``original``, or ``None`` if no change is needed.

    Pure function over text — useful for callers that want to flatten without
    touching disk (e.g. :func:`compact_dag_file`, which flattens in memory
    before computing the fold plan).
    """
    # A nested declaration is a list item whose value is another mapping:
    # ``- data://garden/...:`` (line ends with ``:`` after the step name).
    if not any(_NESTED_LINE_RE.match(line) for line in original.splitlines()):
        return None

    lines = original.splitlines(keepends=True)
    steps_start = _find_steps_start(lines)
    if steps_start is None:
        return None
    steps_end = _find_steps_end(lines, steps_start)

    pre = lines[:steps_start]
    steps_body = lines[steps_start:steps_end]
    post = lines[steps_end:]

    rewritten_body, promoted = _flatten_lines(steps_body)
    promoted_lines = _format_promoted(promoted)

    if promoted_lines:
        while rewritten_body and rewritten_body[-1].strip() == "":
            rewritten_body.pop()
        rewritten_body.append("\n")
        rewritten_body.extend(promoted_lines)

    return "".join(pre + rewritten_body + post)


def _find_steps_start(lines: list[str]) -> int | None:
    for i, line in enumerate(lines):
        if line.strip() == "steps:":
            return i + 1
    return None


def _find_steps_end(lines: list[str], start: int) -> int:
    """Return the index of the first line after the ``steps:`` section.

    The section ends at the first top-level key (``include:``, another
    top-level mapping key) or at EOF.
    """
    for i in range(start, len(lines)):
        stripped = lines[i].lstrip()
        if not stripped or stripped.startswith("#"):
            continue
        indent = len(lines[i]) - len(stripped)
        if indent == 0:
            return i
    return len(lines)


def _flatten_lines(body: list[str]) -> tuple[list[str], list[tuple[str, list[str]]]]:
    """Flatten nested declarations in ``body`` and return (new_body, promoted).

    ``promoted`` is a list of ``(step_name, dep_lines)`` tuples, in the order
    they were encountered. Each ``dep_lines`` is a list of already-normalised
    lines ready to be written under ``  <step>:`` at the top level.
    """
    new_body: list[str] = []
    promoted: list[tuple[str, list[str]]] = []

    i = 0
    while i < len(body):
        line = body[i]
        match = _NESTED_LINE_RE.match(line)
        if match:
            step = match.group("step")
            dash_indent = len(match.group("indent"))
            # Replace the nested declaration with a plain dep reference, dropping
            # the trailing ``:``.
            new_body.append(" " * dash_indent + f"- {step}\n")

            # Collect the nested block: every subsequent line that is
            # indented deeper than ``dash_indent``. A blank line terminates
            # the block — nested chains do not contain blank lines in
            # practice, and treating one as a terminator preserves the blank
            # as an inter-step separator in the outer output.
            j = i + 1
            while j < len(body):
                next_line = body[j]
                if next_line.strip() == "":
                    break
                next_indent = len(next_line) - len(next_line.lstrip())
                if next_indent > dash_indent:
                    j += 1
                else:
                    break
            block = body[i + 1 : j]

            # Dedent the block so it becomes the dep list of a top-level step.
            # Top-level step dep indent is 4 (``    - dep``). Current dep indent
            # starts at ``dash_indent + 2 + 2`` (dash + space at the nested
            # dash_indent, children indented at +2 then +2 more for sequence
            # offset). Instead of guessing, measure the minimum indent of
            # non-empty block lines and dedent to 4.
            non_empty_indents = [len(bl) - len(bl.lstrip()) for bl in block if bl.strip() != ""]
            if non_empty_indents:
                current_indent = min(non_empty_indents)
                dedent = current_indent - 4
                if dedent < 0:
                    dedent = 0
                block = [
                    bl if bl.strip() == "" else (bl[dedent:] if len(bl) - len(bl.lstrip()) >= dedent else bl)
                    for bl in block
                ]

            # Recursively flatten the promoted block (chains of arbitrary depth).
            block, sub_promoted = _flatten_lines(block)
            # Strip trailing blank lines — those separate the next top-level
            # step from this block in the source file and do not belong to
            # the promoted entry.
            while block and block[-1].strip() == "":
                block.pop()
            promoted.append((step, block))
            promoted.extend(sub_promoted)

            i = j
        else:
            new_body.append(line)
            i += 1

    return new_body, promoted


def _format_promoted(promoted: list[tuple[str, list[str]]]) -> list[str]:
    """Format promoted ``(step, dep_lines)`` entries as flat top-level block lines."""
    out: list[str] = []
    for step, dep_lines in promoted:
        out.append(f"  {step}:\n")
        out.extend(dep_lines)
    return out


def build_consumer_graph(dag_file: Path | str = paths.DEFAULT_DAG_FILE) -> dict[str, set[str]]:
    """Return ``{dep: {consumers}}`` for the DAG rooted at ``dag_file``.

    Reuses :func:`load_dag`, so cross-file ``include:`` relations are honoured
    automatically.
    """
    result: dict[str, set[str]] = defaultdict(set)
    for parent, deps in load_dag(dag_file).items():
        for dep in deps:
            result[dep].add(parent)
    return result


def compact_dag_file(
    dag_file: Path,
    consumers: dict[str, set[str]] | None = None,
) -> bool:
    """Rewrite ``dag_file`` to fold linear dependency chains into the nested syntax.

    A dep ``D`` is folded into its parent step ``P`` iff:

    * ``D`` is declared at the top level of **the same file** as ``P``, and
    * ``D`` has exactly one consumer across the whole active DAG, namely ``P``.

    The fold is iterative: once ``D`` is folded under ``P``, ``D``'s own single
    dep may be folded under ``D``, and so on. This is exactly the shape
    produced by a textbook ``snapshot → meadow → garden → grapher`` chain.

    Comments preceding each top-level step are preserved. Comments that used
    to precede a step that gets folded away are migrated onto the outermost
    surviving step. Top-level ordering is preserved for the surviving steps.

    Returns ``True`` if the file changed, ``False`` otherwise.

    ``consumers`` optionally supplies the pre-computed reverse graph
    ``{dep: {parents}}``. When ``None`` (the default), the full active DAG
    (``paths.DEFAULT_DAG_FILE``) is loaded — this is what the CLI uses so
    cross-file consumers prevent spurious folds. Callers that compact many
    files in a batch should build this map once and pass it in.
    """
    dag_file = Path(dag_file)
    original = dag_file.read_text()

    # Flatten in memory so the file on disk is never left in a half-written
    # state if compaction decides to make no change.
    flat = _flatten_dag_text(original) or original

    blocks = _parse_dag_blocks_from_text(flat)
    if blocks is None:
        return False

    preamble, step_blocks, trailer = blocks

    # Build the global consumer map for fold decisions. ``load_dag`` already
    # flattens nested syntax correctly (resolving parent-child relationships
    # through a chain), so we reuse it rather than doing our own parse.
    if consumers is None:
        consumers = build_consumer_graph()

    # Map step -> block (for folding lookups) and compute fold decisions.
    block_by_step = {block["step"]: block for block in step_blocks}
    folded_into: dict[str, str] = {}  # dep -> parent when folded

    # Pass: figure out fold parents. A step can be folded into at most one
    # parent (its unique consumer). We also require the parent to be a
    # top-level step in the same file, and we refuse to fold if the child's
    # dep list contains inline comments — we have nowhere to put them once
    # the block becomes a single nested list item. Looping until stable
    # handles the case where a fold makes a new parent reachable (e.g.
    # a grandchild that was previously only reachable through a fold that
    # hadn't happened yet).
    while True:
        added = False
        for parent_block in step_blocks:
            parent = parent_block["step"]
            if parent in folded_into:
                # ``parent`` is itself folded; its already-chosen deps stay
                # valid because folding is transitive, but we should not
                # introduce new folds that would shadow that fact.
                continue
            stack = [parent_block]
            while stack:
                cur = stack.pop()
                for dep in cur["deps"]:
                    if dep in folded_into:
                        continue
                    if dep not in block_by_step:
                        continue
                    if consumers.get(dep) != {cur["step"]}:
                        continue
                    if block_by_step[dep]["has_inline_comment"]:
                        continue
                    folded_into[dep] = cur["step"]
                    added = True
                    stack.append(block_by_step[dep])
        if not added:
            break

    if not folded_into:
        return False

    # Comment migration. When a chain collapses, the comment that used to sit
    # above the chain's head (typically the meadow step) is the natural label
    # for the whole chain. After compaction only the outermost survivor is
    # emitted at the top level, so migrate each folded step's own comments
    # onto that survivor. Walk folded_into from folded step back to its
    # top-level ancestor, accumulating comments in file order: the innermost
    # folded step's comments come *first*, followed by the surviving step's
    # own comments (which may be empty).
    surviving_comments: dict[str, list[str]] = {
        b["step"]: list(b["comments"]) for b in step_blocks if b["step"] not in folded_into
    }
    for block in step_blocks:
        step = block["step"]
        if step not in folded_into:
            continue
        ancestor = folded_into[step]
        while ancestor in folded_into:
            ancestor = folded_into[ancestor]
        # Only migrate real comments — inherited blank separators would
        # otherwise accumulate on the ancestor every time compact is run
        # after flatten (``flatten_dag_file`` inserts a blank before each
        # promoted block).
        non_blank = [line for line in block["comments"] if line.strip() != ""]
        if non_blank:
            surviving_comments[ancestor] = block["comments"] + surviving_comments.get(ancestor, [])

    # For the *first* surviving step, drop any leading blank lines — they are
    # an artefact of the ``steps:`` header and produce distracting gaps.
    first_surviving = next((b["step"] for b in step_blocks if b["step"] not in folded_into), None)
    if first_surviving is not None:
        while surviving_comments.get(first_surviving) and surviving_comments[first_surviving][0].strip() == "":
            surviving_comments[first_surviving].pop(0)

    # Emit the compacted file.
    out_lines: list[str] = []
    out_lines.extend(preamble)
    for block in step_blocks:
        if block["step"] in folded_into:
            continue  # swallowed by its parent below
        out_lines.extend(surviving_comments.get(block["step"], []))
        out_lines.extend(_emit_step_block(block, block_by_step, folded_into, depth=0))
    if trailer and out_lines and out_lines[-1].strip() != "":
        out_lines.append("\n")
    out_lines.extend(trailer)

    new_text = "".join(out_lines)
    if new_text == original:
        return False
    dag_file.write_text(new_text)
    return True


def _emit_step_block(
    block: dict[str, Any],
    block_by_step: dict[str, dict[str, Any]],
    folded_into: dict[str, str],
    depth: int,
) -> list[str]:
    """Emit a step block (optionally nested) as a list of file lines.

    ``depth`` 0 means we are emitting a top-level step:

        ``  step:``
        ``    - dep``

    ``depth`` 1 means the step is a nested child of a top-level parent:

        ``    - step:``
        ``      - dep``

    And so on. Each nesting level shifts by 2 spaces, matching the YAML
    conventions used in ``dag/*.yml``.
    """
    if depth == 0:
        step_indent = 2
        dep_indent = 4
        header = f"{' ' * step_indent}{block['step']}:\n"
    else:
        # The parent's dep list sits at `(depth - 1) * 2 + 4` spaces indent.
        # So a nested mapping item lives at the same indent as that dep list.
        dash_indent = 4 + (depth - 1) * 2
        step_indent = dash_indent
        dep_indent = dash_indent + 2
        header = f"{' ' * step_indent}- {block['step']}:\n"

    # When none of this block's deps get folded AND the block is at the top
    # level, emit the original raw dep lines verbatim so inline comments and
    # any other formatting survive untouched.
    any_fold = any((dep in folded_into and folded_into[dep] == block["step"]) for dep in block["deps"])
    if depth == 0 and not any_fold and block.get("raw_dep_lines"):
        return [header] + list(block["raw_dep_lines"])

    lines: list[str] = [header]
    for dep in block["deps"]:
        if dep in folded_into and folded_into[dep] == block["step"]:
            child_block = block_by_step[dep]
            lines.extend(_emit_step_block(child_block, block_by_step, folded_into, depth + 1))
        else:
            lines.append(f"{' ' * dep_indent}- {dep}\n")
    return lines


def _parse_dag_blocks(dag_file: Path) -> tuple[list[str], list[dict[str, Any]], list[str]] | None:
    """Parse ``dag_file`` into ``(preamble, step_blocks, trailer)``. See :func:`_parse_dag_blocks_from_text`."""
    return _parse_dag_blocks_from_text(dag_file.read_text())


def _parse_dag_blocks_from_text(content: str) -> tuple[list[str], list[dict[str, Any]], list[str]] | None:
    """Parse a DAG YAML string into ``(preamble, step_blocks, trailer)``.

    ``preamble`` is every line up to and including ``steps:``. ``trailer`` is
    every line from the first non-indented mapping key after ``steps:`` to
    EOF (typically the ``include:`` block, if any). Each element of
    ``step_blocks`` is a dict with keys ``comments`` (list of lines that
    immediately precede the step), ``step`` (step name), and ``deps`` (list
    of dep name strings).

    Returns ``None`` if the file has no ``steps:`` section at all.
    """
    lines = content.splitlines(keepends=True)

    steps_start = _find_steps_start(lines)
    if steps_start is None:
        return None
    steps_end = _find_steps_end(lines, steps_start)

    preamble = lines[:steps_start]
    body = lines[steps_start:steps_end]
    trailer = lines[steps_end:]

    step_blocks: list[dict[str, Any]] = []
    pending_comments: list[str] = []
    i = 0
    while i < len(body):
        line = body[i]
        stripped = line.lstrip()
        if stripped == "" or stripped.startswith("#"):
            pending_comments.append(line)
            i += 1
            continue

        match = _FLAT_STEP_LINE_RE.match(line)
        if not match:
            # A line we do not understand (maybe a malformed file). Flush
            # pending comments back and keep moving.
            pending_comments.append(line)
            i += 1
            continue

        step = match.group("step")
        step_indent = len(match.group("indent"))
        deps: list[str] = []
        raw_dep_lines: list[str] = []
        has_inline_comment = False
        # Track the run of blank/comment lines trailing the dep list — once
        # we have seen a blank, any subsequent comment (even indented at the
        # dep level) is almost certainly a section header for the *next*
        # step, not an inline dep annotation. Rewind through them when the
        # block ends so they re-attach to the next iteration as pending
        # comments.
        trailing_after_blank: list[str] = []
        saw_blank = False
        j = i + 1
        while j < len(body):
            next_line = body[j]
            next_stripped = next_line.lstrip()
            if next_stripped == "":
                trailing_after_blank.append(next_line)
                saw_blank = True
                j += 1
                continue
            next_indent = len(next_line) - len(next_stripped)
            if next_indent == 0:
                break
            if next_stripped.startswith("- "):
                # A new dep line resumes the block; any blank/comment lines
                # we buffered belong inside this step's dep area.
                for buffered in trailing_after_blank:
                    raw_dep_lines.append(buffered)
                trailing_after_blank = []
                saw_blank = False
                deps.append(next_stripped[2:].rstrip())
                raw_dep_lines.append(next_line)
                j += 1
                continue
            if next_stripped.startswith("#"):
                if next_indent <= step_indent or saw_blank:
                    # Either a step-indent-level comment or a dep-indent
                    # comment that follows a blank line — treat as belonging
                    # to the next step.
                    trailing_after_blank.append(next_line)
                    j += 1
                    continue
                # Deeper-indented comment, no preceding blank. Look ahead:
                # if the next non-blank, non-comment line is a step line at
                # our indent or shallower, treat the comment as a (slightly
                # mis-indented) section header for the next step rather
                # than an inline dep annotation.
                lookahead = j + 1
                while lookahead < len(body):
                    peek = body[lookahead]
                    peek_stripped = peek.lstrip()
                    if peek_stripped == "" or peek_stripped.startswith("#"):
                        lookahead += 1
                        continue
                    break
                peek_is_next_step = False
                if lookahead < len(body):
                    peek = body[lookahead]
                    peek_stripped = peek.lstrip()
                    peek_indent = len(peek) - len(peek_stripped)
                    if peek_indent <= step_indent and not peek_stripped.startswith("- "):
                        peek_is_next_step = True
                if peek_is_next_step:
                    trailing_after_blank.append(next_line)
                    j += 1
                    continue
                # Real inline dep comment — preserve with the dep list.
                raw_dep_lines.append(next_line)
                has_inline_comment = True
                j += 1
                continue
            # Hit something we do not understand at a non-zero indent; stop.
            break
        # Push the trailing blanks/comments back so they attach to the next step.
        if trailing_after_blank:
            j -= len(trailing_after_blank)

        # Normalise pending_comments: keep trailing blank lines out (they belong
        # between blocks, not tied to the step) and drop the contiguous run of
        # comment lines that belong to this step.
        # We attach all leading blank+comment lines to the step so they move
        # together if the step is folded away (and therefore dropped).
        step_blocks.append(
            {
                "comments": list(pending_comments),
                "step": step,
                "deps": deps,
                "raw_dep_lines": raw_dep_lines,
                "has_inline_comment": has_inline_comment,
            }
        )
        pending_comments = []
        i = j

    return preamble, step_blocks, trailer


def get_comments_above_step_in_dag(step: str, dag_file: Path) -> str:
    """Get the comment lines right above a step in the dag file."""

    # Read the content of the dag file.
    with open(dag_file) as _dag_file:
        lines = _dag_file.readlines()

    # Initialize a list to store the header lines.
    header_lines = []
    for line in lines:
        if line.strip().startswith("-") or (
            line.strip().endswith(":") and (not line.strip().startswith("#")) and (step not in line)
        ):
            # Restart the header if the current line:
            # * Is a dependency.
            # * Is a step that is not the current step.
            # * Is a special line like "steps:" or "include:".
            header_lines = []
        elif step in line and line.strip().endswith(":"):
            # If the current line is the step, stop reading the rest of the file.
            return "\n".join([line.strip() for line in header_lines]) + "\n" if len(header_lines) > 0 else ""
        elif line.strip() == "":
            # If the current line is empty, ignore it.
            continue
        else:
            # Any line that is not a dependency,
            header_lines.append(line)

    # If the step has not been found, raise an error and return nothing.
    log.error(f"Step {step} not found in dag file {dag_file}.")

    return ""


def write_to_dag_file(
    dag_file: Path,
    dag_part: dict[str, Any],
    comments: dict[str, str] | None = None,
    indent_step=2,
    indent_dependency=4,
):
    """Update the content of a dag file, respecting the comments above the steps.

    NOTE: A simpler implementation of function may be possible using ruamel. However, I couldn't find out how to respect
    comments that are above steps.

    Parameters
    ----------
    dag_file : Path
        Path to dag file.
    dag_part : Dict[str, Any]
        Partial dag, containing the steps that need to be updated.
        This partial dag is a dictionary with steps as keys and the set of dependencies as values.
    comments : Optional[Dict[str, str]], optional
        Comments to add above the steps in the partial dag. The keys are the steps, and the values are the comments.
    indent_step : int, optional
        Number of spaces to use as indentation for steps in the dag.
    indent_dependency : int, optional
        Number of spaces to use as indentation for dependencies in the dag.

    """

    # If comments is not defined, assume an empty dictionary.
    if comments is None:
        comments = {}

    for step in comments:
        if len(comments[step]) > 0 and comments[step][-1] != "\n":
            # Ensure all comments end in a line break, otherwise add it.
            comments[step] = comments[step] + "\n"

    # The line-based logic below assumes every step is declared at the top level.
    # If ``dag_file`` uses the compact nested syntax, flatten it first so that
    # the update touches the right entries. Re-compaction (if desired) is
    # expected to run separately, e.g. via ``etl dag compact``.
    flatten_dag_file(dag_file)

    # Read the lines in the original dag file.
    with open(dag_file) as file:
        lines = file.readlines()

    # Separate that content into the "steps" section (always given) and the "include" section (sometimes given).
    section_steps = []
    section_include = []
    inside_section_steps = True
    for line in lines:
        if line.strip().startswith("include"):
            inside_section_steps = False
        if inside_section_steps:
            section_steps.append(line)
        else:
            section_include.append(line)

    # Now the "steps" section will be updated, and at the end the "include" section will be appended.

    # Initialize a list with the new lines that will be written to the dag file.
    updated_lines = []
    # Initialize a list of comments preceding the next step after a given step.
    comments_next_step = []
    # Initialize a flag to skip lines until the next step.
    skip_until_next_step = False
    # Initialize a set to keep track of the steps that were found in the original dag file.
    steps_found = set()
    for line in section_steps:
        # Remove leading and trailing whitespace from the line.
        stripped_line = line.strip()

        # Identify the start of a step, e.g. "  data://meadow/temp/latest/step:".
        if stripped_line.endswith(":") and not stripped_line.startswith("-") and not stripped_line.startswith("steps:"):
            if comments_next_step:
                updated_lines.extend(comments_next_step)
                comments_next_step = []
            # Extract the name of the step (without the ":" at the end).
            current_step = ":".join(stripped_line.split(":")[:-1])
            if current_step in dag_part:
                # This step was in dag_part, which means it needs to be updated.
                # First add the step itself.
                updated_lines.append(line)
                # Now add each of its dependencies.
                for dep in dag_part[current_step]:
                    updated_lines.append(" " * indent_dependency + f"- {dep}\n")
                # Skip the following lines until the next step is found.
                skip_until_next_step = True
                # Start tracking possible comments of the next step.
                comments_next_step = []
                # Add the current step to the set of steps found in the dag file.
                steps_found.add(current_step)
                continue
            else:
                # This step was not in dag_part, so it will be copied as is.
                skip_until_next_step = False

        # Skip dependencies and comments among dependencies of the step being updated.
        if skip_until_next_step:
            if stripped_line.startswith("-"):
                # Remove comments among dependencies.
                comments_next_step = []
                continue
            elif stripped_line.startswith("#"):
                # Add comments that may potentially be related to the next step.
                comments_next_step.append(line)
                continue

        # Add lines that should not be skipped.
        updated_lines.append(line)

    # Append new steps that weren't found in the original content.
    for step, dependencies in dag_part.items():
        if step not in steps_found:
            # Add the comment for this step, if any was given.
            if step in comments:
                updated_lines.append(
                    " " * indent_step + ("\n" + " " * indent_step).join(comments[step].split("\n")[:-1]) + "\n"
                    if len(comments[step]) > 0
                    else ""
                )
            # Add the step itself.
            updated_lines.append(" " * indent_step + f"{step}:\n")
            # Add each of its dependencies.
            for dep in dependencies:
                updated_lines.append(" " * indent_dependency + f"- {dep}\n")

    if len(section_include) > 0:
        # Append the include section, ensuring there is only one line break in between.
        for i in range(len(updated_lines) - 1, -1, -1):
            if updated_lines[i] != "\n":
                # Slice the list to remove trailing line breaks
                updated_lines = updated_lines[: i + 1]
                break
        # Add a single line break before the include section, and then add the include section.
        updated_lines.extend(["\n"] + section_include)

    # Write the updated content back to the dag file.
    with open(dag_file, "w") as file:
        file.writelines(updated_lines)


def _remove_step_from_dag_file(dag_file: Path, step: str) -> None:
    # Flatten any nested chains first so the line-based logic below finds the step.
    flatten_dag_file(dag_file)

    with open(dag_file) as file:
        lines = file.readlines()

    new_lines = []
    _number_of_comment_lines = 0
    _step_detected = False
    _continue_until_the_end = False
    num_spaces_indent = 0
    for line in lines:
        if line.startswith("include"):
            # Nothing should be removed from here onwards, so, skip until the end of the file.
            _continue_until_the_end = True

            # Ensure there is a space before the include section starts.
            if new_lines[-1].strip() != "":
                new_lines.append("\n")

        if line.startswith("steps:"):
            # Store this special line and move on.
            new_lines.append(line)
            # If there were comments above "steps", keep them.
            _number_of_comment_lines = 0
            continue

        if _continue_until_the_end:
            new_lines.append(line)
            continue

        if not _step_detected:
            if line.strip().startswith("#") or line.strip() == "":
                _number_of_comment_lines += 1
                new_lines.append(line)
                continue
            elif line.strip().startswith(step + ":"):
                if _number_of_comment_lines > 0:
                    # Remove the previous comment lines and ignore the current line.
                    new_lines = new_lines[:-_number_of_comment_lines]
                # Find the number of spaces on the left of the step name.
                # We need this to know if the next comments are indented (as comments within dependencies).
                num_spaces_indent = len(line) - len(line.lstrip())
                _step_detected = True
                continue
            else:
                # This line corresponds to any other step or step dependency.
                new_lines.append(line)
                _number_of_comment_lines = 0
                continue
        else:
            if line.strip().startswith("- "):
                # Ignore the dependencies of the step.
                continue
            elif (line.strip().startswith("#")) and (len(line) - len(line.lstrip()) > num_spaces_indent):
                # Ignore comments that are indented (as comments within dependencies).
                continue
            elif line.strip() == "":
                # Ignore empty lines.
                continue
            else:
                # The step dependencies have ended. Append current line and continue until the end of the dag file.
                new_lines.append(line)
                _continue_until_the_end = True
                continue

    # Write the new content to the active dag file.
    with open(dag_file, "w") as file:
        file.writelines(new_lines)


def remove_steps_from_dag_file(dag_file: Path, steps_to_remove: list[str]) -> None:
    """Remove specific steps from a dag file, including their comments.

    Parameters
    ----------
    dag_file : Path
        Path to dag file.
    steps_to_remove : List[str]
        List of steps to be removed from the DAG file.
        Their dependencies do not need to be specified (they will also be removed).

    """
    for step in steps_to_remove:
        _remove_step_from_dag_file(dag_file=dag_file, step=step)


def create_dag_archive_file(dag_file_archive: Path) -> None:
    """Create an empty dag archive file, and add it to the main dag archive file.

    Parameters
    ----------
    dag_file_archive : Path
        Path to a specific dag archive file that does not exist yet.

    """
    # Create a new archive dag file.
    dag_file_archive.write_text("steps:\n")
    # Find the number of spaces in the indentation of the main dag archive file.
    n_spaces_include_section = 2
    with open(paths.DAG_ARCHIVE_FILE) as file:
        lines = file.readlines()
    for i, line in enumerate(lines):
        if line.strip().startswith("include"):
            n_spaces_include_section = [
                len(_line) - len(_line.lstrip()) for _line in lines[i + 1 :] if _line.strip().startswith("- ")
            ][0]
    # Add this archive dag file to the main dag archive file.
    dag_file_archive_relative = dag_file_archive.relative_to(Path(paths.DAG_DIR).parent)
    with open(paths.DAG_ARCHIVE_FILE, "a") as file:
        file.write(f"{' ' * n_spaces_include_section}- {dag_file_archive_relative}\n")


def load_dag(filename: str | Path = paths.DEFAULT_DAG_FILE) -> Graph:
    return _load_dag(filename, {})


def load_single_dag_file(filename: str | Path) -> Graph:
    """Load the steps declared in a single DAG YAML file, without following ``include``.

    Returns the same flat ``{step: {deps}}`` shape as :func:`load_dag`, so every
    step declared in the file — whether at the top level or nested under
    another step — appears as a top-level key. Useful for tools that need to
    attribute each step to the exact DAG file where it lives.
    """
    return _parse_dag_yaml(_load_dag_yaml(str(filename)))


def _load_dag(filename: str | Path, prev_dag: dict[str, Any]):
    """
    Recursive helper to 1) load a dag itself, and 2) load any sub-dags
    included in the dag via 'include' statements
    """
    dag_yml = _load_dag_yaml(str(filename))
    curr_dag = _parse_dag_yaml(dag_yml)

    # make sure there are no fast-track steps in the DAG
    if "fasttrack.yml" not in str(filename):
        fast_track_steps = {step for step in curr_dag if "/fasttrack/" in step}
        if fast_track_steps:
            raise ValueError(f"Fast-track steps detected in DAG {filename}: {fast_track_steps}")

    duplicate_steps = prev_dag.keys() & curr_dag.keys()
    if duplicate_steps:
        raise ValueError(f"Duplicate steps detected in DAG {filename}: {duplicate_steps}")

    curr_dag.update(prev_dag)

    for sub_dag_filename in dag_yml.get("include", []):
        sub_dag = _load_dag(paths.BASE_DIR / sub_dag_filename, curr_dag)
        curr_dag.update(sub_dag)

    return curr_dag


def _load_dag_yaml(filename: str) -> dict[str, Any]:
    with open(filename) as istream:
        return yaml.safe_load(istream)


def _parse_dag_yaml(dag: dict[str, Any]) -> dict[str, Any]:
    """Parse the ``steps:`` section of a DAG YAML into a flat ``{step: {deps}}`` mapping.

    Supports two equivalent syntaxes side by side:

    * **Flat** (historical) — every step is a top-level key under ``steps:``::

          data://meadow/a: [snapshot://a]
          data://garden/a: [data://meadow/a]

    * **Nested** (compact) — a dep list item may be a single-key mapping
      ``{step: [sub-deps]}``, declaring the step and its dependencies in place.
      The nested form is recursively flattened to the same shape the rest of the
      codebase consumes::

          data://garden/a:
            - data://meadow/a:
              - snapshot://a
    """
    steps = dag["steps"] or {}

    result: dict[str, set[str]] = {}
    for node, deps in steps.items():
        _insert_dag_node(result, node, deps)
    return result


def _insert_dag_node(result: dict[str, set[str]], node: str, deps: Any) -> None:
    """Insert ``node`` with its (possibly nested) ``deps`` into ``result``.

    ``deps`` can be ``None``, an empty list, a list of strings, or a list that
    contains single-key mappings ``{sub_step: [sub_sub_deps]}``. Nested mappings
    are recursively unfolded so that every encountered step ends up as a
    top-level key in ``result``.
    """
    if node in result:
        raise ValueError(f"Duplicate step detected in DAG: {node}")
    if deps is None:
        result[node] = set()
        return
    if isinstance(deps, set):
        # Legacy ``!!set`` YAML form used by a handful of fasttrack entries:
        # every element is a string, no nesting possible.
        result[node] = {str(d) for d in deps}
        return
    if not isinstance(deps, list):
        raise ValueError(f"Step {node!r} has dependencies that are not a list: {deps!r}")

    dep_names: set[str] = set()
    for item in deps:
        if isinstance(item, str):
            dep_names.add(item)
        elif isinstance(item, dict):
            if len(item) != 1:
                raise ValueError(
                    f"Nested dependency under {node!r} must be a single-key mapping, got keys {list(item)!r}. "
                    "Write each nested step as its own list item."
                )
            sub_node, sub_deps = next(iter(item.items()))
            dep_names.add(sub_node)
            _insert_dag_node(result, sub_node, sub_deps)
        else:
            raise ValueError(
                f"Dependency of {node!r} must be a string or a single-key mapping, got {type(item).__name__}: {item!r}"
            )

    result[node] = dep_names


def graph_nodes(graph: Graph) -> set[str]:
    """Get all nodes from a DAG (both keys and values)."""
    all_steps = set(graph)
    for children in graph.values():
        all_steps.update(children)
    return all_steps


def get_active_snapshots() -> set[str]:
    DAG = load_dag()

    active_snapshots = set()

    for s in graph_nodes(DAG):
        if s.startswith("snapshot"):
            active_snapshots.add(s.split("://")[1])

    # Strip extension
    return {s.split(".")[0] + ".py" for s in active_snapshots}


def get_active_steps() -> set[str]:
    DAG = load_dag()

    active_steps = set()

    for s in graph_nodes(DAG):
        if not s.startswith("snapshot"):
            active_steps.add(s.split("://")[1])

    # Strip dataset name after version
    return {s.rsplit("/", 1)[0] for s in active_steps}
