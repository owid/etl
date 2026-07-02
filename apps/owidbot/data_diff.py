import os
import re
import shlex
import subprocess

import structlog
from rich.ansi import AnsiDecoder

from etl.paths import BASE_DIR

log = structlog.get_logger()

EXCLUDE_DATASETS = "excess_mortality|covid|fluid|flunet|country_profile|garden/ihme_gbd/2019/gbd_risk"


def run(include: str) -> str:
    lines = call_etl_diff(include)
    data_diff, data_diff_summary = format_etl_diff(lines)

    body = f"""
<details>
<summary><b>data-diff</b>: {data_diff_summary}</summary>

```diff
{data_diff}
```

Automatically updated datasets matching _{EXCLUDE_DATASETS}_ are not included
</details>
    """.strip()

    return body


def format_etl_diff(lines: list[str]) -> tuple[str, str]:
    new_lines = []
    result = ""
    for line in lines:
        # extract result
        if line and line[0] in ("✅", "❌", "⚠️", "❓"):
            result = line
            continue

        # skip some lines
        if "this may get slow" in line or "comparison with compare" in line:
            continue

        if line.strip().startswith("-"):
            line = "-" + line[1:]
        if line.strip().startswith("+"):
            line = "+" + line[1:]

        new_lines.append(line)

    diff = "\n".join(new_lines)

    # Don't show output if there are no changes and the diff is just too long
    # Example:
    # = Dataset meadow/agriculture/2024-03-26/attainable_yields
    #     = Table attainable_yields
    # = Dataset garden/agriculture/2024-03-26/attainable_yields
    #     = Table attainable_yields
    #        ~ Column A
    # = Dataset grapher/agriculture/2024-03-26/attainable_yields
    #     = Table attainable_yields
    if len(diff) > 64000:
        pattern = r"(= Dataset.*(?:\n\s+=.*)+)\n(?=. Dataset|\n)"
        diff = re.sub(pattern, "", diff)

    # Github has limit of 65,536 characters
    if len(diff) > 64000:
        diff = diff[:64000] + "\n\n...diff too long, truncated..."

    return diff, result


def _tail_output(output: str, max_lines: int = 80) -> str:
    """Return the last non-empty lines from command output for error messages."""
    lines = output.strip().splitlines()
    if len(lines) > max_lines:
        lines = [f"... ({len(lines) - max_lines} lines omitted)", *lines[-max_lines:]]
    return "\n".join(lines)


def call_etl_diff(include: str) -> list[str]:
    cmd = [
        "uv",
        "run",
        "etl",
        "diff",
        "REMOTE",
        "data/",
        "--changed",
        "--include",
        include,
        "--exclude",
        EXCLUDE_DATASETS,
        "--verbose",
        "--workers",
        "3",
    ]

    cmd_str = shlex.join(cmd)
    print(cmd_str)

    env = os.environ.copy()
    env["PATH"] = os.path.expanduser("~/.cargo/bin") + ":" + env["PATH"]

    result = subprocess.Popen(cmd, cwd=BASE_DIR, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env, text=True)
    stdout, stderr = result.communicate()

    # Remove all warnings from stderr
    stderr = re.sub(r"^.*WARNING.*", "", stderr, flags=re.MULTILINE).strip()

    # Remove certain warnings from stdout
    stdout = re.sub(
        r"^.*You're on master branch, using local env instead of STAGING=master*", "", stdout, flags=re.MULTILINE
    )

    if result.returncode == 1 and "Found differences" in stdout:
        log.info("etl diff found differences", returncode=result.returncode)
    elif result.returncode != 0:
        details = [f"etl diff failed (exit {result.returncode})", f"Command: {cmd_str}"]
        if stderr:
            details.append(f"stderr (tail):\n{_tail_output(stderr)}")
        if stdout:
            details.append(f"stdout (tail):\n{_tail_output(stdout)}")
        if not stdout and not stderr:
            details.append("No stdout or stderr was captured.")
        raise RuntimeError("\n\n".join(details))
    if stderr:
        log.warning("etl diff produced stderr output", stderr=stderr)

    return [str(line) for line in AnsiDecoder().decode(stdout)]
