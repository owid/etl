import re
import subprocess
from typing import Tuple

from rich.ansi import AnsiDecoder

from etl.paths import BASE_DIR

EXCLUDE_DATASETS = "weekly_wildfires|excess_mortality|covid|fluid|flunet|country_profile|garden/ihme_gbd/2019/gbd_risk"


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


def format_etl_diff(lines: list[str]) -> Tuple[str, str]:
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


def call_etl_diff(include: str) -> list[str]:
    cmd = [
        "poetry",
        "run",
        "etl",
        "diff",
        "REMOTE",
        "data/",
        "--include",
        include,
        "--exclude",
        EXCLUDE_DATASETS,
        "--verbose",
        "--workers",
        "3",
    ]

    print(" ".join(cmd))

    result = subprocess.Popen(cmd, cwd=BASE_DIR, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = result.communicate()

    stdout = stdout.decode()
    stderr = stderr.decode()

    if stderr:
        raise Exception(f"Error: {stderr}")

    return [str(line) for line in AnsiDecoder().decode(stdout)]
