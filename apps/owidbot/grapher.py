import datetime as dt
import json
from pathlib import Path

from structlog import get_logger

from etl.config import get_container_name
from etl.paths import BASE_DIR

log = get_logger()

SVG_TESTER_SUITES = ("graphers", "grapher-views", "mdims", "thumbnails")

# Written by devTools/svgTester/verify-graphs.ts, one per suite
VERIFY_RESULTS_FILENAME = "verify-results.json"

# Mirrors SVG_TESTER_HEARTBEAT_STALE_MS in owid-grapher
HEARTBEAT_STALE_SECONDS = 90


def run(branch: str, head_sha: str | None = None) -> str:
    container_name = get_container_name(branch)

    svgs_repo = BASE_DIR.parent / "owid-grapher-svgs"

    results_by_suite = {suite: read_verify_results(svgs_repo / suite) for suite in SVG_TESTER_SUITES}

    # The first owidbot run of a build happens before the SVG tester step, so no suite
    # has results yet and the per-suite block is left out.
    svg_tester_has_run = any(results is not None for results in results_by_suite.values())

    rows = "\n".join(
        f"- {suite.replace('-', ' ')}: {make_suite_line(results, container_name=container_name, suite=suite)}"
        for suite, results in results_by_suite.items()
    )

    details = "\n\n".join(part for part in (rows, make_freshness_note(results_by_suite, head_sha)) if part)

    svg_tester_block = (
        f"""
<details open>
<summary><b>SVG tester:</b> </summary>

{details}

</details>
""".strip()
        if svg_tester_has_run
        else ""
    )

    body = f"""
- **Site-screenshots:** https://github.com/owid/site-screenshots/compare/{branch}
- **SVG tester:** {make_report_url(container_name)}

<details open>
<summary><b>Archive:</b> </summary>

- [Data page with archive citation](http://{container_name}/grapher/life-expectancy)
- [Archived data page](http://{container_name}:8789/latest/grapher/life-expectancy.html)
- [Archived grapher page](http://{container_name}:8789/latest/grapher/life-expectancy-vs-healthcare-expenditure.html)
- [Archived indicator-based explorer](http://{container_name}:8789/latest/explorers/air-pollution.html)
- [Archived grapher-based explorer](http://{container_name}:8789/latest/explorers/co2.html)
- [Archived multidimensional data page](http://{container_name}:8789/latest/grapher/vaccination-coverage-who-unicef.html)
- [Archived article](http://{container_name}:8789/latest/vaping-vs-smoking-health-risks.html)
</details>

🎨 [Bespoke dev server](http://{container_name}:8089)

{svg_tester_block}
    """.strip()

    return body


def read_verify_results(suite_dir: Path) -> dict | None:
    """Parsed verify-results.json, None when the file does not exist."""
    path = suite_dir / VERIFY_RESULTS_FILENAME
    if not path.exists():
        return None

    try:
        return json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as e:
        # Reported as its own state rather than as absent: absence means "this suite
        # was never run". A truncated file is plausible - the process can be killed mid-write.
        log.warning("owidbot.svg_tester.unreadable_results", path=str(path), error=str(e))
        return {"status": "unreadable"}


def make_freshness_note(results_by_suite: dict[str, dict | None], head_sha: str | None) -> str:
    """Whether the results above came from the PR's current commit."""
    if not head_sha:
        return ""

    # An unreadable file carries no commit
    readable = [results for results in results_by_suite.values() if results and results.get("status") != "unreadable"]

    commits = {results.get("grapherCommit") for results in readable}
    if not commits:
        return ""

    if commits == {head_sha}:
        if any(results.get("status") == "running" and not is_stalled(results) for results in readable):
            return f"_⏳ Still running on the current commit `{head_sha[:7]}`._"
        return f"_Results are for the current commit `{head_sha[:7]}`._"

    ran_on = ", ".join(sorted(f"`{commit[:7]}`" if commit else "an unknown commit" for commit in commits - {head_sha}))
    return f"_⏳ Stale: from {ran_on}. The run for the current commit `{head_sha[:7]}` hasn't reported yet._"


def make_suite_line(results: dict | None, container_name: str, suite: str) -> str:
    """One suite's line in the PR comment."""
    if results is None:
        return "_skipped_"

    status = results.get("status")

    if status == "unreadable":
        return "⚠️ no result (results file unreadable)"

    counts = results.get("counts", {})
    report = f" ([report]({make_report_url(container_name, suite=suite)}))"

    if status == "running":
        # verify-graphs.ts writes this before its first render and rewrites it every few
        # seconds after. owidbot only reads the file once the SVG tester step is over, so
        # a heartbeat that stopped means the run was killed
        label = "⚠️ stopped mid-run" if is_stalled(results) else "⏳ running"
        return f"{label}, {describe_progress(counts)}{report}"

    num_differences = counts.get("differences", 0)
    num_errors = counts.get("errors", 0)

    if status == "ok":
        return "✅ no differences"

    notes = []
    if num_differences:
        notes.append(f"❌ {num_differences} difference{'s' if num_differences != 1 else ''}")
    if num_errors:
        notes.append(f"⚠️ {num_errors} error{'s' if num_errors != 1 else ''}")

    return f"{', '.join(notes)}{report}"


def is_stalled(results: dict) -> bool:
    """Whether a running suite's heartbeat has stopped, meaning the run itself has."""
    updated_at = results.get("updatedAt")
    try:
        heartbeat = dt.datetime.fromisoformat(updated_at)
    except (TypeError, ValueError):
        return True

    return (dt.datetime.now(dt.timezone.utc) - heartbeat).total_seconds() > HEARTBEAT_STALE_SECONDS


def describe_progress(counts: dict) -> str:
    """How far a run that hasn't reported yet has got."""
    total = counts.get("total", 0)
    num_differences = counts.get("differences", 0)

    # Until the run has enumerated its jobs, `total` is a zeroed placeholder.
    if not total:
        return "no charts checked yet"

    done = counts.get("ok", 0) + num_differences + counts.get("errors", 0)
    progress = f"{done:,} of {total:,} charts checked"
    return f"{progress}, {num_differences:,} differences so far" if num_differences else progress


def make_report_url(container_name: str, suite: str | None = None) -> str:
    """The SVG tester page on the staging container, for one suite or the index"""
    url = f"http://{container_name}/admin/svgtester"
    return f"{url}/{suite}" if suite else url
