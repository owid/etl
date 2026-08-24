import datetime as dt
import json
import time
from pathlib import Path

from structlog import get_logger

from etl.config import get_container_name
from etl.paths import BASE_DIR

log = get_logger()

SVG_TESTER_SUITES = ("graphers", "grapher-views", "mdims", "thumbnails")

# Written by devTools/svgTester/verify-graphs.ts, one per suite
VERIFY_RESULTS_FILENAME = "verify-results.json"

# Written by templates/lxc-manager/site-screenshots in owid/ops, next to the repos
SCREENSHOTS_STATUS_FILENAME = "site-screenshots-status.json"

# Mirrors SVG_TESTER_HEARTBEAT_STALE_MS in owid-grapher
HEARTBEAT_STALE_SECONDS = 90

# Mirrors SVG_TESTER_PROGRESS_INTERVAL_MS in owid-grapher
HEARTBEAT_POLL_SECONDS = 5


def run(branch: str, head_sha: str | None = None) -> str:
    container_name = get_container_name(branch)

    svgs_repo = BASE_DIR.parent / "owid-grapher-svgs"
    screenshots_status = read_screenshots_status(BASE_DIR.parent / SCREENSHOTS_STATUS_FILENAME)

    results_by_suite = {suite: resolve_running(svgs_repo / suite) for suite in SVG_TESTER_SUITES}

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
- **Site-screenshots:** {make_screenshots_line(screenshots_status, branch=branch, head_sha=head_sha)}
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


def read_screenshots_status(path: Path) -> dict | None:
    """Parsed site-screenshots-status.json, None when the file does not exist."""
    if not path.exists():
        return None

    try:
        return json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as e:
        log.warning("owidbot.site_screenshots.unreadable_status", path=str(path), error=str(e))
        return {"status": "unreadable"}


def make_screenshots_line(status: dict | None, branch: str, head_sha: str | None) -> str:
    """The Site-screenshots line: the compare link, plus how the run that produced it went.

    Worth stating because a failed run is invisible otherwise. The buildkite step soft
    fails, and a run that fails commits nothing, so the compare link keeps showing the
    previous run's diff - which reads exactly like a run that found no changes.

    There is one status file for all branches, because every branch's screenshots are
    taken in the same working copy on lxc-manager-1. A run of another branch that landed
    in between therefore says nothing about ours, and is reported as not-yet-reported.
    """
    compare_url = f"https://github.com/owid/site-screenshots/compare/{branch}"

    if status is None:
        return compare_url

    if status.get("status") == "unreadable":
        return f"⚠️ status file unreadable: {compare_url}"

    is_ours = status.get("branch") == branch and (not head_sha or status.get("grapherCommit") == head_sha)
    if not is_ours:
        pending = f"`{head_sha[:7]}`" if head_sha else "this commit"
        return f"⏳ the run for {pending} hasn't reported yet: {compare_url}"

    if status.get("status") == "ok":
        return f"✅ {compare_url}"

    return f"❌ the run failed, so {compare_url} is still the previous run's"


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
    """Whether the results above came from the PR's current commit.

    Expects statuses that resolve_running has already settled, so that `running` here
    means a run that is actually alive rather than one that died mid-run.
    """
    if not head_sha:
        return ""

    # An unreadable file carries no commit
    readable = [results for results in results_by_suite.values() if results and results.get("status") != "unreadable"]

    commits = {results.get("grapherCommit") for results in readable}
    if not commits:
        return ""

    if commits == {head_sha}:
        if any(results.get("status") == "running" for results in readable):
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

    # `stalled` is resolve_running's verdict on a `running` file whose heartbeat stopped
    if status in ("running", "stalled"):
        label = "⏳ running" if status == "running" else "⚠️ stopped mid-run"
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


def resolve_running(suite_dir: Path) -> dict | None:
    """A suite's results, with a `running` status resolved to `running` or `stalled`.

    A single read can't tell a live run from one killed a moment ago: both leave a recent
    heartbeat behind. owidbot reads the file once per build step and never comes back, so
    a wrong guess sits in the PR comment for good. A live run rewrites the file every few
    seconds; a dead one never does, so wait for the next tick instead of guessing.
    """
    results = read_verify_results(suite_dir)
    if not results or results.get("status") != "running":
        return results

    age = heartbeat_age(results)

    # Nothing to wait for: a heartbeat we can't read belongs to a file written before
    # heartbeats existed, and one already past the threshold is unambiguous. Both runs
    # are long over.
    if age is None or age > HEARTBEAT_STALE_SECONDS:
        log.info("owidbot.svg_tester.stalled", suite=suite_dir.name, heartbeat_age=age)
        return {**results, "status": "stalled"}

    # Wait out the remainder of the threshold, so a heartbeat that was already 80s old
    # costs 10s here rather than a fresh 90. A timestamp in the future - clock skew, or a
    # garbage-but-parseable date - has a negative age that would otherwise *extend* the
    # wait without bound, and neither owidbot buildkite step sets a timeout, so treat
    # anything ahead of now as if it had just been written.
    wait_for = HEARTBEAT_STALE_SECONDS - max(0.0, age)
    log.info("owidbot.svg_tester.awaiting_heartbeat", suite=suite_dir.name, seconds=round(wait_for))
    deadline = time.monotonic() + wait_for

    while time.monotonic() < deadline:
        time.sleep(HEARTBEAT_POLL_SECONDS)
        latest = read_verify_results(suite_dir)

        # Swept from under us by the next run's `git clean -fdx`. The run we were watching
        # still died, and reporting that beats reporting the suite as never run: a None
        # here would take the whole SVG tester block out of the comment.
        if latest is None:
            break

        # A torn read tells us nothing either way - the writer renames into place, so this
        # is near-impossible, but keep waiting rather than give up on the verdict.
        if latest.get("status") == "unreadable":
            continue

        # It ticked, or it reported while we waited.
        if latest.get("updatedAt") != results.get("updatedAt") or latest.get("status") != "running":
            return latest

    log.info("owidbot.svg_tester.stalled", suite=suite_dir.name, heartbeat_age=age)
    return {**results, "status": "stalled"}


def heartbeat_age(results: dict) -> float | None:
    """Seconds since the run last rewrote its results, None when there's no reading it."""
    updated_at = results.get("updatedAt")
    if not isinstance(updated_at, str):
        return None

    try:
        heartbeat = dt.datetime.fromisoformat(updated_at)
    except ValueError:
        return None

    if heartbeat.tzinfo is None:
        heartbeat = heartbeat.replace(tzinfo=dt.timezone.utc)

    return (dt.datetime.now(dt.timezone.utc) - heartbeat).total_seconds()


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
