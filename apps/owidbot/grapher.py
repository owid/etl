import json
from pathlib import Path

from git import InvalidGitRepositoryError, NoSuchPathError, Repo
from structlog import get_logger

from etl.config import get_container_name
from etl.paths import BASE_DIR

log = get_logger()

SVG_TESTER_SUITES = ("graphers", "grapher-views", "mdims", "thumbnails")

# Written by devTools/svgTester/verify-graphs.ts, one per suite
VERIFY_RESULTS_FILENAME = "verify-results.json"


def run(branch: str) -> str:
    container_name = get_container_name(branch)

    svgs_repo = BASE_DIR.parent / "owid-grapher-svgs"
    grapher_repo = BASE_DIR.parent / "owid-grapher"

    results_by_suite = {
        suite: load_suite_results(svgs_repo / suite, grapher_repo=grapher_repo) for suite in SVG_TESTER_SUITES
    }

    # The first owidbot run of a build happens before the SVG tester step,
    # so no suite has results yet and the whole block is left out.
    svg_tester_has_run = any(has_results(results) for results in results_by_suite.values())

    rows = "\n".join(
        f"- {suite.replace('-', ' ')}: {make_suite_line(results, container_name=container_name, suite=suite)}"
        for suite, results in results_by_suite.items()
    )

    svg_tester_line = f"- **SVG tester:** {make_report_url(container_name)}" if svg_tester_has_run else ""
    svg_tester_block = (
        f"""
<details open>
<summary><b>SVG tester:</b> </summary>

{rows}

</details>
""".strip()
        if svg_tester_has_run
        else ""
    )

    body = f"""
- **Site-screenshots:** https://github.com/owid/site-screenshots/compare/{branch}
{svg_tester_line}

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


def load_suite_results(suite_dir: Path, grapher_repo: Path) -> dict | None:
    """Results for one suite. None when the suite produced no file at all."""
    results = read_verify_results(suite_dir)
    if results is None:
        return None

    if is_stale(results, grapher_repo=grapher_repo):
        log.warning(
            "owidbot.svg_tester.stale_results",
            suite_dir=str(suite_dir),
            results_commit=results.get("grapherCommit"),
        )
        return {"status": "stale"}

    return results


def has_results(results: dict | None) -> bool:
    """True when the suite actually reported on the commit under test."""
    return results is not None and results.get("status") != "stale"


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


def is_stale(results: dict, grapher_repo: Path) -> bool:
    """True when the results describe a run against a different commit."""
    results_commit = results.get("grapherCommit")
    if not results_commit:
        return False

    head = get_head_commit(grapher_repo)
    if head is None:
        return False

    return results_commit != head


def get_head_commit(repo_path: Path) -> str | None:
    """HEAD of the grapher checkout the SVG tester ran against, or None if unavailable."""
    try:
        return Repo(repo_path).head.commit.hexsha
    except (InvalidGitRepositoryError, NoSuchPathError, ValueError) as e:
        log.warning("owidbot.svg_tester.no_grapher_checkout", repo_path=str(repo_path), error=str(e))
        return None


def make_suite_line(results: dict | None, container_name: str, suite: str) -> str:
    """One suite's line in the PR comment."""
    if results is None:
        return "_skipped_"

    status = results.get("status")

    if status == "stale":
        return "_skipped_ (ignored a leftover results file)"

    if status == "unreadable":
        return "⚠️ no result (results file unreadable)"

    if status == "running":
        # verify-graphs.ts writes this before its first render and overwrites it at the end
        return "_running_ (or killed mid-run)"

    counts = results.get("counts", {})
    num_differences = counts.get("differences", 0)
    num_errors = counts.get("errors", 0)

    if not num_differences and not num_errors:
        return "✅ no differences"

    notes = []
    if num_differences:
        notes.append(f"❌ {num_differences} difference{'s' if num_differences != 1 else ''}")
    if num_errors:
        notes.append(f"⚠️ {num_errors} error{'s' if num_errors != 1 else ''}")

    return f"{', '.join(notes)} ([report]({make_report_url(container_name, suite=suite)}))"


def make_report_url(container_name: str, suite: str | None = None) -> str:
    """The SVG tester page on the staging container, for one suite or the index"""
    url = f"http://{container_name}/admin/svgtester"
    return f"{url}/{suite}" if suite else url
