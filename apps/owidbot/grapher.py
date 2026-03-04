from pathlib import Path

from etl.config import get_container_name
from etl.paths import BASE_DIR


def run(branch: str) -> str:
    container_name = get_container_name(branch)

    svg_tester_dirs = ("graphers", "grapher-views", "mdims", "explorers", "thumbnails")
    svg_tester_has_run = any(
        (BASE_DIR.parent / "owid-grapher-svgs" / dir_name / "verify-graphs.log").exists()
        for dir_name in svg_tester_dirs
    )
    svg_tester_graphers = make_differences_line("graphers") if svg_tester_has_run else ""
    svg_tester_grapher_views = make_differences_line("grapher-views") if svg_tester_has_run else ""
    svg_tester_mdims = make_differences_line("mdims") if svg_tester_has_run else ""
    svg_tester_explorers = make_differences_line("explorers") if svg_tester_has_run else ""
    svg_tester_thumbnails = make_differences_line("thumbnails") if svg_tester_has_run else ""

    svg_tester_line = (
        f"- **SVG tester:** https://github.com/owid/owid-grapher-svgs/compare/{branch}" if svg_tester_has_run else ""
    )
    svg_tester_block = (
        f"""
<details open>
<summary><b>SVG tester:</b> </summary>

Number of differences (graphers): {svg_tester_graphers}
Number of differences (grapher views): {svg_tester_grapher_views}
Number of differences (mdims): {svg_tester_mdims}
Number of differences (explorers): {svg_tester_explorers}
Number of differences (thumbnails): {svg_tester_thumbnails}

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

{svg_tester_block}
    """.strip()

    return body


def make_differences_line(dir: str) -> str:
    log_file = BASE_DIR.parent / "owid-grapher-svgs" / dir / "verify-graphs.log"
    commit_file = BASE_DIR.parent / "owid-grapher-svgs" / dir / "commit.log"
    report_filename = f"{dir}/differences.html"

    # Handle missing log files based on the test suite type:
    # - 'graphers' is the core test suite that always runs: missing file indicates an error
    # - Other test suites are optional: missing file likely means skipped
    try:
        num_differences = get_num_differences(log_file)
        status_icon = get_status_icon(num_differences)
    except FileNotFoundError:
        num_differences = "error" if dir == "graphers" else "_skipped_"
        status_icon = "❓" if dir == "graphers" else ""

    commit_id = get_commit_id(commit_file)
    commit_link = f"({make_commit_link(commit_id=commit_id)})" if commit_id else ""
    report_link = (
        f"[Report]({make_report_url(commit_id=commit_id, report_filename=report_filename)})"
        if status_icon == "❌" and commit_id
        else ""
    )

    return f"{num_differences} {commit_link} {status_icon} {report_link}".strip()


def get_num_differences(path: Path) -> int:
    with open(path) as f:
        return len(f.readlines())


def get_status_icon(num_differences: int) -> str:
    if num_differences > 0:
        return "❌"
    else:
        return "✅"


def get_commit_id(path: Path) -> str:
    try:
        with open(path) as f:
            return f.readline().strip()
    except FileNotFoundError:
        return ""


def make_commit_link(commit_id: str) -> str:
    commit_hash = commit_id[:6]
    commit_url = f"https://github.com/owid/owid-grapher-svgs/commit/{commit_id}"
    return f"[{commit_hash}]({commit_url})"


def make_report_url(commit_id: str, report_filename: str) -> str:
    # raw.githack.com serves raw files from GitHub with proper HTML content type
    return f"https://rawcdn.githack.com/owid/owid-grapher-svgs/{commit_id}/{report_filename}"
