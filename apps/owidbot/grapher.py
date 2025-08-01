from etl.config import get_container_name
from etl.paths import BASE_DIR


def run(branch: str) -> str:
    container_name = get_container_name(branch)

    default_views = make_differences_line("verify-graphs_default-views.log", "commit_default-views.log")
    all_views = make_differences_line("verify-graphs_all-views.log", "commit_all-views.log")

    body = f"""
- **Site-screenshots:** https://github.com/owid/site-screenshots/compare/{branch}
- **SVG tester:** https://github.com/owid/owid-grapher-svgs/compare/{branch}

<details open>
<summary><b>Archive:</b> </summary>

- [Archived data page](http://{container_name}:8789/latest/grapher/life-expectancy.html)
- [Archived grapher page](http://{container_name}:8789/latest/grapher/life-expectancy-vs-healthcare-expenditure.html)
- [Archived multidimensional data page](http://{container_name}:8789/latest/grapher/vaccination-coverage-who-unicef.html)
- [Data page with archive citation](http://{container_name}/grapher/life-expectancy)
</details>

<details open>
<summary><b>SVG tester:</b> </summary>

Number of differences (default views): {default_views}
Number of differences (all views): {all_views}

</details>
    """.strip()

    return body


def make_differences_line(log_file: str, commit_file: str) -> str:
    try:
        num_differences = get_num_differences(log_file)
        status_icon = get_status_icon(num_differences)
    except FileNotFoundError:
        num_differences = "error"
        status_icon = "❓"

    commit_id = get_commit_id(commit_file)
    commit_link = f"({make_commit_link(commit_id=commit_id)})" if commit_id else ""

    return f"{num_differences} {commit_link} {status_icon}"


def get_num_differences(log_file: str) -> int:
    path = BASE_DIR.parent / "owid-grapher-svgs" / log_file
    with open(path) as f:
        return len(f.readlines())


def get_status_icon(num_differences: int) -> str:
    if num_differences > 0:
        return "❌"
    else:
        return "✅"


def get_commit_id(commit_file: str) -> str:
    path = BASE_DIR.parent / "owid-grapher-svgs" / commit_file
    try:
        with open(path) as f:
            return f.readline().strip()
    except FileNotFoundError:
        return ""


def make_commit_link(commit_id: str) -> str:
    commit_hash = commit_id[:6]
    commit_url = f"https://github.com/owid/owid-grapher-svgs/commit/{commit_id}"
    return f"[{commit_hash}]({commit_url})"
