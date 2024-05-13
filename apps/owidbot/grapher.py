from etl.paths import BASE_DIR


def run(branch: str) -> str:
    try:
        with open(BASE_DIR.parent / "owid-grapher-svgs/verify-graphs_default-views.log") as f:
            default_views = len(f.readlines())

        with open(BASE_DIR.parent / "owid-grapher-svgs/verify-graphs_all-views.log") as f:
            all_views = len(f.readlines())
    except FileNotFoundError:
        default_views = "error"
        all_views = "error"

    try:
        with open(BASE_DIR.parent / "owid-grapher-svgs/commit_default-views.log") as f:
            commit_id = f.readline().strip()
            default_views_commit = f"({make_commit_link(commit_id=commit_id)})" if commit_id else ""
    except FileNotFoundError:
        default_views_commit = ""

    try:
        with open(BASE_DIR.parent / "owid-grapher-svgs/commit_all-views.log") as f:
            commit_id = f.readline().strip()
            all_views_commit = f"({make_commit_link(commit_id=commit_id)})" if commit_id else ""
    except FileNotFoundError:
        all_views_commit = ""

    body = f"""
- **Site-screenshots**: https://github.com/owid/site-screenshots/compare/{branch}
- **SVG tester**: https://github.com/owid/owid-grapher-svgs/compare/{branch}

<details open>
<summary><b>SVG tester</b>: </summary>
Number of differences (default views): {default_views} {default_views_commit}

Number of differences (all views): {all_views} {all_views_commit}
</details>
    """.strip()

    return body


def make_commit_link(commit_id: str) -> str:
    commit_hash = commit_id[:6]
    commit_url = f"https://github.com/owid/owid-grapher-svgs/commit/{commit_id}"
    return f"[{commit_hash}]({commit_url})"
