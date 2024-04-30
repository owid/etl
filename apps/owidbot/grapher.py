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

    body = f"""
- **Site-screenshots**: https://github.com/owid/site-screenshots/compare/{branch}
- **SVG tester**: https://github.com/owid/owid-grapher-svgs/compare/{branch}

<details open>
<summary><b>SVG tester</b>: </summary>
Number of differences (default views): {default_views}

Number of differences (all views): {all_views}
</details>
    """.strip()

    return body
