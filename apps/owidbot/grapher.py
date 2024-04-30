def run(branch: str) -> str:
    body = f"""
- **Site-screenshots**: https://github.com/owid/site-screenshots/compare/{branch}
- **SVG tester**: https://github.com/owid/owid-grapher-svgs/compare/{branch}
    """.strip()
    return body
