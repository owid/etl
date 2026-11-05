"""Publish shared navigation and page assets to the local prototype output directory."""

import hashlib
import shutil
from pathlib import Path

from etl.paths import BASE_DIR

HERE = Path(__file__).parent
OUT = BASE_DIR / "ai/aqli_gridded/webapp"
PAGES = [
    ("country_explorer.html", "index.html", "Countries and cities", "./"),
    ("pm25_explorer.html", "global.html", "Global grid", "global.html"),
    ("comparison.html", "comparison.html", "Compare scales", "comparison.html"),
]


def publish_pages():
    OUT.mkdir(parents=True, exist_ok=True)
    revision = hashlib.sha256(
        b"".join(
            (HERE / name).read_bytes()
            for name in ["site_shell.css", "time_comparison.js", *[page[0] for page in PAGES]]
        )
    ).hexdigest()[:12]
    for source, filename, _, _ in PAGES:
        links = "".join(
            f'<a href="{href}"' + (' aria-current="page"' if target == filename else "") + f">{label}</a>"
            for _, target, label, href in PAGES
        )
        shell = f"""<header class="site-header"><div class="site-header-inner">
<a class="site-brand" href="./" aria-label="Prague offsite 2026">Prague offsite 2026</a>
<nav class="site-nav" aria-label="Prototype pages">{links}</nav></div></header>
<section class="site-hero"><div class="site-hero-inner">
<h1>Air pollution data: A closer look</h1>
<p>What can we learn from looking at air pollution on the local level?</p>
</div></section>"""
        html = (HERE / source).read_text()
        html = html.replace("site_shell.css", f"site_shell.css?v={revision}").replace(
            "comparison.css", f"comparison.css?v={revision}"
        )
        html = html.replace("time_comparison.js", f"time_comparison.js?v={revision}")
        html = html.replace(
            "</head>",
            '<link rel="icon" href="data:image/svg+xml,%3Csvg xmlns=%22http://www.w3.org/2000/svg%22 viewBox=%220 0 100 100%22%3E%3Ctext y=%22.9em%22 font-size=%2290%22%3E%F0%9F%8C%8D%3C/text%3E%3C/svg%3E">\n</head>',
        )
        assert html.count("<!-- SITE_SHELL -->") == 1
        (OUT / filename).write_text(html.replace("<!-- SITE_SHELL -->", shell))
    shutil.copy(HERE / "site_shell.css", OUT / "site_shell.css")
    shutil.copy(HERE / "time_comparison.js", OUT / "time_comparison.js")
    style = (HERE / "country_explorer.html").read_text().split("<style>")[1].split("</style>")[0]
    (OUT / "comparison.css").write_text(style)


if __name__ == "__main__":
    publish_pages()
