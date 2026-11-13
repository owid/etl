"""Local app for reviewing explorer migrations, with the explorer and the multidim as live iframes.

Serves every review folder under `--root` (default `ai/`) that `build_review_page.py` has set up
(a `<slug>-review/` with `review.json` and `eval.json`): `/` lists them, `/<slug>/` is one
migration's page with both sides embedded live. Pages are rebuilt on every request, so re-running
`compare_views.py`, `scan_references.py` or `build_review_page.py` shows up on reload.

Live iframes need a page served over http (an https page can't frame the http staging server) and
the staging server reachable from the browser (Tailscale). Binds to localhost only.

Usage:
    .venv/bin/python .claude/skills/migrate-explorer-to-chart/scripts/review_app.py [--port 8771] [--root ai]
"""

import argparse
import html
import importlib
import json
import mimetypes
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import unquote, urlsplit

sys.path.insert(0, str(Path(__file__).parent))
import build_review_page  # noqa: E402


def reviews(root: Path) -> dict[str, Path]:
    return {
        d.name.removesuffix("-review"): d
        for d in sorted(root.glob("*-review"))
        if (d / "review.json").exists() and (d / "eval.json").exists()
    }


def hub(root: Path) -> str:
    rows = []
    for slug, d in reviews(root).items():
        ev = json.loads((d / "eval.json").read_text())
        cfg = json.loads((d / "review.json").read_text())
        pairs = ev["pairs"]
        same = sum(1 for p in pairs if not [c for c in p["checks"] if c["check"].split(":")[0] != "config"])
        rows.append(
            f"<tr><td><a href='/{html.escape(slug)}/'>{html.escape(slug)}</a></td><td>{len(pairs)}</td>"
            f"<td>{same} / {len(pairs)}</td><td>{html.escape(cfg['stage'])}</td><td>{html.escape(cfg['payload_env'])}</td></tr>"
        )
    body = "".join(rows) or "<tr><td colspan=5>No review folders yet: run build_review_page.py.</td></tr>"
    return f"""<!doctype html><meta charset="utf-8"><title>Explorer migration reviews</title>
<style>body{{font:15px system-ui,sans-serif;margin:32px;color:#18202b;background:#f5f6f8}}
table{{border-collapse:collapse;background:#fff}}td,th{{padding:8px 14px;border-bottom:1px solid #dfe3e8;text-align:left}}
th{{font-size:12px;text-transform:uppercase;letter-spacing:.05em;color:#5d6775}}a{{color:#2457a6}}</style>
<h1>Explorer migration reviews</h1>
<table><tr><th>Explorer</th><th>Views</th><th>Match on what readers see</th><th>Stage</th><th>Redirect JSON built on</th></tr>{body}</table>"""


def make_handler(root: Path):
    class Handler(BaseHTTPRequestHandler):
        def send(self, code: int, body: bytes, ctype: str) -> None:
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):  # noqa: N802
            parts = [p for p in unquote(urlsplit(self.path).path).split("/") if p]
            found = reviews(root)
            try:
                if not parts:
                    return self.send(200, hub(root).encode(), "text/html; charset=utf-8")
                if parts[0] not in found:
                    return self.send(404, b"Unknown review", "text/plain")
                d = found[parts[0]]
                if len(parts) == 1:
                    if not self.path.endswith("/"):
                        self.send_response(301)
                        self.send_header("Location", f"/{parts[0]}/")
                        return self.end_headers()
                    page = "<!doctype html><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'>"
                    # Reload the builder too, so edits to its template show without a restart.
                    importlib.reload(build_review_page)
                    body = page + build_review_page.render_page(d, live=True)
                    return self.send(200, body.encode(), "text/html; charset=utf-8")
                f = (d / "/".join(parts[1:])).resolve()
                if d.resolve() not in f.parents or not f.is_file():
                    return self.send(404, b"Not found", "text/plain")
                return self.send(200, f.read_bytes(), mimetypes.guess_type(f.name)[0] or "application/octet-stream")
            except Exception as e:  # show the error in the browser rather than a dropped connection
                return self.send(500, f"{type(e).__name__}: {e}".encode(), "text/plain")

        def log_message(self, *args):
            pass

    return Handler


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--port", type=int, default=8771)
    ap.add_argument("--root", type=Path, default=Path("ai"))
    a = ap.parse_args()
    # Take the next free port if this one is busy (another local server may hold it).
    for port in range(a.port, a.port + 20):
        try:
            srv = ThreadingHTTPServer(("127.0.0.1", port), make_handler(a.root))
            break
        except OSError:
            continue
    else:
        raise SystemExit(f"No free port in {a.port}-{a.port + 19}")
    print(f"Reviews: http://localhost:{port}/  ({', '.join(reviews(a.root)) or 'none yet'})")
    srv.serve_forever()
