"""Presentation helpers shared by the consumers of the reference sweep.

The sweep in `find_references.py` answers *what references this subject*. Every consumer
then has to turn a finding into something a human can click and act on, and that work is
identical whichever subject was swept: resolve a `where_path` to an absolute URL (routing
admin routes to the admin origin), build a scroll-to-the-reference deep link, name the
ArchieML component and the page type, produce a copy-paste search string for the Google
Doc, and shell out to the sweep itself.

It lives in this skill rather than in either consumer because this skill *is* the shared
producer — both `map-charts-to-mdim/scripts/audit_references.py` and
`map-explorer-to-mdim/scripts/audit_references.py` import from here, so a fix to the
`/admin/` routing or the text-fragment encoding lands in both at once. That has already
been a real bug in one consumer while the other was correct.

Deliberately NOT here: `replacement_url()` and the markdown table builders. A chart
reference's params merge over the target view's, reference-wins; an explorer reference's do
the opposite (the redirect *deletes* matched source params and lets view params win), and
the table columns differ. Sharing either would make one consumer subtly wrong.
"""

import hashlib
import json
import re
import subprocess
import sys
import tempfile
from pathlib import Path
from urllib.parse import quote, urlsplit

from etl.analytics.config import OWID_BASE_URL, POST_TYPE_TO_URL

FIND_REFERENCES = Path(__file__).resolve().parent / "find_references.py"

# Public path prefix per gdoc type, derived from the repo's own routing map rather than
# restated, so the two cannot drift: a data insight lives under /data-insights/, an author
# under /team/, everything else at the root — and `None` means the type has no public URL
# at all (fragment, homepage), so no link should be offered for it.
POST_TYPE_PATH = {
    post_type: None if base is None else base.removeprefix(OWID_BASE_URL)
    for post_type, base in POST_TYPE_TO_URL.items()
}

RED, YELLOW, INFO = "RED", "YELLOW", "INFO"
# Staging admin hosts carry a tailscale suffix that is noise in a link handed to a human.
TAILSCALE_SUFFIX_RE = re.compile(r"\.tail[0-9a-z]+\.ts\.net")


def run_sweep(subject_args: list[str]) -> tuple[list[dict], list[str]]:
    """Run the sweep for any subject and return (findings, gaps).

    `subject_args` is the subject selection verbatim, e.g. `["--chart-slugs", "a,b"]` or
    `["--explorer", "x", "--explorer", "y"]`.

    Returning the gaps alongside the findings is the whole point of going through here: the
    sweep fails open on optional surfaces (a legacy table that is absent, a subject that
    does not resolve), so a run that skipped one returns fewer references and no error —
    indistinguishable from a clean result unless the gaps travel with the findings into the
    consumer's own report.
    """
    if not FIND_REFERENCES.exists():
        raise SystemExit(f"Missing {FIND_REFERENCES} — this skill provides the surface sweep.")
    with tempfile.NamedTemporaryFile("r", suffix=".json", delete=False) as tmp:
        out_path = tmp.name
    with tempfile.NamedTemporaryFile("r", suffix=".json", delete=False) as tmp:
        gaps_path = tmp.name
    try:
        cmd = [sys.executable, str(FIND_REFERENCES), *subject_args, "--json", out_path, "--gaps-json", gaps_path]
        proc = subprocess.run(cmd, capture_output=True, text=True)
        if proc.returncode != 0:
            raise SystemExit(f"find_references.py failed:\n{proc.stdout}\n{proc.stderr}")
        print(proc.stdout.rstrip())
        gaps_raw = Path(gaps_path).read_text().strip()
        return json.loads(Path(out_path).read_text()), (json.loads(gaps_raw) if gaps_raw else [])
    finally:
        Path(out_path).unlink(missing_ok=True)
        Path(gaps_path).unlink(missing_ok=True)


# The identity of a reference, for freshness purposes: which surface, of what kind, on which
# page, pointing where. Presentation-only fields (`text`, the resolved URLs) are excluded on
# purpose — prose changing next to a link is not a new reference, and treating it as one trains
# people to re-run an audit to clear noise, which is how a real change gets waved through.
REFERENCE_DIGEST_FIELDS = (
    "surface",
    "kind",
    "where",
    "where_path",
    "surface_id",
    "config_id",
    "query_string",
    "published",
)


def reference_digest(raw: list[dict], subject: str) -> str:
    """Stable digest of the live references the sweep just found for one subject.

    An audit records this; a preflight recomputes it and compares. Without that binding, a gate
    reading the audit's CSV cannot tell whether the CSV still describes the site: a page that
    added an embed after the audit ran, or an audit folder carried over from an earlier
    migration, both leave the gate reporting a clean audit while the change is about to break
    something live. Order-insensitive, because SQL row order is not a fact about the site.

    The ArchieML component joins the fields above because consumers derive SEVERITY from it, not
    from `kind` alone: an `explorer-tiles` reference survives a redirect while a `chart` embed
    breaks, and both are `kind == "embed"` on the same page with the same ids. Swapping one for
    the other turns a harmless row into a blocker without moving any other field, so the
    component is digested — normalized, so the prose it is parsed out of still is not.
    """
    items = sorted(
        json.dumps(
            [archie_component(r)] + [r.get(f) for f in REFERENCE_DIGEST_FIELDS],
            sort_keys=True,
            ensure_ascii=False,
        )
        for r in raw
        if str(r.get("subject")) == str(subject)
    )
    return hashlib.sha256("\n".join(items).encode()).hexdigest()[:16]


def redirect_target_path(target: str | None) -> str:
    """Pathname of a site-redirect target, with a trailing slash normalized away.

    A target may be a bare path, a path carrying a query and/or fragment, or an absolute URL,
    and only its PATHNAME says which page it points at. A target that merely mentions another
    page inside its query — `/article?next=/explorers/foo` — does not point at that page, so
    substring-matching the raw string reports an unrelated redirect as an inbound chain. That
    is a blocker on both sides, and acting on it means repointing or deleting a redirect that
    had nothing to do with the migration.

    It lives here, alongside the presentation helpers, because the sweep and the preflight must
    apply this rule *identically*: the sweep decides whether to emit the finding and the
    preflight decides whether to block on it, and two copies of the rule already drifted once.
    """
    path = urlsplit((target or "").strip()).path
    return path.rstrip("/") or path


def absolute_url(where_path: str, host: str, admin: str = "") -> str:
    """Absolute URL for a `where_path`, routing admin routes to the admin origin.

    A narrative chart's `where_path` is its admin editor (`/admin/narrative-charts/…`),
    which the public site does not serve — prefixing it with `host` produces a link that
    404s. `admin` is an admin ROOT (".../admin") and the path already starts with
    `/admin/`, so the root's suffix comes off before joining.
    """
    if not where_path:
        return ""
    if where_path.startswith("/admin/"):
        origin = (admin or host).removesuffix("/").removesuffix("/admin")
        return f"{origin}{where_path}"
    return f"{host}{where_path}"


def public_page_url(post_type: str, slug: str, host: str) -> str:
    """Public URL of a gdoc, or "" when its type has no public route.

    A gdoc's slug does NOT sit at the root for every type — a data insight is served under
    /data-insights/ and an author page under /team/ — so building `host/<slug>` for all of
    them points the reader at a 404. An unknown type falls back to the root, which is where
    every currently routable type other than those two lives.
    """
    if not slug:
        return ""
    prefix = POST_TYPE_PATH.get(post_type, "")
    if prefix is None:
        return ""
    return f"{host}/{prefix}{slug}"


def deep_link(where_path: str, anchor: str, host: str, admin: str = "") -> str:
    """Published-page URL scrolled to the reference via a text fragment (block embeds
    have no anchor text, so those fall back to the plain URL). Same encoding as
    find-chart-references / chart_diff citations: parentheses literal, hyphens escaped."""
    base = absolute_url(where_path, host, admin)
    if not base or not anchor:
        return base
    encoded = quote(anchor[:200], safe="()").replace("-", "%2D")
    return f"{base}#:~:text={encoded}"


def page_deep_link(ref: dict, host: str, admin: str = "") -> str:
    """Public URL of the page holding this reference, scrolled to it — "" when it has none.

    The base cannot be taken from `where_path` alone. The gdoc-link sweep builds it as
    `/<slug>` for every gdoc type, but a data insight is served under `/data-insights/` and an
    author page under `/team/`, so those links 404 — and a prose link has anchor text, so the
    text fragment attaches successfully and makes the wrong base look like a working link. The
    page TYPE therefore decides the base whenever it is one the site routes, and `where_path`
    is used where it is the authority instead: admin routes, and surfaces whose type is not a
    routed gdoc type (a narrative chart's editor URL, or the data-insight front-matter surface,
    whose `where_path` already carries the right prefix).
    """
    ptype = page_type(ref)
    if ptype and POST_TYPE_PATH.get(ptype, "") is None:
        return ""  # fragment / homepage: no reader-facing URL exists
    base = ""
    if ptype in POST_TYPE_PATH and ref.get("where"):
        base = public_page_url(ptype, ref["where"], host)
    if not base:
        base = absolute_url(ref.get("where_path") or "", host, admin)
    if not base:
        return ""
    anchor = ref.get("text") or ""
    if not anchor:
        return base
    encoded = quote(anchor[:200], safe="()").replace("-", "%2D")
    return f"{base}#:~:text={encoded}"


def archie_component(ref: dict) -> str:
    """The ArchieML component this reference lives in: chart, span-link, front-matter, …

    The sweep encodes it as the head of `context` ("chart (article)", "span-link
    (data-insight)"); the data-insight surface spells its front-matter reference out in
    prose instead, so normalize that to the same token. This is what the gdoc tables
    group by — the person editing the doc cares which construct they are touching, not
    whether the page is an article or a data insight (both are gdocs).
    """
    head = ref["context"].split(" — ")[0]
    if head.startswith("grapher-url"):
        return "front-matter"
    return head.split(" (")[0].strip()


def page_type(ref: dict) -> str:
    """article / data insight / topic-page / fragment — the parenthesized tail of context.

    The data-insight surface spells its front-matter reference out in prose with no
    parenthesized suffix, so fall back to the surface name. Without that fallback those
    rows lose the page-type marker exactly where it matters most: articles and data
    insights share one table, and the Where column is the only thing distinguishing them.
    """
    m = re.search(r"\(([^)]+)\)", ref["context"].split(" — ")[0])
    if m:
        return m.group(1)
    return ref["surface"] if ref["surface"] == "data insight" else ""


def find_in_doc(ref: dict) -> str:
    """Copy-paste search string for the Google Doc's find box: the visible anchor text for
    a prose hyperlink; for a block embed the doc holds a bare URL, so the subject — exactly
    as the author typed it, which is what `posts_gdocs_links.target` stores and may be an
    old slug."""
    anchor = " ".join((ref.get("text") or "").split())
    return anchor or ref["subject"]


def cell(value: str, limit: int = 70, marker: str = "…") -> str:
    """Table-safe cell: escape pipes and newlines, truncate runaway text.

    Pass marker="" for copy-paste search strings: an appended ellipsis is a character
    that does not exist in the doc, so the copied text would match nothing — a bare
    literal prefix still finds the spot. Pipes are escaped after truncating so the cut
    can never leave half an escape sequence behind.
    """
    text = " ".join(str(value or "").split())
    if len(text) > limit:
        text = text[: limit - 1] + marker
    return text.replace("|", "\\|")
