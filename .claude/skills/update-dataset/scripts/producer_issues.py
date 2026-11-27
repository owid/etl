"""Keep the log of problems found in a producer's data during an update, and turn it into a message to them.

Every step of `/update-dataset` that finds a problem in the source (snapshot diff, sanity checks, harmonization, the
fact check and its Anomalist pass, data-diff, the user's QA) adds an entry to
`workbench/<short_name>/producer-issues.yml`. Procedure and entry format: `reference/producer-issues.md`.

    producer_issues.py template <short_name> <namespace>/<version>/<short_name>   start the log
    producer_issues.py check <short_name>                  which entries are ready to send, and why the rest are not
    producer_issues.py contacts <namespace>/<version>/<short_name>   where to write: emails, forms, citation authors
    producer_issues.py history <namespace>/<version>/<short_name>    earlier reports: corrections, owid-issues, PRs
    producer_issues.py draft <short_name> [--ids 1,3]      the message skeleton, from ready entries only

An entry is ready only when the problem is in the raw snapshot (so it is the producer's, not ours), earlier reports
were searched for (`previously_reported`: where and when, or `none`), and every kind of producer documentation was
searched for an explanation and none fully explains it. The script never sends anything.
"""

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path
from urllib.parse import urljoin, urlsplit

import yaml

HERE = Path(__file__).resolve().parent
LOG = "producer-issues.yml"
MESSAGE = "producer-message.md"
KINDS = ("error", "suspected", "question")
STATUSES = ("open", "sent", "answered", "dropped")
# The producer documentation every entry must have been checked against. A kind the producer does not publish is
# recorded with `document: not published`, so "not searched" and "does not exist" stay apart.
DOC_KINDS = {
    "methodology": "methodology or technical notes",
    "codebook": "codebook, data dictionary or indicator definitions",
    "country_notes": "country or series notes",
    "release_notes": "release notes, what's new, revisions",
    "file_notes": "footnotes, flags and notes sheets inside the data file itself",
}
EXPLAINED = ("no", "partly", "yes")
EMAIL = re.compile(r"(?<![\w.+-])[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)*\.[A-Za-z]{2,}")
URL = re.compile(r"https?://[^\s\"'<>\]\[`|)]+")
CONTACT_WORDS = re.compile(r"contact|feedback|helpdesk|help-desk|support|enquir|inquir|query|queries|ask-|get-in-touch")
DATA_MAILBOX = re.compile(r"^(data|stat|help|info|contact|support|quer|enquir|inquir|feedback|dissemination)")
JUNK_EMAIL = re.compile(
    r"(example\.(com|org)|noreply|no-reply|donotreply|sentry|wixpress|\.(png|jpe?g|gif|svg|webp)$|@2x|@\d+x)",
    re.IGNORECASE,
)
# Generic second-level labels of country-code domains (`gov.uk`, `govt.nz`, `gob.cl`): under them each label is a
# separate organization, so `stats.govt.nz` and `health.govt.nz` are two agencies, not one.
GENERIC_SECOND_LEVEL = {
    "ac",
    "co",
    "com",
    "edu",
    "gc",
    "go",
    "gob",
    "gouv",
    "gov",
    "govt",
    "mil",
    "ne",
    "net",
    "nic",
    "or",
    "org",
}


# ----------------------------------------------------------------------------------------------------------------
# The log
# ----------------------------------------------------------------------------------------------------------------


def workbench(short_name: str) -> Path:
    from etl import paths

    return paths.BASE_DIR / "workbench" / short_name


def load_log(path: Path) -> dict:
    if not path.exists():
        sys.exit(f"No log at {path}: start one with `producer_issues.py template <short_name> <step>`.")
    return yaml.safe_load(path.read_text()) or {}


def verdict(issue: dict) -> tuple[str, list[str]]:
    """(`ready` | `held` | `ours` | `explained` | <status>, the reasons), for one entry of the log."""
    status = str(issue.get("status") or "open")
    if status not in STATUSES:
        return "held", [f"status `{status}` is not one of {', '.join(STATUSES)}"]
    if status != "open":
        return status, []
    reasons = [f"no `{field}`" for field in ("summary", "where") if not issue.get(field)]
    if issue.get("kind") not in KINDS:
        reasons.append(f"`kind` must be one of {', '.join(KINDS)}")
    in_raw = issue.get("in_raw_snapshot")
    if in_raw is False:
        return "ours", ["not in the raw snapshot: our processing introduced it, so fix the step; never sent"]
    if in_raw is not True:
        reasons.append("`in_raw_snapshot` not established: read the raw snapshot first (source-errors.md)")
    checked = {entry.get("kind"): entry for entry in issue.get("docs_checked") or [] if isinstance(entry, dict)}
    missing = [kind for kind in DOC_KINDS if kind not in checked]
    if missing:
        reasons.append("documentation not checked: " + ", ".join(f"{k} ({DOC_KINDS[k]})" for k in missing))
    for kind, entry in checked.items():
        empty = [field for field in ("document", "looked_for", "result") if not entry.get(field)]
        if kind in DOC_KINDS and empty and str(entry.get("document", "")).strip() != "not published":
            reasons.append(f"`docs_checked` {kind}: no {', '.join(empty)}")
    if issue.get("previously_reported") in (None, ""):
        reasons.append(
            "`previously_reported` not recorded: search for earlier reports first (`history`), then write where and "
            "when it was reported, or `none`"
        )
    explained = explained_of(issue)
    if explained not in EXPLAINED:
        reasons.append("`explained` must be no, partly or yes, once the documentation is checked")
    elif explained in ("partly", "yes") and not issue.get("explanation"):
        reasons.append("`explanation` must quote the passage (document and section)")
    if reasons:
        return "held", reasons
    if explained == "yes":
        return "explained", ["the documentation explains it: not sent. Does our metadata carry that caveat?"]
    return "ready", ["sent as a question quoting the passage"] if explained == "partly" else []


def explained_of(issue: dict) -> str:
    """`explained` as written: YAML reads a bare `no`/`yes` as a boolean, so map those back."""
    value = issue.get("explained")
    if isinstance(value, bool):
        return "yes" if value else "no"
    return "" if value is None else str(value).strip().lower()


def entry_id(issue: dict, number: int) -> str:
    """An entry's `id` as text, or its position in the log when it has none."""
    return str(issue.get("id", number))


def duplicate_ids(issues: list[dict]) -> list[str]:
    """The `id`s used by more than one entry, as `#<id>`: a draft keyed by id would silently keep only one."""
    counts: dict[str, int] = {}
    for number, issue in enumerate(issues, start=1):
        counts[entry_id(issue, number)] = counts.get(entry_id(issue, number), 0) + 1
    return [f"#{key}" for key, count in counts.items() if count > 1]


def check(log: dict) -> tuple[str, list[dict]]:
    issues = log.get("issues") or []
    rows = []
    for number, issue in enumerate(issues, start=1):
        state, reasons = verdict(issue)
        rows.append(
            {"id": entry_id(issue, number), "state": state, "summary": issue.get("summary", ""), "why": reasons}
        )
    out = [f"# Producer issues: {log.get('dataset', '?')} ({log.get('producer', 'producer unknown')})", ""]
    repeated = duplicate_ids(issues)
    if repeated:
        out += [
            f"**Repeated ids: {', '.join(repeated)}.** Give each entry its own `id`: `draft` refuses until then.",
            "",
        ]
    if not rows:
        return "\n".join(out + ["The log is empty: nothing to send. The draft step is complete.", ""]), rows
    order = ("ready", "held", "explained", "ours", "sent", "answered", "dropped")
    for state in order:
        group = [row for row in rows if row["state"] == state]
        if not group:
            continue
        out += [f"## {state} ({len(group)})", ""]
        for row in group:
            out.append(f"- #{row['id']}: {row['summary']}")
            out += [f"  - {reason}" for reason in row["why"]]
        out.append("")
    return "\n".join(out), rows


# ----------------------------------------------------------------------------------------------------------------
# The draft
# ----------------------------------------------------------------------------------------------------------------


def draft(log: dict, ids: list[str] | None) -> str:
    """The message skeleton for the chosen ready entries; refuses any entry that is not ready."""
    issues = log.get("issues") or []
    repeated = duplicate_ids(issues)
    if repeated:
        sys.exit("Not drafted. Give each entry its own `id`; these are used more than once: " + ", ".join(repeated))
    by_id = {entry_id(issue, number): issue for number, issue in enumerate(issues, start=1)}
    chosen = ids or [key for key, issue in by_id.items() if verdict(issue)[0] == "ready"]
    refused = [
        f"#{key}: {verdict(by_id[key])[0]}" for key in chosen if key in by_id and verdict(by_id[key])[0] != "ready"
    ]
    unknown = [f"#{key}" for key in chosen if key not in by_id]
    if refused or unknown:
        sys.exit("Not drafted. Only ready entries go in a message: " + ", ".join(refused + unknown))
    if not chosen:
        sys.exit("No entry is ready: run `check` to see what holds each one back.")

    picked = [by_id[key] for key in chosen]
    follow_ups = [issue for issue in picked if issue.get("previously_reported") not in (None, "", "none")]
    out = [
        "<!-- Skeleton from producer_issues.py: rewrite every [FILL] in plain words, keep the facts. Not sent. -->",
        "",
        f"To: [FILL: contact from `producer_issues.py contacts {log.get('dataset', '<step>')}`]",
        f"Subject: [FILL: short, names the dataset and release] {log.get('title') or log.get('dataset', '')}",
        "",
        "[FILL: greeting]",
        "",
        f"[FILL: the ask first. We use {log.get('title') or 'your data'} ({log.get('release', 'release [FILL]')}) on Our "
        f"World in Data, and while updating it we found {len(picked)} point(s) we would like to check with you. "
        "Could you confirm …?]",
        "",
    ]
    for number, issue in enumerate(picked, start=1):
        where = issue.get("where") or {}
        where_text = ", ".join(f"{k}: {v}" for k, v in where.items()) if isinstance(where, dict) else str(where)
        out += [f"{number}. {issue.get('summary', '').strip()}", f"   - Where: {where_text}"]
        values = issue.get("values") or {}
        if values:
            out.append("   - Values: " + "; ".join(f"{k}: {v}" for k, v in values.items()))
        for item in issue.get("evidence") or []:
            out.append(f"   - Evidence: {item}")
        searched = [e for e in issue.get("docs_checked") or [] if str(e.get("document", "")) != "not published"]
        if searched:
            out.append(
                "   - We checked: " + "; ".join(f"{e['document']} ({e.get('looked_for', '')})" for e in searched)
            )
        if explained_of(issue) == "partly":
            out.append(f"   - Your documentation says: {issue.get('explanation')} [FILL: ask whether this covers it]")
        if issue in follow_ups:
            out.append(f"   - Raised before: {issue['previously_reported']} [FILL: say so, as a follow-up]")
        out.append(f"   - [FILL: the question, as a question{' (kind: ' + str(issue.get('kind')) + ')'}]")
        out.append("")
    out += [
        "[FILL: thanks, and that we are happy to share more detail]",
        "",
        "[FILL: sign-off with the sender's name]",
        "",
        f"<!-- Entries: {', '.join('#' + key for key in chosen)} -->",
    ]
    return "\n".join(out) + "\n"


# ----------------------------------------------------------------------------------------------------------------
# Contacts
# ----------------------------------------------------------------------------------------------------------------


def organization(host: str) -> str:
    """The organization's domain of a host: `datacatalog.worldbank.org` → `worldbank.org`, `stats.govt.nz` stays.

    Under a country-code domain with a generic second level (`gov.uk`, `govt.nz`), the organization is the label
    before it. A heuristic, not the full public-suffix list: an unusual suffix can still merge two organizations.
    """
    labels = host.lower().rstrip(".").removeprefix("www.").split(".")
    generic = len(labels) >= 3 and len(labels[-1]) == 2 and labels[-2] in GENERIC_SECOND_LEVEL
    return ".".join(labels[-(3 if generic else 2) :])


def contacts_in_text(text: str, source: str) -> list[dict]:
    """Email addresses, and links that look like a contact or feedback page, in a document's text."""
    found = []
    for email in dict.fromkeys(EMAIL.findall(text)):
        if not JUNK_EMAIL.search(email):
            found.append({"kind": "email", "value": email.rstrip("."), "source": source})
    for url in dict.fromkeys(URL.findall(text)):
        if CONTACT_WORDS.search(urlsplit(url).path.lower()):
            found.append({"kind": "form or page", "value": url.rstrip(".,;"), "source": source})
    return found


def contacts_in_html(html: str, url: str) -> list[dict]:
    """`mailto:` links and contact-like links of a page (the saved text drops links, so read the HTML)."""
    from bs4 import BeautifulSoup

    found = []
    for anchor in BeautifulSoup(html, "lxml").find_all("a", href=True):
        href, label = anchor["href"].strip(), anchor.get_text(" ", strip=True)
        if href.lower().startswith("mailto:"):
            email = href[7:].split("?")[0].strip()
            if EMAIL.fullmatch(email) and not JUNK_EMAIL.search(email):
                found.append({"kind": "email", "value": email, "source": url, "label": label})
        elif CONTACT_WORDS.search(label.lower()) or CONTACT_WORDS.search(urlsplit(href).path.lower()):
            if not href.startswith(("#", "javascript:")):
                found.append({"kind": "form or page", "value": urljoin(url, href), "source": url, "label": label})
    return found + contacts_in_text(BeautifulSoup(html, "lxml").get_text(" "), url)


def rank(found: list[dict], producer_domains: set[str]) -> list[dict]:
    """Deduplicated, best first: a data mailbox at the producer, a producer form, other producer addresses, the rest.

    The order of reference/producer-issues.md: a form the producer set up for questions beats a mailbox of theirs
    that is not meant for data (`admin@`, a person's address).
    """
    unique: dict[tuple[str, str], dict] = {}
    for item in found:
        key = (item["kind"], item["value"].lower())
        if key in unique:
            unique[key].setdefault("also_on", []).append(item["source"])
            continue
        unique[key] = dict(item)
    for item in unique.values():
        host = item["value"].rpartition("@")[2] if item["kind"] == "email" else urlsplit(item["value"]).hostname or ""
        own = organization(host) in producer_domains
        local = item["value"].partition("@")[0].lower()
        if item["kind"] == "email":
            item["rank"] = 1 if own and DATA_MAILBOX.match(local) else (3 if own else 4)
        else:
            item["rank"] = 2 if own else 5
        item["at_producer"] = own
    return sorted(unique.values(), key=lambda item: (item["rank"], item["value"]))


def contacts(step: str) -> str:
    origins, dossier = origins_and_dossier(step)
    import fetch_docs

    domains = {
        organization(urlsplit(o[key]).hostname or "")
        for o in origins
        for key in ("url_main", "url_download")
        if o.get(key)
    }
    found: list[dict] = []
    index_path = dossier / "docs" / "index.json"
    if index_path.exists():
        for entry in json.loads(index_path.read_text()):
            if entry.get("file") and (dossier / "docs" / entry["file"]).exists():
                found += contacts_in_text((dossier / "docs" / entry["file"]).read_text(errors="ignore"), entry["url"])
    pages = list(dict.fromkeys(o["url_main"] for o in origins if o.get("url_main")))
    fetched = []
    for page in pages:
        got = fetch_docs._get(page)
        if got and "html" in got[1]:
            on_page = contacts_in_html(got[0].decode("utf-8", errors="ignore"), page)
            found += on_page
            fetched.append(f"{page}: read")
            # One hop: the producer's own contact page often holds the address the landing page only links to.
            for item in [
                i
                for i in on_page
                if i["kind"] == "form or page" and organization(urlsplit(i["value"]).hostname or "") in domains
            ][:2]:
                hop = fetch_docs._get(item["value"])
                if hop and "html" in hop[1]:
                    found += contacts_in_html(hop[0].decode("utf-8", errors="ignore"), item["value"])
                    fetched.append(f"{item['value']}: read (linked from the dataset page)")
        else:
            fetched.append(f"{page}: could not read (offer the user's Chrome, or check by hand)")

    ranked = rank(found, domains)
    out = [f"# Where to write: {step}", ""]
    out += ["## Pages read", ""] + [f"- {line}" for line in fetched]
    out += [
        f"- saved documentation: `{index_path.parent}`"
        if index_path.exists()
        else "- no saved documentation: run the fact check's `prepare.py` first, it fetches the producer's documents"
    ]
    out += ["", f"## Found ({len(ranked)}), best first", ""]
    labels = {
        1: "data mailbox at the producer",
        2: "producer form or contact page",
        3: "other producer address",
        4: "other address (an author? check the page)",
        5: "other form or page",
    }
    out += [f"- {labels[i['rank']]}: {i['value']} (on {i['source']})" for i in ranked] or ["- nothing found"]
    out += ["", "## Authors, for when no contact is published", ""]
    for origin in origins:
        out += [f"- {origin.get('producer', '?')}: {origin.get('citation_full', '(no citation_full)')}"]
    out += [
        "",
        "Look for an author's address only where they publish it themselves (their staff page, the paper's "
        "corresponding-author line, ORCID), at their organization's domain. Never build one from a name pattern, "
        "and always record the page it came from.",
        "",
        "Compare these with OWID's Notion producer contacts table (`NOTION_DATA_PRODUCERS_CONTACTS_TABLE_URL` in "
        "`.env`): suggest adding any contact it lacks, and add it only on the user's yes.",
        "",
    ]
    return "\n".join(out)


def origins_and_dossier(step: str) -> tuple[list[dict], Path]:
    """The step's snapshot origins and its fact-check dossier, via the fact check's own helpers."""
    sys.path.insert(0, str(HERE.parents[1] / "fact-check-dataset" / "scripts"))
    import prepare

    from etl.dag_helpers import load_dag

    dag = load_dag()
    namespace, version, short_name = prepare.parse_step(step, dag)
    chain, siblings = prepare.pipeline(dag, namespace, version, short_name)
    files = prepare.step_files(chain + siblings, namespace, version, short_name)
    return prepare.all_origins(dag, chain, files, []), prepare.dossier_dir(namespace, version, short_name)


# ----------------------------------------------------------------------------------------------------------------
# Earlier reports
# ----------------------------------------------------------------------------------------------------------------


def history(step: str) -> str:
    """What the repo and GitHub already record about reports to this producer."""
    origins, _ = origins_and_dossier(step)
    from etl import paths

    namespace, _, short_name = step.split("/")[-3:]
    producers = sorted({str(o.get("producer")) for o in origins if o.get("producer")})
    out = [f"# Earlier reports: {step}", "", "## Corrections already reported", ""]
    reported = []
    for path in sorted((paths.STEPS_DATA_DIR / "garden" / namespace).glob(f"*/{short_name}.corrections.yml")):
        for line in path.read_text().splitlines():
            if re.search(r"^\s*(reported|status|reason):", line):
                reported.append(f"- `{path.relative_to(paths.BASE_DIR)}`: {line.strip()}")
    out += reported or ["- none in any version's `corrections.yml`"]
    terms = [short_name, *producers]
    out += ["", "## owid-issues (open and closed)", ""]
    out += gh(
        [
            "issue",
            "list",
            "-R",
            "owid/owid-issues",
            "--state",
            "all",
            "--limit",
            "30",
            "--search",
            " OR ".join(f'"{t}"' for t in terms),
            "--json",
            "number,title,state,url",
        ]
    )
    out += ["", "## Earlier PRs of this dataset", ""]
    out += gh(
        [
            "pr",
            "list",
            "-R",
            "owid/etl",
            "--state",
            "all",
            "--limit",
            "10",
            "--search",
            f"{short_name} in:title",
            "--json",
            "number,title,state,url",
        ]
    )
    out += [
        "",
        "Read the bodies of those PRs for producer questions left open (`gh pr view <n> --json body`), and the "
        "issues for reports already made. The topic page's issues need a search by topic name too. Slack and "
        "Gmail are searched through their connectors (reference/producer-issues.md).",
        "",
    ]
    return "\n".join(out)


def gh(arguments: list[str]) -> list[str]:
    try:
        result = subprocess.run(["gh", *arguments], capture_output=True, text=True, timeout=60)
    except (OSError, subprocess.TimeoutExpired) as error:
        return [f"- FAILED ({error}): run `gh {' '.join(arguments)}` by hand"]
    if result.returncode != 0:
        return [f"- FAILED ({result.stderr.strip()[:200]}): run `gh {' '.join(arguments)}` by hand"]
    rows = json.loads(result.stdout or "[]")
    return [f"- #{r['number']} [{r['state'].lower()}] {r['title']}: {r['url']}" for r in rows] or ["- none found"]


# ----------------------------------------------------------------------------------------------------------------
# Template
# ----------------------------------------------------------------------------------------------------------------

TEMPLATE = """# Problems found in the producer's data during this update. Format and rules: update-dataset/reference/producer-issues.md
dataset: {step}
producer: {producer}
title: {title}
release: {release}
issues: []
# - id: 1
#   kind: error                 # error | suspected | question
#   found_by: fact check, values lane
#   summary: One sentence, in the producer's terms.
#   where: {{file: <file or sheet>, indicator: <producer's code or name>, entity: <country>, period: <year>}}
#   values: {{ours: <value>, expected: <value>, independent: <value and source>}}
#   evidence:
#     - <link>: <what it shows>
#   in_raw_snapshot: true       # true (theirs) | false (ours: fix the step) | unknown
#   docs_checked:               # every kind, or `document: not published`
#     - {{kind: methodology, document: <url>, looked_for: <terms or section>, result: nothing on it}}
#     - {{kind: codebook, document: <url>, looked_for: ..., result: ...}}
#     - {{kind: country_notes, document: not published}}
#     - {{kind: release_notes, document: <url>, looked_for: ..., result: ...}}
#     - {{kind: file_notes, document: <file and sheet or column>, looked_for: ..., result: ...}}
#   explained: no               # no | partly | yes
#   explanation: <quote, document and section; required when partly or yes>
#   our_handling: corrections.yml entry | kept as shipped | flagged in metadata
#   previously_reported: none   # or where and when (corrections.yml `reported:`, an issue, an email)
#   status: open                # open | sent | answered | dropped
"""


def template(short_name: str, step: str) -> str:
    path = workbench(short_name) / LOG
    if path.exists():
        return f"{path} exists already: add entries to it."
    origins, _ = origins_and_dossier(step)
    first = origins[0] if origins else {}
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        TEMPLATE.format(
            step=step,
            producer=json.dumps(first.get("producer", "")),
            title=json.dumps(first.get("title", "")),
            release=json.dumps(str(first.get("version_producer") or first.get("date_published") or "")),
        )
    )
    return f"Started {path}."


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = parser.add_subparsers(dest="command", required=True)
    start = commands.add_parser("template", help="start the log")
    start.add_argument("short_name")
    start.add_argument("step", help="<namespace>/<version>/<short_name>")
    commands.add_parser("check", help="list ready and held-back entries").add_argument("short_name")
    commands.add_parser("contacts", help="where to write").add_argument("step")
    commands.add_parser("history", help="earlier reports").add_argument("step")
    message = commands.add_parser("draft", help="the message skeleton, from ready entries")
    message.add_argument("short_name")
    message.add_argument("--ids", help="comma-separated entry ids (default: every ready entry)")
    args = parser.parse_args()

    if args.command == "template":
        print(template(args.short_name, args.step))
    elif args.command == "check":
        print(check(load_log(workbench(args.short_name) / LOG))[0])
    elif args.command == "contacts":
        print(contacts(args.step))
    elif args.command == "history":
        print(history(args.step))
    else:
        text = draft(load_log(workbench(args.short_name) / LOG), args.ids.split(",") if args.ids else None)
        path = workbench(args.short_name) / MESSAGE
        path.write_text(text)
        print(text + f"\nSaved to {path}.")


if __name__ == "__main__":
    main()
