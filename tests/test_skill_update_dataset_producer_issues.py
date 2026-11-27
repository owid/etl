"""Guards for the `update-dataset` skill's `producer_issues.py` script.

Nothing under `.claude/` is linted or collected by CI, so these tests pin the rules a message to a producer depends
on: an entry reaches a draft only when the problem is the producer's and their documentation was searched for an
explanation; an explained entry is never sent; contacts are only ever read, never invented. All data is synthetic
and nothing here touches the network.
"""

import importlib.util
import sys
from pathlib import Path

import pytest

RELATIVE = Path(".claude/skills/update-dataset/scripts/producer_issues.py")


def _load_module():
    for directory in [Path(__file__).resolve(), *Path(__file__).resolve().parents]:
        candidate = directory / RELATIVE
        if candidate.is_file():
            spec = importlib.util.spec_from_file_location("producer_issues", candidate)
            assert spec and spec.loader
            module = importlib.util.module_from_spec(spec)
            sys.modules["producer_issues"] = module
            spec.loader.exec_module(module)
            return module
    raise AssertionError(f"could not locate {RELATIVE} from {__file__}")


pi = _load_module()


def _docs(**overrides) -> list[dict]:
    entries = {
        "methodology": {"document": "https://example.int/methods.pdf", "looked_for": "Chad 2023", "result": "nothing"},
        "codebook": {"document": "https://example.int/codebook", "looked_for": "SP.POP", "result": "nothing"},
        "country_notes": {"document": "not published"},
        "release_notes": {"document": "https://example.int/news", "looked_for": "revision", "result": "nothing"},
        "file_notes": {"document": "data.xlsx, Notes sheet", "looked_for": "Chad", "result": "no footnote"},
    }
    entries.update(overrides)
    return [{"kind": kind, **entry} for kind, entry in entries.items() if entry is not None]


def _issue(**overrides) -> dict:
    issue = {
        "id": 1,
        "kind": "error",
        "summary": "Chad's 2023 population is ten times the 2022 value.",
        "where": {"file": "data.csv", "indicator": "SP.POP.TOTL", "entity": "Chad", "period": 2023},
        "values": {"ours": 180_000_000, "expected": 18_000_000},
        "in_raw_snapshot": True,
        "docs_checked": _docs(),
        "explained": "no",
        "previously_reported": "none",
        "status": "open",
    }
    issue.update(overrides)
    return issue


# ----------------------------------------------------------------------------------------------------------------
# The gate
# ----------------------------------------------------------------------------------------------------------------


def test_complete_unexplained_source_issue_is_ready():
    assert pi.verdict(_issue()) == ("ready", [])


def test_every_kind_of_documentation_must_be_checked():
    state, reasons = pi.verdict(_issue(docs_checked=_docs(country_notes=None, file_notes=None)))
    assert state == "held"
    assert any("country_notes" in r and "file_notes" in r for r in reasons)


def test_no_documentation_check_at_all_holds_the_entry():
    state, reasons = pi.verdict(_issue(docs_checked=[]))
    assert state == "held" and any("documentation not checked" in r for r in reasons)


def test_a_checked_document_must_say_what_was_looked_for_and_found():
    state, reasons = pi.verdict(_issue(docs_checked=_docs(codebook={"document": "https://example.int/cb"})))
    assert state == "held" and any("codebook" in r and "looked_for" in r for r in reasons)


def test_not_published_documentation_counts_as_checked():
    docs = _docs(**{kind: {"document": "not published"} for kind in pi.DOC_KINDS})
    assert pi.verdict(_issue(docs_checked=docs))[0] == "ready"


def test_explained_issue_is_never_sent():
    state, reasons = pi.verdict(_issue(explained="yes", explanation="Methods p. 4: Chad rebased to the 2023 census."))
    assert state == "explained" and "not sent" in reasons[0]


def test_explained_or_partly_needs_the_quote():
    assert pi.verdict(_issue(explained="yes"))[0] == "held"
    assert pi.verdict(_issue(explained="partly"))[0] == "held"
    assert pi.verdict(_issue(explained="partly", explanation="Methods p. 4 mentions a rebase."))[0] == "ready"


def test_unanswered_explained_field_holds_the_entry():
    assert pi.verdict(_issue(explained=None))[0] == "held"


def test_bare_yaml_no_and_yes_are_read_as_written():
    """YAML parses `explained: no` as False: it must still mean "no", and `yes` must still hold back the entry."""
    issue = pi.yaml.safe_load("explained: no\n") | {k: v for k, v in _issue().items() if k != "explained"}
    assert issue["explained"] is False and pi.verdict(issue)[0] == "ready"
    assert pi.verdict(_issue(explained=True, explanation="Methods p. 4."))[0] == "explained"


def test_earlier_reports_must_be_searched_before_an_entry_is_ready():
    """A missing `previously_reported` means the history search did not run, not that nothing was reported."""
    issue = {k: v for k, v in _issue().items() if k != "previously_reported"}
    state, reasons = pi.verdict(issue)
    assert state == "held" and any("previously_reported" in r for r in reasons)
    assert pi.verdict(_issue(previously_reported=None))[0] == "held"
    assert pi.verdict(_issue(previously_reported="issue owid-issues#1, 2025-03"))[0] == "ready"
    with pytest.raises(SystemExit, match="#1: held"):
        pi.draft({"issues": [issue]}, ["1"])


def test_our_own_error_is_never_sent():
    state, reasons = pi.verdict(_issue(in_raw_snapshot=False))
    assert state == "ours" and "fix the step" in reasons[0]


def test_unattributed_issue_is_held():
    state, reasons = pi.verdict(_issue(in_raw_snapshot="unknown"))
    assert state == "held" and any("raw snapshot" in r for r in reasons)


def test_closed_statuses_leave_the_ready_list():
    for status in ("sent", "answered", "dropped"):
        assert pi.verdict(_issue(status=status))[0] == status
    assert pi.verdict(_issue(status="pending"))[0] == "held"


def test_check_lists_every_entry_with_its_state():
    log = {"dataset": "x/2026-01-01/y", "issues": [_issue(), _issue(id=2, in_raw_snapshot=False)]}
    text, rows = pi.check(log)
    assert [row["state"] for row in rows] == ["ready", "ours"]
    assert "## ready (1)" in text and "## ours (1)" in text


def test_empty_log_completes_the_step():
    text, rows = pi.check({"issues": []})
    assert rows == [] and "nothing to send" in text


# ----------------------------------------------------------------------------------------------------------------
# The draft
# ----------------------------------------------------------------------------------------------------------------


def test_draft_takes_only_ready_entries_by_default():
    log = {"dataset": "x/v/y", "issues": [_issue(), _issue(id=2, explained="yes", explanation="Methods p. 4.")]}
    text = pi.draft(log, None)
    assert "Chad's 2023 population" in text and "#1" in text and "#2" not in text
    assert "Methods p. 4" not in text


def test_draft_refuses_an_entry_that_is_not_ready():
    log = {"issues": [_issue(), _issue(id=2, docs_checked=[])]}
    with pytest.raises(SystemExit, match="#2: held"):
        pi.draft(log, ["1", "2"])


def test_draft_refuses_when_nothing_is_ready():
    with pytest.raises(SystemExit, match="No entry is ready"):
        pi.draft({"issues": [_issue(in_raw_snapshot=False)]}, None)


def test_draft_quotes_partial_explanations_and_marks_follow_ups():
    issue = _issue(explained="partly", explanation="Methods p. 4: a rebase.", previously_reported="email 2025-03-01")
    text = pi.draft({"issues": [issue]}, None)
    assert "Your documentation says: Methods p. 4: a rebase." in text
    assert "Raised before: email 2025-03-01" in text


def test_repeated_ids_are_refused_before_drafting():
    """Two entries with one `id` would leave only the last in a draft keyed by id."""
    log = {"issues": [_issue(), _issue(summary="Peru's 2023 value is missing.")]}
    with pytest.raises(SystemExit, match="used more than once: #1"):
        pi.draft(log, None)
    text, rows = pi.check(log)
    assert len(rows) == 2 and "Repeated ids: #1" in text
    assert pi.duplicate_ids([_issue(), _issue(id=2)]) == []
    assert pi.duplicate_ids([{"summary": "a"}, {"id": 1}]) == ["#1"]


def test_draft_lists_what_was_checked_but_not_unpublished_documents():
    text = pi.draft({"issues": [_issue()]}, None)
    assert "https://example.int/methods.pdf" in text and "not published" not in text


# ----------------------------------------------------------------------------------------------------------------
# Contacts
# ----------------------------------------------------------------------------------------------------------------


def test_organization_domain():
    assert pi.organization("datacatalog.worldbank.org") == "worldbank.org"
    assert pi.organization("www.ons.gov.uk") == "ons.gov.uk"
    assert pi.organization("lse.ac.uk") == "lse.ac.uk"
    # Agencies under a generic second level of a country domain stay apart.
    assert pi.organization("www.stats.govt.nz") == "stats.govt.nz"
    assert pi.organization("health.govt.nz") == "health.govt.nz"
    assert pi.organization("www.ine.gob.cl") == "ine.gob.cl"
    assert pi.organization("insee.fr") == "insee.fr"


def test_contacts_in_text_finds_emails_and_contact_pages_only():
    text = (
        "Write to data@producer.org or see https://producer.org/contact-us. Logo logo@2x.png, "
        "noreply@producer.org, https://producer.org/methods.pdf"
    )
    found = {(item["kind"], item["value"]) for item in pi.contacts_in_text(text, "page")}
    assert found == {("email", "data@producer.org"), ("form or page", "https://producer.org/contact-us")}


def test_contacts_in_html_reads_mailto_links_that_the_saved_text_drops():
    html = (
        '<a href="mailto:stats@producer.org?subject=Hi">Email us</a>'
        '<a href="/feedback">Send feedback</a><a href="/methods">Methods</a><a href="#contact">Contact</a>'
    )
    found = {(i["kind"], i["value"]) for i in pi.contacts_in_html(html, "https://producer.org/data")}
    assert ("email", "stats@producer.org") in found
    assert ("form or page", "https://producer.org/feedback") in found
    assert not any("methods" in value or "#" in value for _, value in found)


def test_rank_prefers_a_data_mailbox_then_a_producer_form_and_never_adds_addresses():
    found = [
        {"kind": "email", "value": "jane.doe@university.edu", "source": "paper"},
        {"kind": "form or page", "value": "https://producer.org/contact", "source": "page"},
        {"kind": "email", "value": "admin@producer.org", "source": "page"},
        {"kind": "email", "value": "data@producer.org", "source": "page"},
        {"kind": "email", "value": "DATA@producer.org", "source": "codebook"},
    ]
    ranked = pi.rank(found, {"producer.org"})
    assert [item["value"] for item in ranked] == [
        "data@producer.org",
        "https://producer.org/contact",
        "admin@producer.org",
        "jane.doe@university.edu",
    ]
    assert ranked[0]["also_on"] == ["codebook"]
    assert {item["value"].lower() for item in ranked} <= {item["value"].lower() for item in found}
