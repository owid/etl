"""The digest's message shape and, above all, who each finding gets addressed to.

The addressing rule is the part worth a test: a data-level finding must never fall back to the
chart's last editor. A wrong column is not the business of whoever last touched the chart, and a
digest whose mentions land on the wrong person is one whose mentions stop meaning anything.
"""

from apps.chart_critic import digest


def _result(slug: str, kind: str, claim: str = "Something is off") -> dict:
    return {
        "slug": slug,
        "views": 3650,
        "cost": 0.01,
        "status": "ok",
        "summary": f"title: Chart {slug}",
        "issues": [{"severity": "medium", "kind": kind, "confidence": "high", "claim": claim}],
    }


def _format(results: list[dict], facts: dict) -> list[str]:
    return digest.format_slack(
        digest.new_findings(results, {}, facts), reviewed=len(results), candidates=len(results), facts=facts
    )


def test_one_message_per_finding_after_the_lead():
    results = [_result("a", "chart", "Claim A"), _result("b", "chart", "Claim B")]
    messages = _format(results, {})
    assert len(messages) == 3
    assert "reviewed 2, 2 worth a look" in messages[0]
    assert "Claim A" in messages[1] and "Claim B" not in messages[1]


def test_no_findings_is_no_messages():
    assert digest.format_slack([], reviewed=5, candidates=5) == []


def test_chart_level_finding_names_the_last_editor():
    facts = {"a": {"chart_id": 1, "indicators": [], "editor_mention": "<@U1>", "owner_mentions": ["<@U2>"]}}
    (_, message) = _format([_result("a", "chart")], facts)
    assert "last edited by <@U1>" in message
    assert "<@U2>" not in message


def test_data_level_finding_names_the_dataset_owners_and_never_the_editor():
    facts = {"a": {"chart_id": 1, "indicators": [], "editor_mention": "<@U1>", "owner_mentions": ["<@U2>", "<@U3>"]}}
    (_, message) = _format([_result("a", "data")], facts)
    assert "dataset owners <@U2> <@U3>" in message
    assert "<@U1>" not in message


def test_a_finding_with_nobody_to_name_names_nobody():
    facts = {"a": {"chart_id": 1, "indicators": [], "editor_mention": None, "owner_mentions": []}}
    (_, message) = _format([_result("a", "data")], facts)
    assert "owner" not in message and "last edited" not in message
    assert "Edit chart>" in message
