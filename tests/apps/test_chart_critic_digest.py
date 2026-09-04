"""The digest's message shape and who each finding gets addressed to."""

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


def test_a_finding_names_the_last_editor():
    facts = {"a": {"chart_id": 1, "indicators": [], "editor_mention": "<@U1>"}}
    (_, message) = _format([_result("a", "chart")], facts)
    assert "last edited by <@U1>" in message


def test_a_finding_with_nobody_to_name_names_nobody():
    facts = {"a": {"chart_id": 1, "indicators": [], "editor_mention": None}}
    (_, message) = _format([_result("a", "data")], facts)
    assert "last edited" not in message
    assert "Edit chart>" in message


def test_mentions_are_only_attached_when_the_sweep_earns_them():
    facts = {"a": {"chart_id": 1, "indicators": [], "editor": {"name": "Pablo Rosado", "email": ""}}}
    digest.attach_mentions(facts, tag_last_editor=False)
    assert facts["a"]["editor_mention"] is None
    digest.attach_mentions(facts, tag_last_editor=True)
    # No address to look up, so the plain name stands in rather than a broken mention.
    assert facts["a"]["editor_mention"] == "Pablo Rosado"


def test_a_service_account_is_never_named():
    facts = {"a": {"chart_id": 1, "indicators": [], "editor": {"name": "ETL", "email": "etl@ourworldindata.org"}}}
    digest.attach_mentions(facts, tag_last_editor=True)
    assert facts["a"]["editor_mention"] is None


def test_a_slack_outage_falls_back_to_the_plain_name(monkeypatch):
    """Slack being unreachable must not fail the run — the footer just names the person plainly."""
    from etl import config, slack_helpers

    class Unreachable:
        def users_lookupByEmail(self, email):
            raise OSError("temporary failure in name resolution")

    monkeypatch.setattr(config, "SLACK_API_TOKEN", "xoxb-not-a-real-token")
    monkeypatch.setattr(slack_helpers, "slack_client", Unreachable())
    digest._slack_member_id.cache_clear()

    facts = {"a": {"chart_id": 1, "indicators": [], "editor": {"name": "Pablo Rosado", "email": "pablo@owid.org"}}}
    digest.attach_mentions(facts, tag_last_editor=True)
    assert facts["a"]["editor_mention"] == "Pablo Rosado"
    digest._slack_member_id.cache_clear()
