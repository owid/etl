"""The digest's message shape and who each finding gets addressed to."""

from apps.chart_critic import digest


def _result(slug: str, kind: str, claim: str = "Something is off") -> dict:
    return {
        "slug": slug,
        "views": 3650,
        "cost": 0.01,
        "status": "ok",
        "summary": f"title: Chart {slug}",
        "issues": [
            {
                "severity": "medium",
                "kind": kind,
                "confidence": "high",
                "claim": claim,
                "evidence": "The 1913 value is 104.2.",
                "reader_impact": "A reader would read an impossible share as real.",
            }
        ],
    }


def _format(results: list[dict], facts: dict) -> list[digest.DigestMessage]:
    return digest.format_slack(
        digest.new_findings(results, {}, facts),
        reviewed=len(results),
        candidates=len(results),
        facts=facts,
        model="claude-opus-5",
    )


def test_one_message_per_finding_after_the_lead():
    results = [_result("a", "chart", "Claim A"), _result("b", "chart", "Claim B")]
    messages = _format(results, {})
    assert len(messages) == 3
    assert "reviewed 2, 2 worth a look" in messages[0].text
    assert "Claim A" in messages[1].text and "Claim B" not in messages[1].text


def test_no_findings_is_no_messages():
    assert digest.format_slack([], reviewed=5, candidates=5) == []


def test_a_finding_shows_the_chart_at_the_view_the_claim_is_about():
    """The picture is what a reader adjudicates the claim against, so it must be the finding's
    own view — the thumbnail endpoint honours the same query parameters the link carries."""
    result = _result("a", "chart")
    result["issues"][0]["url"] = "https://ourworldindata.org/grapher/a?country=~GBR&tab=chart"
    (_, message) = _format([result], {})
    assert "https://ourworldindata.org/grapher/thumbnail/a.png?country=~GBR&tab=chart" in message.text


def test_the_evidence_is_posted_in_the_finding_s_thread():
    """Not in the message: the parent is the decision, the reply is the argument."""
    (_, message) = _format([_result("a", "chart")], {})
    assert "The 1913 value is 104.2." not in message.text
    (reply,) = message.thread
    assert "The 1913 value is 104.2." in reply
    assert "A reader would read an impossible share as real." in reply
    # Per-finding provenance, which a lead message posted once cannot carry.
    assert "high confidence" in reply and "claude-opus-5" in reply


def test_a_finding_names_the_last_editor():
    facts = {"a": {"chart_id": 1, "indicators": [], "editor_mention": "<@U1>"}}
    (_, message) = _format([_result("a", "chart")], facts)
    assert "<@U1>   ·   _You last edited this chart._" in message.text
    # The ask and the mention share a line near the top, not the footer.
    assert message.text.splitlines().index(digest.ASK + "   <@U1>   ·   _You last edited this chart._") == 3


def test_a_finding_with_nobody_to_name_names_nobody():
    facts = {"a": {"chart_id": 1, "indicators": [], "editor_mention": None}}
    (_, message) = _format([_result("a", "data")], facts)
    assert "last edited" not in message.text
    assert digest.ASK in message.text
    assert "Edit in admin>" in message.text


def test_an_mdim_view_gets_no_edit_link():
    """There is no single chart behind a multi-dim view, so there is nothing to edit."""
    facts = {"a": {"chart_id": None, "indicators": [], "editor_mention": None}}
    (_, message) = _format([_result("a", "chart")], facts)
    assert "Edit in admin" not in message.text
    assert "View live chart" in message.text


def test_the_digest_file_shows_the_split_the_channel_will_see():
    messages = _format([_result("a", "chart")], {})
    text = digest.render(messages)
    assert digest.MESSAGE_SEPARATOR in text and digest.THREAD_SEPARATOR in text
    assert digest.post_count(messages) == 3  # lead, finding, its evidence


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


def test_a_reworded_repeat_of_a_posted_finding_is_not_posted_again():
    """The two wordings below are what the model actually said on consecutive days about
    `oil-prices-inflation-adjusted`, and the second was posted because the state key carried the
    claim's words. They are one finding."""
    yesterday = _result(
        "oil-prices-inflation-adjusted",
        "chart",
        "The subtitle states prices are in constant 2023 US$, while the indicator metadata specifies constant 2025 US$.",
    )
    today = _result(
        "oil-prices-inflation-adjusted",
        "chart",
        "The chart subtitle states prices are measured in constant 2023 US$, contradicting the indicator metadata base year of 2025.",
    )
    state = digest.stamp(digest.new_findings([yesterday], {}), {})
    assert digest.new_findings([today], state) == []


def test_two_different_findings_on_one_chart_both_get_posted():
    """Dedup is per claim, not per chart — a second, unrelated problem is still news."""
    subtitle = _result("a", "chart", "The subtitle says the values are age-standardized but they are not.")
    empty = _result("a", "chart", "The chart opens with no entity selected, so a reader sees an empty chart.")
    state = digest.stamp(digest.new_findings([subtitle], {}), {})
    assert len(digest.new_findings([empty], state)) == 1


def test_the_same_data_finding_on_a_chart_sharing_an_indicator_is_not_news():
    facts = {
        "a": {"chart_id": 1, "indicators": ["grapher/energy/energy_mix#coal_share"], "editor_mention": None},
        "b": {"chart_id": 2, "indicators": ["grapher/energy/energy_mix#coal_share"], "editor_mention": None},
    }
    claim = "The UK coal share exceeds 100% in 1913, which is impossible for a share."
    state = digest.stamp(digest.new_findings([_result("a", "data", claim)], {}, facts), {}, facts)
    reworded = "Coal share for the United Kingdom is above 100% in 1913, an impossible value for a share."
    assert digest.new_findings([_result("b", "data", reworded)], state, facts) == []


def test_the_old_state_format_still_suppresses_what_it_recorded():
    """The file on the runner is the only record of what the channel has seen, so the previous
    format — the claim's words baked into the key — is read rather than dropped."""
    legacy = {
        "oil-prices-inflation-adjusted:chart:constant-indicator-metadata-price-specifie-state-subtitle-while": "2026-09-03"
    }
    state = digest._upgrade(legacy)
    today = _result(
        "oil-prices-inflation-adjusted",
        "chart",
        "The chart subtitle states prices are measured in constant 2023 US$, contradicting the indicator metadata base year of 2025.",
    )
    assert digest.new_findings([today], state) == []
