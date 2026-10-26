"""Which bundles a sweep is allowed to replay, and how its passes merge."""

from apps.chart_critic import cache
from apps.chart_critic.cli import _merge, _resolve_cache_ttl


def test_a_changed_since_sweep_refetches_by_default():
    # The charts are selected because they changed inside the window a cached bundle covers.
    assert _resolve_cache_ttl(None, 1) == 0.0


def test_an_ordinary_sweep_keeps_the_default_ttl():
    assert _resolve_cache_ttl(None, None) == cache.DEFAULT_TTL_HOURS


def test_an_explicit_ttl_always_wins():
    assert _resolve_cache_ttl(72.0, 1) == 72.0
    assert _resolve_cache_ttl(-1.0, 1) == -1.0


def test_a_merged_finding_keeps_the_highest_severity_any_pass_gave():
    """Severity decides whether the digest posts it, so it must not depend on which pass came first."""
    found: list = []
    for severity in ["low", "medium", "low"]:
        _merge(found, {"kind": "data", "claim": "Malta's 1981 value is implausibly high.", "severity": severity})
    assert [(i["severity"], i["passes"]) for i in found] == [("medium", 3)]
