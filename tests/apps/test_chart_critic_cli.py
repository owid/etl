"""Which bundles a sweep is allowed to replay."""

from apps.chart_critic import cache
from apps.chart_critic.cli import _resolve_cache_ttl


def test_a_changed_since_sweep_refetches_by_default():
    # The charts are selected because they changed inside the window a cached bundle covers.
    assert _resolve_cache_ttl(None, 1) == 0.0


def test_an_ordinary_sweep_keeps_the_default_ttl():
    assert _resolve_cache_ttl(None, None) == cache.DEFAULT_TTL_HOURS


def test_an_explicit_ttl_always_wins():
    assert _resolve_cache_ttl(72.0, 1) == 72.0
    assert _resolve_cache_ttl(-1.0, 1) == -1.0
