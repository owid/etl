"""Test functions in etl.data_helpers.population module."""

import pytest

from etl.data_helpers.population import _select_age_buckets

# UN WPP 2024 single-year buckets plus aggregates.
WPP_AGES = [f"{i}-{i + 4}" for i in range(0, 100, 5)] + ["100+", "all", "15+", "18+", "65+"]


def test_exact_match_aggregate():
    assert _select_age_buckets(WPP_AGES, 0, float("inf")) == ["all"]
    assert _select_age_buckets(WPP_AGES, 65, float("inf")) == ["65+"]


def test_exact_match_single_bucket():
    assert _select_age_buckets(WPP_AGES, 0, 4) == ["0-4"]


def test_aligned_multi_bucket_range():
    assert _select_age_buckets(WPP_AGES, 0, 14) == ["0-4", "5-9", "10-14"]


def test_open_ended_via_atomic_buckets():
    # 95+ is not in WPP_AGES, so [95, inf) must combine 95-99 and 100+.
    assert _select_age_buckets(WPP_AGES, 95, float("inf")) == ["95-99", "100+"]


def test_misaligned_lower_bound_raises():
    # [2, 9] cannot be covered: 0-4 starts at 0, not 2.
    with pytest.raises(ValueError, match=r"\[2, 9\]"):
        _select_age_buckets(WPP_AGES, 2, 9)


def test_misaligned_no_bucket_inside_raises():
    # [15, 17] has no bucket fully inside.
    with pytest.raises(ValueError, match=r"\[15, 17\]"):
        _select_age_buckets(WPP_AGES, 15, 17)


def test_misaligned_upper_bound_raises():
    # [0, 6] would only pick 0-4 (atomic), missing 5-6.
    with pytest.raises(ValueError, match=r"\[0, 6\]"):
        _select_age_buckets(WPP_AGES, 0, 6)
