"""Tests for the deterministic sampling of variables for anomaly detection."""

from apps.anomalist.anomalist_api import sample_variable_ids

# Variable ids as a real dataset has them: a contiguous block, assigned in indicator order.
VARIABLE_IDS = list(range(1295095, 1295095 + 9000))


def test_sample_is_deterministic() -> None:
    assert sample_variable_ids(VARIABLE_IDS, 1000) == sample_variable_ids(VARIABLE_IDS, 1000)
    # Input order must not matter either — callers build the list from a SQL result.
    assert sample_variable_ids(VARIABLE_IDS, 1000) == sample_variable_ids(list(reversed(VARIABLE_IDS)), 1000)


def test_sample_is_not_the_lowest_ids() -> None:
    """The regression this guards: ranking by `hash()` returned the n lowest ids.

    Variable ids are assigned in indicator order, so the lowest 1000 ids of un_sdg were its SDG goal
    1-10 indicators and goals 11-17 were never checked at all.
    """
    sample = sample_variable_ids(VARIABLE_IDS, 1000)

    assert sample != sorted(VARIABLE_IDS)[:1000]
    # A uniform sample of 1000 from 9000 spreads over the whole range; a contiguous block cannot.
    assert max(sample) > VARIABLE_IDS[-100]
    assert min(sample) < VARIABLE_IDS[100]


def test_sample_spreads_over_the_whole_input() -> None:
    """Every tenth of the id range should get roughly a tenth of the sample."""
    sample = sample_variable_ids(VARIABLE_IDS, 1000)
    bucket_size = len(VARIABLE_IDS) // 10

    for bucket in range(10):
        lo = VARIABLE_IDS[bucket * bucket_size]
        hi = lo + bucket_size
        n_in_bucket = len([variable_id for variable_id in sample if lo <= variable_id < hi])
        # Expected 100 per bucket; allow generous slack for a genuinely random draw.
        assert 50 < n_in_bucket < 150, f"bucket {bucket} got {n_in_bucket} of 1000"


def test_sample_returns_a_subset_of_the_right_size() -> None:
    sample = sample_variable_ids(VARIABLE_IDS, 1000)

    assert len(sample) == 1000
    assert len(set(sample)) == 1000
    assert set(sample) <= set(VARIABLE_IDS)
    assert sample == sorted(sample)


def test_nothing_to_sample() -> None:
    assert sample_variable_ids([3, 1, 2], 3) == [3, 1, 2]
    assert sample_variable_ids([3, 1, 2], 10) == [3, 1, 2]
    assert sample_variable_ids([], 10) == []
