"""Tests for restricting the indicator-upgrader mapping to the versions actually being compared."""

import pandas as pd

from apps.wizard.app_pages.anomalist.utils import keep_mapping_for_compared_versions

# One dataset (un_sdg) updated three times: every generation of old indicators points at the newest
# dataset, which is what the upgrader's chain composition produces.
VARMAP = pd.DataFrame(
    {
        "id_old": [792720, 991059, 1120844, 792725, 991064, 1120849, 5001],
        "id_new": [1295940, 1295940, 1295940, 1295945, 1295945, 1295945, 6001],
        "dataset_id_old": [6193, 6761, 7278, 6193, 6761, 7278, 4001],
        "dataset_id_new": [8087, 8087, 8087, 8087, 8087, 8087, 5002],
    }
)


def test_only_the_replaced_version_survives() -> None:
    kept = keep_mapping_for_compared_versions(VARMAP, {8087: 7278})

    # The 2023-08-16 (6193) and 2024-08-27 (6761) generations are dropped; only the version this
    # update replaces is kept, one old indicator per new one.
    assert kept["dataset_id_old"].unique().tolist() == [7278]
    assert kept.set_index("id_old")["id_new"].to_dict() == {1120844: 1295940, 1120849: 1295945}


def test_the_regression_this_guards() -> None:
    """Unfiltered, each new indicator collects three old ones and the detectors OR across them."""
    unfiltered = VARMAP.set_index("id_old")["id_new"].to_dict()
    assert len([old for old, new in unfiltered.items() if new == 1295940]) == 3

    kept = keep_mapping_for_compared_versions(VARMAP, {8087: 7278})
    filtered = kept.set_index("id_old")["id_new"].to_dict()
    assert len([old for old, new in filtered.items() if new == 1295940]) == 1


def test_unrelated_dataset_pairs_are_dropped() -> None:
    """A mapping row for a dataset we are not comparing must not reach the detectors."""
    kept = keep_mapping_for_compared_versions(VARMAP, {8087: 7278})

    assert 5001 not in set(kept["id_old"])


def test_several_datasets_compared_at_once() -> None:
    kept = keep_mapping_for_compared_versions(VARMAP, {8087: 7278, 5002: 4001})

    assert sorted(kept["dataset_id_old"].unique().tolist()) == [4001, 7278]
    assert 5001 in set(kept["id_old"])


def test_passed_through_when_there_is_nothing_to_filter_by() -> None:
    """Without known previous versions we cannot tell generations apart, so keep the mapping as-is."""
    assert len(keep_mapping_for_compared_versions(VARMAP, None)) == len(VARMAP)
    assert len(keep_mapping_for_compared_versions(VARMAP, {})) == len(VARMAP)
    # A new dataset with no previous version gives nothing to compare against.
    assert len(keep_mapping_for_compared_versions(VARMAP, {8087: None})) == len(VARMAP)


def test_empty_mapping() -> None:
    empty = VARMAP.iloc[0:0]

    assert keep_mapping_for_compared_versions(empty, {8087: 7278}).empty
