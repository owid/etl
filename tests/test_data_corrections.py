"""Tests for etl.data_corrections (the .corrections.yml mechanism)."""

import pytest
from owid.catalog import Table

from etl.data_corrections import apply_corrections, load_corrections


def _make_table() -> Table:
    tb = Table(
        {
            "country": ["Panama", "Panama", "Panama", "France", "France"],
            "year": [2006, 2008, 2016, 2006, 2016],
            "value": [-1.0, -2.0, -3.0, 10.0, 20.0],
        }
    )
    # Give the indicator column some metadata so we can check it survives.
    tb["value"].metadata.unit = "tonnes"
    tb["value"].metadata.title = "Some emissions"
    return tb


def _correction(**overrides):
    base = {
        "id": "test-correction",
        "indicator": "value",
        "action": "drop",
        "reason": "Test reason.",
        "provider": "Test provider.",
        "status": "open",
    }
    base.update(overrides)
    return base


def test_drop_by_entity_and_year_list():
    tb = _make_table()
    out = apply_corrections(tb, [_correction(entity="Panama", years=[2006, 2016])])
    # Two Panama rows dropped; France untouched.
    assert list(zip(out["country"], out["year"])) == [("Panama", 2008), ("France", 2006), ("France", 2016)]
    # Index is renumbered (matching the df.drop(...).reset_index(drop=True) idiom).
    assert list(out.index) == [0, 1, 2]
    # Metadata preserved.
    assert out["value"].metadata.unit == "tonnes"


def test_drop_does_not_mutate_input():
    tb = _make_table()
    apply_corrections(tb, [_correction(entity="Panama", years=[2006])])
    assert len(tb) == 5


def test_years_latest_resolves_per_entity():
    tb = _make_table()
    out = apply_corrections(tb, [_correction(entity="Panama", years="latest")])
    # Panama's latest year is 2016.
    assert (out["country"] == "Panama").sum() == 2
    assert not ((out["country"] == "Panama") & (out["year"] == 2016)).any()


def test_years_range_from_to():
    tb = _make_table()
    out = apply_corrections(tb, [_correction(entity="Panama", years={"from": 2007, "to": 2016})])
    # Drops Panama 2008 and 2016, keeps 2006.
    assert ((out["country"] == "Panama") & (out["year"] == 2006)).any()
    assert (out["country"] == "Panama").sum() == 1


def test_years_range_after_before():
    tb = _make_table()
    out = apply_corrections(tb, [_correction(entity="France", years={"after": 2006, "before": 2017})])
    assert (out["country"] == "France").sum() == 1  # only 2016 dropped


def test_override_by_match_value():
    tb = _make_table()
    out = apply_corrections(
        tb,
        [_correction(id="ovr", action="override", match={"value": 10.0}, value=99.0)],
    )
    # France 2006 had value 10.0 → now 99.0.
    assert out.loc[(out["country"] == "France") & (out["year"] == 2006), "value"].item() == 99.0
    # Metadata preserved through override.
    assert out["value"].metadata.unit == "tonnes"


def test_override_requires_value():
    # Validation rejects an override entry without a `value`.
    from etl.data_corrections import _validate_correction

    with pytest.raises(AssertionError):
        _validate_correction(_correction(id="bad", action="override", match={"value": 10.0}), "<test>")


def test_flag_is_a_noop_on_data():
    tb = _make_table()
    out = apply_corrections(tb, [_correction(id="flagged", action="flag", entity="Panama", years=[2006])])
    assert len(out) == len(tb)


def test_unmatched_drop_raises():
    tb = _make_table()
    with pytest.raises(AssertionError, match="matched no rows"):
        apply_corrections(tb, [_correction(entity="Atlantis", years=[2006])])


def test_works_with_country_year_in_index():
    tb = _make_table().set_index(["country", "year"])
    out = apply_corrections(tb, [_correction(entity="Panama", years=[2006])])
    assert ("Panama", 2006) not in out.index
    assert len(out) == 4


def test_load_corrections_validates_and_dedupes(tmp_path):
    p = tmp_path / "x.corrections.yml"
    p.write_text(
        "- id: a\n"
        "  indicator: value\n"
        "  entity: Panama\n"
        "  years: [2006]\n"
        "  action: drop\n"
        "  reason: r\n"
        "  provider: p\n"
        "  status: open\n"
    )
    corrections = load_corrections(p)
    assert corrections[0]["id"] == "a"


def test_load_corrections_rejects_duplicate_ids(tmp_path):
    p = tmp_path / "x.corrections.yml"
    entry = (
        "- id: dup\n"
        "  indicator: value\n"
        "  entity: Panama\n"
        "  years: [2006]\n"
        "  action: drop\n"
        "  reason: r\n"
        "  provider: p\n"
        "  status: open\n"
    )
    p.write_text(entry + entry)
    with pytest.raises(AssertionError, match="Duplicate correction id"):
        load_corrections(p)


def test_load_corrections_rejects_both_entity_and_match(tmp_path):
    p = tmp_path / "x.corrections.yml"
    p.write_text(
        "- id: a\n"
        "  indicator: value\n"
        "  entity: Panama\n"
        "  years: [2006]\n"
        "  match: {value: 4.5}\n"
        "  action: drop\n"
        "  reason: r\n"
        "  provider: p\n"
        "  status: open\n"
    )
    with pytest.raises(AssertionError, match="exactly one of"):
        load_corrections(p)
