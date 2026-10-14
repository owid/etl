"""What the model is told about a chart's indicators."""

from apps.chart_critic.bundle import indicator_lines


def test_a_repeated_value_points_at_the_first_indicator_that_carried_it():
    columns = {
        "_2026": {"titleShort": "2026", "descriptionProcessing": "Converted from kelvin.", "unit": "°C"},
        "_2025": {"titleShort": "2025", "descriptionProcessing": "Converted from kelvin.", "unit": "°C"},
    }
    first, second = indicator_lines(columns)
    assert "descriptionProcessing: Converted from kelvin." in first
    assert "descriptionProcessing: same as [_2026]" in second
    assert "unit: same as [_2026]" in second
    # Values that differ are always written out.
    assert "titleShort: 2025" in second


def test_the_same_text_in_different_fields_is_not_a_repeat():
    columns = {
        "a": {"unit": "%"},
        "b": {"shortUnit": "%"},
    }
    assert indicator_lines(columns)[1] == "indicator [b]\n    shortUnit: %"
