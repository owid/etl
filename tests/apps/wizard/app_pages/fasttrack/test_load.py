import pandas as pd
import pytest

from apps.wizard.app_pages.fasttrack.load import ValidationError, parse_data_from_sheets


def test_parse_data_from_sheets_rejects_duplicate_country_year_keys():
    data = pd.DataFrame(
        {
            "country": ["Taiwan", "Taiwan", "Zimbabwe"],
            "year": [2023, 2023, 2023],
            "value": [1, 2, 3],
        }
    )

    with pytest.raises(ValidationError, match="Duplicate keys found"):
        parse_data_from_sheets(data)


def test_parse_data_from_sheets_allows_duplicate_country_year_with_different_dimensions():
    data = pd.DataFrame(
        {
            "country": ["Taiwan", "Taiwan"],
            "year": [2023, 2023],
            "dim_scenario": ["low", "high"],
            "value": [1, 2],
        }
    )

    parsed = parse_data_from_sheets(data)

    assert parsed.index.names == ["country", "year"]
    assert list(parsed["dim_scenario"]) == ["low", "high"]
