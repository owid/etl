from unittest.mock import patch

import pytest

from etl.collections.explorer import Explorer, extract_explorers_tables
from etl.collections.model import Dimension

# EXAMPLE explorer. Inspired by Mpox explorer.
EXPLORER_CONFIG = {
    "config": {
        "explorerTitle": "Mpox",
        "explorerSubtitle": "Explore the data produced by the World Health Organization and Africa CDC on mpox (monkeypox).",
        "selection": ["Democratic Republic of Congo", "Burundi", "Uganda", "Central African Republic"],
        "isPublished": "true",
        "thumbnail": "https://assets.ourworldindata.org/uploads/2022/05/Monkeypox-Data-Explorer.png",
        "hideAlertBanner": "true",
        "yAxisMin": "0",
        "hasMapTab": "true",
        "downloadDataLink": "https://catalog.ourworldindata.org/garden/who/latest/monkeypox/monkeypox.csv",
    },
    "dimensions": [
        {
            "slug": "metric",
            "name": "Metric",
            "choices": [
                {"slug": "confirmed_cases", "name": "Confirmed cases"},
                {"slug": "confirmed_cases_test", "name": "Confirmed cases (test)"},
                {"slug": "confirmed_and_suspected_cases", "name": "Confirmed and suspected cases"},
                {"slug": "confirmed_deaths", "name": "Confirmed deaths"},
                {"slug": "suspected_cases", "name": "Suspected cases"},
            ],
            "presentation": {"type": "radio"},
        },
        {
            "slug": "frequency",
            "name": "Frequency",
            "choices": [
                {"slug": "_7_day_average", "name": "7-day average"},
                {"slug": "cumulative", "name": "Cumulative"},
                {"slug": "daily", "name": "Daily"},
            ],
            "presentation": {"type": "radio"},
        },
        {
            "slug": "scale",
            "name": "Relative to population",
            "choices": [
                {"slug": "absolute", "name": "Total population"},
                {"slug": "relative_to_population", "name": "Relative to population"},
            ],
            "presentation": {"type": "checkbox", "choice_slug_true": "relative_to_population"},
        },
    ],
    "views": [
        {
            "dimensions": {"metric": "confirmed_cases", "frequency": "cumulative", "scale": "absolute"},
            "indicators": {
                "y": [
                    {
                        "catalogPath": "monkeypox#total_cases",
                        "display": {
                            "sourceName": "World Health Organization",
                            "sourceLink": "https://xmart-api-public.who.int/MPX/V_MPX_VALIDATED_DAILY",
                            "additionalInfo": "Data on mpox is collated by the [World Health Organization](https://extranet.who.int/publicemergency/) since 2022, and is updated as new information is reported.\\n\\nWe fetch the latest version of the WHO data every hour, keep records up to the previous day, apply some transformations (7-day averages, per-capita adjustments, etc.), and produce a transformed version of the data, [available on GitHub](https://github.com/owid/monkeypox). This transformed data powers our Mpox Data Explorer on Our World in Data.",
                            "colorScaleNumericMinValue": "0",
                            "colorScaleScheme": "OrRd",
                            "name": "Total confirmed cases",
                            "type": "Integer",
                            "tolerance": "30",
                            "colorScaleNumericBins": "100;200;500;1000;2000;5000;10000;20000;50000",
                        },
                    }
                ]
            },
            "config": {"title": "Mpox: Cumulative confirmed cases", "type": "LineChart"},
        },
        {
            "dimensions": {"metric": "confirmed_cases_test", "frequency": "cumulative", "scale": "absolute"},
            "indicators": {
                "y": [
                    {
                        "catalogPath": "monkeypox#total_cases",
                        "display": {
                            "sourceName": "World Health Organization",
                            "sourceLink": "https://example.com",
                            "additionalInfo": "Data on mpox is collated by the [World Health Organization](https://extranet.who.int/publicemergency/) since 2022, and is updated as new information is reported.\\n\\nWe fetch the latest version of the WHO data every hour, keep records up to the previous day, apply some transformations (7-day averages, per-capita adjustments, etc.), and produce a transformed version of the data, [available on GitHub](https://github.com/owid/monkeypox). This transformed data powers our Mpox Data Explorer on Our World in Data.",
                            "colorScaleNumericMinValue": "0",
                            "colorScaleScheme": "OrRd",
                            "name": "Total confirmed cases",
                            "type": "Integer",
                            "tolerance": "30",
                            "colorScaleNumericBins": "100;200;500;1000;2000;5000;10000;20000;50000",
                        },
                    }
                ]
            },
            "config": {"title": "Mpox: Cumulative confirmed cases", "type": "LineChart"},
        },
        {
            "dimensions": {"metric": "confirmed_cases", "frequency": "cumulative", "scale": "relative_to_population"},
            "indicators": {
                "y": [
                    {
                        "catalogPath": "monkeypox#total_cases_per_million",
                        "display": {
                            "sourceName": "World Health Organization",
                            "sourceLink": "https://xmart-api-public.who.int/MPX/V_MPX_VALIDATED_DAILY",
                            "additionalInfo": "Data on mpox is collated by the [World Health Organization](https://extranet.who.int/publicemergency/) since 2022, and is updated as new information is reported.\\n\\nWe fetch the latest version of the WHO data every hour, keep records up to the previous day, apply some transformations (7-day averages, per-capita adjustments, etc.), and produce a transformed version of the data, [available on GitHub](https://github.com/owid/monkeypox). This transformed data powers our Mpox Data Explorer on Our World in Data.",
                            "colorScaleNumericMinValue": "0",
                            "colorScaleScheme": "OrRd",
                            "name": "Total cases per 1M",
                            "type": "Ratio",
                            "tolerance": "30",
                            "colorScaleNumericBins": "1;2;5;10;20;50;100;200;500",
                        },
                    }
                ]
            },
            "config": {"title": "Mpox: Cumulative confirmed cases per million people", "type": "LineChart"},
        },
        {
            "dimensions": {"metric": "confirmed_and_suspected_cases", "frequency": "cumulative", "scale": "absolute"},
            "indicators": {
                "y": [
                    {
                        "catalogPath": "monkeypox#total_cases",
                        "display": {
                            "sourceName": "World Health Organization (test duplicate)",
                            "sourceLink": "https://xmart-api-public.who.int/MPX/V_MPX_VALIDATED_DAILY",
                            "additionalInfo": "Data on mpox is collated by the [World Health Organization](https://extranet.who.int/publicemergency/) since 2022, and is updated as new information is reported.\\n\\nWe fetch the latest version of the WHO data every hour, keep records up to the previous day, apply some transformations (7-day averages, per-capita adjustments, etc.), and produce a transformed version of the data, [available on GitHub](https://github.com/owid/monkeypox). This transformed data powers our Mpox Data Explorer on Our World in Data.",
                            "colorScaleNumericMinValue": "0",
                            "colorScaleScheme": "OrRd",
                            "name": "Total confirmed cases",
                            "type": "Integer",
                            "tolerance": "30",
                            "colorScaleNumericBins": "100;200;500;1000;2000;5000;10000;20000;50000",
                        },
                    },
                    {
                        "catalogPath": "monkeypox#suspected_cases_cumulative",
                        "display": {
                            "sourceName": "Global.health",
                            "sourceLink": "https://africacdc.org/resources/?wpv_aux_current_post_id=217&wpv_view_count=549&wpv-resource-type=ebs-weekly-reports",
                            "additionalInfo": "Data on suspected cases of mpox are manually compiled from reports from Africa Centres for Disease Control and Prevention (CDC).",
                            "colorScaleScheme": "OrRd",
                            "name": "Total suspected cases",
                            "type": "Integer",
                            "tolerance": "30",
                            "colorScaleNumericBins": "1;2;5;10;20;50;100",
                        },
                    },
                ]
            },
            "config": {
                "title": "Mpox: Cumulative confirmed and suspected cases",
                "subtitle": "Confirmed cases are those that have been verified through laboratory testing. Suspected cases are those where mpox is likely based on an individual's initial clinical signs and symptoms, but the diagnosis has not yet been confirmed through laboratory testing.",
                "type": "LineChart",
                "selectedFacetStrategy": "entity",
                "hasMapTab": "false",
                "minTime": "1433",
                "facetYDomain": "independent",
                "note": "As of November 2024, suspected cases are no longer being reported.",
            },
        },
        {
            "dimensions": {"metric": "suspected_cases", "frequency": "cumulative", "scale": "absolute"},
            "indicators": {
                "x": {
                    "catalogPath": "monkeypox#suspected_cases_cumulative",
                    "display": {
                        "sourceName": "Global.health",
                        "sourceLink": "https://africacdc.org/resources/?wpv_aux_current_post_id=217&wpv_view_count=549&wpv-resource-type=ebs-weekly-reports",
                        "additionalInfo": "Data on suspected cases of mpox are manually compiled from reports from Africa Centres for Disease Control and Prevention (CDC).",
                        "colorScaleScheme": "OrRd",
                        "name": "Total suspected cases (test)",
                        "type": "Integer",
                        "tolerance": "30",
                        "colorScaleNumericBins": "1;2;5;10;20;50;100",
                    },
                },
            },
        },
        {
            "dimensions": {"metric": "suspected_cases", "frequency": "cumulative", "scale": "relative_to_population"},
            "indicators": {
                "y": {
                    "catalogPath": "monkeypox#suspected_cases_cumulative",
                },
            },
        },
    ],
}


def test_checkbox_errors():
    # Missing choice_slug_true
    dimension = {
        "slug": "scale",
        "name": "Relative to population",
        "choices": [
            {"slug": "absolute", "name": "Total population"},
            {"slug": "relative_to_population", "name": "Relative to population"},
        ],
        "presentation": {"type": "checkbox"},
    }
    with pytest.raises(ValueError):
        _ = Dimension.from_dict(dimension)

    # More than one choice
    dimension = {
        "slug": "scale",
        "name": "Relative to population",
        "choices": [
            {"slug": "absolute", "name": "Total population"},
            {"slug": "absolute2", "name": "Total population2"},
            {"slug": "relative_to_population", "name": "Relative to population"},
        ],
        "presentation": {"type": "checkbox", "choice_slug_true": "relative_to_population"},
    }
    with pytest.raises(ValueError):
        _ = Dimension.from_dict(dimension)


def test_from_dict():
    # Read explorer from dict
    _ = Explorer.from_dict(EXPLORER_CONFIG)


def mock_map_indicator_paths_to_ids(paths):
    return [e for e, _ in enumerate(paths)]


@patch("etl.collections.explorer.map_indicator_paths_to_ids", side_effect=mock_map_indicator_paths_to_ids)
def test_explorer_config_legacy(mock_map_func):
    explorer = Explorer.from_dict(EXPLORER_CONFIG)
    assert len(explorer.views) == 6
    df_grapher, df_columns = extract_explorers_tables(explorer)

    # Output shape test
    assert df_grapher.shape[0] == 6
    assert df_columns.shape[0] == 7

    # Check that columns makes sense
    mask = df_columns["catalogPath"].notna()
    assert (df_columns[mask].index == [0, 2, 4]).all()
    assert df_columns.loc[mask, "slug"].isna().all()
    mask = df_columns["slug"].notna()
    assert (df_columns[mask].index == [1, 3, 5]).all()
    assert df_columns.loc[mask, "catalogPath"].isna().all()

    # TODO: check df_grapher
    mask = df_grapher["yVariableIds"].notna()
    assert (df_grapher[mask].index == [5]).all()
    mask = df_grapher["ySlugs"].notna()
    assert (df_grapher[mask].index == [0, 1, 2, 3]).all()
    mask = df_grapher["xSlug"].notna()
    assert (df_grapher[mask].index == [4]).all()
