from etl.collections.explorer import Explorer

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
                {"slug": "confirmed_and_suspected_cases", "name": "Confirmed and suspected cases"},
                {"slug": "confirmed_deaths", "name": "Confirmed deaths"},
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
    ],
}


def test_from_dict():
    _ = Explorer.from_dict(EXPLORER_CONFIG)
