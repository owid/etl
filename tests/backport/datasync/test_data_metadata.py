from backport.datasync.data_metadata import variable_metadata
from etl.db import get_engine


def test_variable_metadata():
    engine = get_engine()
    meta = variable_metadata(engine, 525715)

    meta["dimensions"]["years"]["values"] = meta["dimensions"]["years"]["values"][:3]
    meta["dimensions"]["entities"]["values"] = meta["dimensions"]["entities"]["values"][:3]

    assert meta == {
        "id": 525715,
        "name": "Population density",
        "unit": "people per km²",
        "description": "Population density by country, available from 10,000 BCE to 2100 based on Gapminder data, HYDE, and UN Population Division (2022) estimates. Estimated by dividing a country's population by its land area (from FAO via World Bank).\n\n10,000 BCE - 1799: Historical estimates by HYDE (v3.2).\n1800-1949: Historical estimates by Gapminder.\n1950-2021: Population records by the United Nations - Population Division (2022).\n2022-2100: Projections based on Medium variant by the United Nations - Population Division (2022).\n",
        "createdAt": "2022-09-20T12:16:46.000Z",
        "updatedAt": "2023-02-10T11:46:31.000Z",
        "coverage": "",
        "timespan": "-10000-2100",
        "datasetId": 5774,
        "columnOrder": 0,
        "shortName": "population_density",
        "catalogPath": "grapher/owid/latest/key_indicators/population_density",
        "datasetName": "Key Indicators",
        "type": "float",
        "nonRedistributable": False,
        "display": {
            "name": "Population density",
            "unit": "people per km²",
            "shortUnit": None,
            "includeInTable": True,
            "numDecimalPlaces": 1,
        },
        "source": {
            "id": 27065,
            "name": "Gapminder (v6); UN (2022); HYDE (v3.2); Food and Agriculture Organization of the United Nations",
            "dataPublishedBy": "Gapminder (v6); United Nations - Population Division (2022); HYDE (v3.2); World Bank",
            "dataPublisherSource": "",
            "link": "https://www.gapminder.org/data/documentation/gd003/ ; https://population.un.org/wpp/Download/Standard/Population/ ; https://dataportaal.pbl.nl/downloads/HYDE/ ; http://data.worldbank.org/data-catalog/world-development-indicators",
            "retrievedDate": "October 8, 2021",
            "additionalInfo": 'Our World in Data builds and maintains a long-run dataset on population by country, region, and for the world, based on three key sources: HYDE, Gapminder, and the UN World Population Prospects. This combines historical population estimates with median scenario projections to 2100. You can find more information on these sources and how our time series is constructed on this page: <a href="https://ourworldindata.org/population-sources">What sources do we rely on for population estimates?</a>\n\nWe combine this population dataset with the <a href="https://ourworldindata.org/grapher/land-area-km">land area estimates published by the World Bank</a>, to produce a long-run dataset of population density.\n\nIn all sources that we rely on, population estimates and land area estimates are based on today’s geographical borders.\n',
        },
        "dimensions": {
            "years": {"values": [{"id": -10000}, {"id": -9000}, {"id": -8000}]},
            "entities": {
                "values": [
                    {"code": None, "id": 273, "name": "Africa"},
                    {"code": None, "id": 275, "name": "Asia"},
                    {"code": None, "id": 276, "name": "Europe"},
                ]
            },
        },
    }
