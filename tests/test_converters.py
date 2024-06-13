from etl.steps.data.converters import convert_grapher_source


def test_convert_grapher_source():
    db_source = dict(
        name="test",
        description=dict(
            link="https://www.gapminder.org/data/",
            retrievedDate="15/11/2017",
            additionalInfo="Mortality data was compiled...",
            dataPublisherSource="Census data, IARC data",
        ),
    )

    etl_source = convert_grapher_source(db_source)
    assert etl_source.to_dict() == {
        "name": "test",
        "description": "Mortality data was compiled...\nPublisher source: Census data, IARC data",
        "url": "https://www.gapminder.org/data/",
        "date_accessed": "15/11/2017",
    }
