import yaml
from owid import catalog

from etl.helpers import Names

N = Names(__file__)


def run(dest_dir: str) -> None:
    # get dataset
    dataset = catalog.Dataset.create_empty(dest_dir, N.garden_dataset.metadata)

    # get tables
    tb = N.garden_dataset["life_expectancy"]
    tb_historical = N.garden_dataset["historical"]
    tb_projection = N.garden_dataset["projection"]

    # add tables
    dataset.add(tb)
    dataset.add(tb_historical)
    dataset.add(tb_projection)

    # add anomalies text
    dataset = add_anomalies_text(dataset)

    # save table
    dataset.save()


def add_anomalies_text(ds: catalog.Dataset):
    """Add anomalies text to dataset description.

    This is added to the end of dataset's description."""
    anomalies_text = build_anomalies_text()
    ds.metadata.description = ds.metadata.description + "\n" + anomalies_text
    return ds


def build_anomalies_text():
    """Build anomaly text.

    Expected input is a table called '_anomalies', which just has one cell with a YAML file raw content as a string.

    Expected YAML format:

        {
            "country1": [
                {"name": "event1", "link": "https://example.com"},
                {"name": "event2", "link": "https://example.com"},
                ...
            ],
            "country2": [
                ...
            ]
            ...
        }

    The generated text is (in html):

        <b>ANOMALIES</b>
        Find below a list of events by country and year that likely affected the life expectancy.
        <b>country1</b>
        <ul>
            <li><a href="https://example.com">event1</a></li>
            <li><a href="https://example.com">event2</a></li>
            ...
        </ul>
        ...
    """
    tb = N.garden_dataset["_anomalies"]
    # load anomalies as YAML
    anomalies_all = yaml.safe_load(tb.loc[0, "anomalies"])
    # build anomalies text
    anomalies_text = (
        "<b>ANOMALIES</b>\nFind below a list of events by country and year that likely affected the life expectancy.\n"
    )
    for country, anomalies in anomalies_all.items():
        anomalies_text += f"\n<b>{country}</b>"
        anomalies_list = [f"- <a href='{anomaly['link']}'>{anomaly['name']}</a>" for anomaly in anomalies]
        anomalies_list = f"<ul>{''.join(anomalies_list)}</ul>"
        anomalies_text += anomalies_list
    return anomalies_text
