"""Grapher step for our Life Expectancy OMM."""
import yaml
from owid import catalog

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    # Load garden dataset
    ds_garden: catalog.Dataset = paths.load_dependency("life_expectancy", "garden")
    # get dataset
    dataset = catalog.Dataset.create_empty(dest_dir, ds_garden.metadata)

    # get tables
    tb = ds_garden["life_expectancy"]
    tb_historical = ds_garden["historical"]
    tb_projection = ds_garden["projection"]

    # add tables
    dataset.add(tb)
    dataset.add(tb_historical)
    dataset.add(tb_projection)

    # add anomalies text
    dataset = add_anomalies_text(dataset, ds_garden)
    # save table
    dataset.save()


def add_anomalies_text(ds: catalog.Dataset, ds_garden: catalog.Dataset):
    """Add anomalies text to dataset description.

    This is added to the end of dataset's description."""
    anomalies_text = build_anomalies_text(ds_garden)
    ds.metadata.description = ds.metadata.description + "\n" + anomalies_text
    return ds


def build_anomalies_text(ds_garden: catalog.Dataset) -> str:
    """Build anomaly text.

    Expected input is a table called '_hist_events', which just has one cell with a YAML file raw content as a string.

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
    tb = ds_garden["_hist_events"]
    # load historical events as YAML
    anomalies_all = yaml.safe_load(tb.loc[0, "hist_events"])
    # build historical events text
    anomalies_text = (
        "<b>DATA ANOMALIES</b>\nFind below a list of events by country and year that likely affected the life"
        " expectancy, and thus created data anomalies.\n"
    )
    for country, anomalies in anomalies_all.items():
        anomalies_text += f"\n<b>{country}</b>\n"
        anomalies_list = [f"- <a href='{anomaly['link']}'>{anomaly['name']}</a>" for anomaly in anomalies]
        anomalies_list = "\n".join(anomalies_list) + "\n"
        anomalies_text += anomalies_list
    return anomalies_text
