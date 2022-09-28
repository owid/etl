from owid import catalog

from etl.helpers import Names

# Naming conventions.
N = Names(__file__)


def run(dest_dir: str) -> None:
    # Create a new grapher dataset.
    dataset = catalog.Dataset.create_empty(dest_dir, N.garden_dataset.metadata)

    # Load tables from dataset and change the "origin" column to act as if it was the country name.
    # This is a workaround to be able to visualize all curves of origin together in a line chart.
    table_1p5celsius = (
        N.garden_dataset["co2_mitigation_curves_1p5celsius"].reset_index().rename(columns={"origin": "entity"})
    )
    table_2celsius = (
        N.garden_dataset["co2_mitigation_curves_2celsius"].reset_index().rename(columns={"origin": "entity"})
    )

    # Add tables to grapher dataset and save it.
    dataset.add(table_1p5celsius)
    dataset.add(table_2celsius)
    dataset.save()
