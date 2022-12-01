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

    # save table
    dataset.save()
