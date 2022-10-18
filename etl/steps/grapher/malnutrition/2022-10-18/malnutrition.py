from owid import catalog

from etl.helpers import Names

N = Names(__file__)
N = Names("/Users/fionaspooner/Documents/OWID/repos/etl/etl/steps/grapher/malnutrition/2022-10-18/malnutrition.py")


def run(dest_dir: str) -> None:
    dataset = catalog.Dataset.create_empty(dest_dir, N.garden_dataset.metadata)

    table = N.garden_dataset["malnutrition"]
    # optionally set additional dimensions
    # table = table.set_index(["sex", "income_group"], append=True)

    # if you data is in long format, check gh.long_to_wide_tables
    dataset.add(table)

    dataset.save()
