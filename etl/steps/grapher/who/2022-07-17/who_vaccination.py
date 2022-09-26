import pandas as pd
from owid import catalog

from etl.helpers import Names
from etl.paths import DATA_DIR

N = Names(__file__)
N = Names('/Users/fionaspooner/Documents/OWID/repos/etl/etl/steps/data/grapher/who/2022-07-17/who_vaccination.py')

UNWPP = DATA_DIR / "garden/un/2022-07-11/un_wpp"


def run(dest_dir: str) -> None:
    dataset = catalog.Dataset.create_empty(dest_dir, N.garden_dataset.metadata)
    pop_one_yr = 
    table = N.garden_dataset["who_vaccination"]

    # optionally set additional dimensions
    # table = table.set_index(["sex", "income_group"], append=True)

    # if you data is in long format, check gh.long_to_wide_tables
    dataset.add(table)

    dataset.save()


def get_population_one_year_olds() -> pd.DataFrame:
    un_wpp_data = catalog.Dataset(UNWPP)
    pop = un_wpp_data['population'].reset_index()
    pop_one_yr = pop[(pop['age'] == '1') & (pop['variant'] == 'estimates') & (pop['metric'] == 'population')& (pop['sex'] == 'all')]
    pop_one_yr = pd.DataFrame(pop_one_yr)
    return pop_one_yr
