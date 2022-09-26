from functools import reduce

import pandas as pd
from owid import catalog
from owid.catalog import Table

from etl.helpers import Names
from etl.paths import DATA_DIR

N = Names(__file__)

UNWPP = DATA_DIR / "garden/un/2022-07-11/un_wpp"


def run(dest_dir: str) -> None:
    dataset = catalog.Dataset.create_empty(dest_dir, N.garden_dataset.metadata)
    pop_one_yr = get_population_one_year_olds()
    pop_one_yr = pop_one_yr.rename(columns={"location": "country", "value": "population"}).drop(
        columns=["metric", "variant", "sex", "age"]
    )
    table = N.garden_dataset["who_vaccination"]
    table_fin = calculate_vaccinated_unvaccinated_population(table, pop_one_yr)
    table_fin = Table(table_fin)
    table_fin.metadata = table.metadata
    dataset.add(table_fin)

    dataset.save()


def get_population_one_year_olds() -> pd.DataFrame:
    un_wpp_data = catalog.Dataset(UNWPP)
    pop = un_wpp_data["population"].reset_index()
    pop_one_yr = pop[
        (pop["age"] == "1") & (pop["variant"] == "estimates") & (pop["metric"] == "population") & (pop["sex"] == "all")
    ]
    pop_one_yr = pd.DataFrame(pop_one_yr)
    return pop_one_yr


def calculate_vaccinated_unvaccinated_population(table: Table, pop_one_yr: pd.DataFrame) -> pd.DataFrame:
    # vaccines where the coverage is measured as % of one-year olds
    vax_one_year_olds = [
        "bcg",
        "dtp_containing_vaccine__1st_dose",
        "dtp_containing_vaccine__3rd_dose",
        "hepb3",
        "hepb__birth_dose__given_within_24_hours_of_birth",
        "hib3",
        "inactivated_polio_containing_vaccine__1st_dose",
        "measles_containing_vaccine__1st_dose",
        "measles_containing_vaccine__2nd_dose",
        "pneumococcal_conjugate_vaccine__final_dose",
        "polio__3rd_dose",
        "rubella_containing_vaccine__1st_dose",
        "rotavirus__last_dose",
        "yellow_fever_vaccine",
    ]
    cov_pop = table[["country", "year"] + vax_one_year_olds].merge(pop_one_yr, on=["country", "year"])
    vax_pop = cov_pop
    vax_pop[vax_one_year_olds] = (
        vax_pop[vax_one_year_olds].multiply(0.01).multiply(vax_pop["population"], axis="index").round(0).astype("Int64")
    )
    unvax_pop = vax_pop
    vax_pop = vax_pop.rename(columns={c: c + "_vaccinated" for c in vax_pop.columns if c in vax_one_year_olds})
    vax_pop = vax_pop.drop(columns=["population"])

    unvax_pop[vax_one_year_olds] = (
        unvax_pop[vax_one_year_olds].sub(unvax_pop["population"], axis="index").multiply(-1).astype("Int64")
    )
    unvax_pop = unvax_pop.rename(columns={c: c + "_unvaccinated" for c in unvax_pop.columns if c in vax_one_year_olds})
    unvax_pop = unvax_pop.drop(columns=["population"])
    data_frames = [table, vax_pop, unvax_pop]

    df_merged = reduce(lambda left, right: pd.merge(left, right, on=["country", "year"], how="outer"), data_frames)
    return df_merged
