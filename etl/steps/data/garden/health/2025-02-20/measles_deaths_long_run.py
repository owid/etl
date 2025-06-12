"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    # Historical data from early 20th C to 1949
    ds_meadow_phr = paths.load_dataset("measles_deaths_public_health_reports")
    ds_meadow_cb = paths.load_dataset("measles_deaths_census_bureau")
    # Data from WHO mortality database for 1950-present
    ds_who_mort = paths.load_dataset("mortality_database_vaccine_preventable")
    # Load population data to calculate death rates.
    ds_population = paths.load_dataset("population")
    tb_pop = ds_population.read("population")
    # Read table from meadow dataset.
    tb_phr = ds_meadow_phr.read("measles_deaths_public_health_reports")
    tb_cb = ds_meadow_cb.read("measles_deaths_census_bureau")
    tb_pres = ds_who_mort.read("mortality_database_vaccine_preventable", reset_metadata="keep_origins")
    tb_pres = tb_pres[
        (tb_pres["cause"] == "Measles")
        & (tb_pres["age_group"] == "all ages")
        & (tb_pres["country"] == "United States")
        & (tb_pres["sex"] == "Both sexes")
    ]
    tb_pres = tb_pres[["country", "year", "number"]]
    tb_pres = tb_pres.rename(columns={"number": "deaths"})
    # Combine the historical and present data.
    tb = pr.concat([tb_phr, tb_cb, tb_pres], short_name="measles_deaths_long_run", ignore_index=True)

    tb = pr.merge(
        tb,
        tb_pop,
        on=["country", "year"],
        how="left",
    )
    tb["death_rate"] = tb["deaths"] / tb["population"] * 100000
    tb = tb.drop(columns=["population", "source", "world_pop_share"])
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(tables=[tb], check_variables_metadata=True)

    # Save changes in the new garden dataset.
    ds_garden.save()
