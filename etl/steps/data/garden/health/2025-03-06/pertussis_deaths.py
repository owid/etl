"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table
from owid.catalog import processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("pertussis_deaths", namespace="us_census_bureau")
    ds_who = paths.load_dataset("mortality_database_vaccine_preventable")
    ds_population = paths.load_dataset("population")
    # Read table from meadow dataset.
    tb = ds_meadow.read("pertussis_deaths")
    tb_who = ds_who.read("mortality_database_vaccine_preventable", reset_metadata="keep_origins")

    tb_who = clean_who_mortality_data(tb_who, cause="Pertussis")
    tb_pop = ds_population.read("population", reset_metadata="keep_origins")

    tb = pr.concat([tb, tb_who], short_name="pertussis_deaths", ignore_index=True)

    tb = pr.merge(
        tb,
        tb_pop,
        on=["country", "year"],
        how="left",
    )
    tb["death_rate"] = tb["deaths"] / tb["population"] * 1000000
    tb = tb.drop(columns=["population", "source_x", "source_y", "world_pop_share"])
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()


def clean_who_mortality_data(tb: Table, cause: str) -> Table:
    tb = tb[
        (tb["cause"] == cause)
        & (tb["age_group"] == "all ages")
        & (tb["country"] == "United States")
        & (tb["sex"] == "Both sexes")
    ]  # type: ignore
    assert tb.shape[0] > 1
    tb = tb.drop(
        columns=[
            "sex",
            "age_group",
            "cause",
            "icd10_codes",
            "percentage_of_cause_specific_deaths_out_of_total_deaths",
            "age_standardized_death_rate_per_100_000_standard_population",
            "death_rate_per_100_000_population",
        ]
    )
    tb = tb.rename(columns={"number": "deaths"})

    return tb
