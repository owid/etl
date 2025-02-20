"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow_hist = paths.load_dataset("measles_deaths_historical")
    ds_who_mort = paths.load_dataset("mortality_database_vaccine_preventable")

    # Read table from meadow dataset.
    tb_hist = ds_meadow_hist.read("measles_deaths_historical")
    tb_pres = ds_who_mort.read("mortality_database_vaccine_preventable")
    tb_pres = tb_pres[
        (tb_pres["cause"] == "Measles")
        & (tb_pres["age_group"] == "all ages")
        & (tb_pres["country"] == "United States")
        & (tb_pres["sex"] == "Both sexes")
    ]
    tb_pres = tb_pres[["country", "year", "number"]]
    tb_pres = tb_pres.rename(columns={"number": "deaths"})

    tb = pr.concat([tb_hist, tb_pres], short_name="measles_deaths_long_run", ignore_index=True)
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)

    # Save changes in the new garden dataset.
    ds_garden.save()
