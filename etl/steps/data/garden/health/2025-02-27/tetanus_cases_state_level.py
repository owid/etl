"""Load a meadow dataset and create a garden dataset."""

import pandas as pd
from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("tetanus_cases_state_level")
    ds_us_pop = paths.load_dataset("us_state_population")
    # Read table from meadow dataset.
    tb = ds_meadow.read("tetanus_cases_state_level")
    tb_pop = ds_us_pop.read("us_state_population")
    #
    # Process data.
    #
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path, country_col="countryname")
    # Clean up the data
    tb = clean_project_tycho(tb)
    tb = tb.groupby(["countryname", "state", "year"])["countvalue"].sum().reset_index()
    tb = tb.merge(tb_pop, left_on=["state", "year"], right_on=["state", "year"], how="left")
    tb["case_rate"] = tb["countvalue"] / tb["population"] * 100000
    tb = tb.drop(columns=["countryname", "population"])
    tb = tb.rename(columns={"state": "country", "countvalue": "case_count"})
    tb = tb.format(["country", "year"], short_name="tetanus_cases_state_level")
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def clean_project_tycho(tb: Table) -> Table:
    tb["state"] = tb["admin1name"].str.title()
    tb["periodstartdate"] = pd.to_datetime(tb["periodstartdate"], errors="coerce")
    tb["periodenddate"] = pd.to_datetime(tb["periodenddate"], errors="coerce")

    tb["diff"] = tb["periodenddate"] - tb["periodstartdate"]
    # Check we are using the correct data, we are expecting a 6 day difference
    assert all(tb["diff"] == "6 days")
    tb["year"] = tb["periodstartdate"].dt.year
    # Standardize the state names
    tb["state"] = tb["state"].replace({"District Of Columbia": "District of Columbia"})
    return tb
