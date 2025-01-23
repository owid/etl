"""Load a meadow dataset and create a garden dataset."""

import pandas as pd

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("measles")
    ds_pop = paths.load_dataset("us_state_population")

    # Read table from meadow dataset.
    tb = ds_meadow.read("measles")
    tb_pop = ds_pop.read("us_state_population")

    #
    # Process data.
    #
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path, country_col="countryname")
    tb["state"] = tb["admin1name"].str.title()
    tb["periodstartdate"] = pd.to_datetime(tb["periodstartdate"], errors="coerce")
    tb["periodenddate"] = pd.to_datetime(tb["periodenddate"], errors="coerce")

    tb["diff"] = tb["periodenddate"] - tb["periodstartdate"]
    # Check we are using the correct data, we are expecting a 6 day difference
    assert all(tb["diff"] == "6 days")
    tb["year"] = tb["periodstartdate"].dt.year

    tb = tb.groupby(["countryname", "state", "year"])["countvalue"].sum().reset_index()
    # Combine with population
    tb = tb.merge(tb_pop, left_on=["state", "year"], right_on=["state", "year"], how="left")
    tb["case_rate"] = tb["countvalue"] / tb["population"] * 100000
    tb = tb.rename(columns={"countryname": "country"})

    # tb.metadata = metadata
    tb = tb.format(["country", "state", "year"], short_name="measles")

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
