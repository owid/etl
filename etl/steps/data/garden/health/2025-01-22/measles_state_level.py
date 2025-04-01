"""Load a meadow dataset and create a garden dataset."""

import pandas as pd
from owid.catalog import Table
from owid.catalog import processing as pr

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset - the Project Tycho dataset.
    ds_meadow = paths.load_dataset("measles_state_level", namespace="health")
    # Load the fast track of the CDC archive for 2002-2015
    ds_measles_cdc_archive = paths.load_snapshot("cdc_measles")
    # Load in the CDC historical dataset for missing data < 2001
    ds_measles_cdc_historical = paths.load_snapshot("measles_state_level", namespace="cdc")
    # Load the CDC dataset for 2016-2022
    ds_measles_cdc = paths.load_dataset("measles_state_level", namespace="cdc")
    ds_pop = paths.load_dataset("us_state_population")

    # Read table from meadow dataset.
    tb = ds_meadow.read("measles_state_level")

    origins = tb["countvalue"].metadata.origins
    tb_cdc_historical = ds_measles_cdc_historical.read_csv().drop(columns=["Unnamed: 0"])
    tb_cdc_historical = tb_cdc_historical[tb_cdc_historical["cases"] != "NN"].dropna(subset=["cases"])
    tb_cdc_archive = ds_measles_cdc_archive.read_csv()
    tb_cdc_archive["source"] = ""
    tb_cdc_state = ds_measles_cdc.read("state_measles")
    tb_cdc_state["source"] = "CDC WONDER - https://wonder.cdc.gov/controller/datarequest/D130"
    tb_pop = ds_pop.read("us_state_population")
    #
    # Process data.
    #
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path, country_col="countryname")
    # Clean up the data
    tb = clean_project_tycho(tb)
    # Calculate annual cases
    tb = tb.groupby(["countryname", "state", "year"])["countvalue"].sum().reset_index()
    tb["source"] = "Project Tycho - https://zenodo.org/records/11452259"
    # Combine the tables from the different sources into one table.
    tb = combine_state_tables(tb, tb_cdc_historical, tb_cdc_archive, tb_cdc_state)
    # Combine with population
    tb = tb.merge(tb_pop, left_on=["country", "year"], right_on=["state", "year"], how="left")

    tb["case_count"] = pd.to_numeric(tb["case_count"], errors="coerce")
    tb["population"] = pd.to_numeric(tb["population"], errors="coerce")
    tb["case_rate"] = tb["case_count"] / tb["population"] * 100000
    tb = tb.drop(columns=["state", "population"])
    tb["case_count"].metadata.origins = origins
    tb["case_rate"].metadata.origins = origins
    tb["source"].metadata.origins = origins
    tb = tb.format(["country", "year"], short_name="measles")
    #
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


def combine_state_tables(tb: Table, tb_cdc_historical: Table, tb_cdc_archive: Table, tb_cdc_state: Table) -> Table:
    """
    Combine the tables from the different sources into one table.
    - Project Tycho: 1888-2001 (tb)
    - CDC Historical (from PDFs) <2001
    - CDC archive: 2002-2015 (tb_cdc_archive)
    - CDC NNDSS: 2016-2022 (tb_cdc_state)
    """
    # Format the Project Tycho data to match the CDC data
    tb = tb[["state", "year", "countvalue", "source"]]
    tb = tb.rename(columns={"countvalue": "case_count", "state": "country"})

    #
    tb_cdc_historical = tb_cdc_historical.rename(columns={"cases": "case_count"})

    # Drop national data and type of case (indigenous vs imported) from the CDC archive
    tb_cdc_archive = tb_cdc_archive[["country", "year", "total_measles_cases", "source"]]
    tb_cdc_archive = tb_cdc_archive.rename(columns={"total_measles_cases": "case_count"})
    tb_cdc_archive = tb_cdc_archive[tb_cdc_archive["country"] != "United States"]

    # Format the CDC current data to match
    tb_cdc_state = combine_new_yorks(tb_cdc_state)
    tb_cdc_state = tb_cdc_state[tb_cdc_state["disease"] == "Total"]
    tb_cdc_state = tb_cdc_state[["country", "year", "case_count", "source"]]

    combined_tb = pr.concat([tb, tb_cdc_historical, tb_cdc_archive, tb_cdc_state])
    # Check there aren't duplicate rows
    combined_tb.set_index(["country", "year"], verify_integrity=True)

    return combined_tb


def combine_new_yorks(tb_cdc_state: Table) -> Table:
    """
    Combine the data for New York City and New York State
    """
    msk = tb_cdc_state["country"].isin(["New York City", "New York (excluding New York City)"])
    tb_ny = tb_cdc_state[msk]
    tb_not_ny = tb_cdc_state[~msk]

    tb_ny = tb_ny.groupby(["year", "disease", "source"])["case_count"].sum().reset_index()
    tb_ny["country"] = "New York"

    tb_cdc_state = pr.concat([tb_not_ny, tb_ny])

    return tb_cdc_state
