"""Load a meadow dataset and create a garden dataset."""

import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# List columns to select from wide dataframe, and how to rename them.
WIDE_DF_COLUMNS = {
    "area": "country",
    "year": "year",
    "Electricity demand - Demand - Demand - TWh": "demand__twh",
    "Electricity demand - Demand per capita - Demand per capita - MWh": "demand_per_capita__mwh",
    "Electricity generation - Aggregate fuel - Clean - %": "generation__clean__pct",
    "Electricity generation - Aggregate fuel - Clean - TWh": "generation__clean__twh",
    "Electricity generation - Aggregate fuel - Coal - %": "generation__coal__pct",
    "Electricity generation - Aggregate fuel - Coal - TWh": "generation__coal__twh",
    "Electricity generation - Aggregate fuel - Fossil - %": "generation__fossil__pct",
    "Electricity generation - Aggregate fuel - Fossil - TWh": "generation__fossil__twh",
    "Electricity generation - Aggregate fuel - Hydro, bioenergy and other renewables - %": "generation__hydro__bioenergy_and_other_renewables__pct",
    "Electricity generation - Aggregate fuel - Hydro, bioenergy and other renewables - TWh": "generation__hydro__bioenergy_and_other_renewables__twh",
    "Electricity generation - Aggregate fuel - Renewables - %": "generation__renewables__pct",
    "Electricity generation - Aggregate fuel - Renewables - TWh": "generation__renewables__twh",
    "Electricity generation - Aggregate fuel - Wind and solar - %": "generation__wind_and_solar__pct",
    "Electricity generation - Aggregate fuel - Wind and solar - TWh": "generation__wind_and_solar__twh",
    "Electricity generation - Fuel - Bioenergy - %": "generation__bioenergy__pct",
    "Electricity generation - Fuel - Bioenergy - TWh": "generation__bioenergy__twh",
    "Electricity generation - Fuel - Gas - %": "generation__gas__pct",
    "Electricity generation - Fuel - Gas - TWh": "generation__gas__twh",
    "Electricity generation - Fuel - Hard Coal - %": "generation__hard_coal__pct",
    "Electricity generation - Fuel - Hard Coal - TWh": "generation__hard_coal__twh",
    "Electricity generation - Fuel - Hydro - %": "generation__hydro__pct",
    "Electricity generation - Fuel - Hydro - TWh": "generation__hydro__twh",
    "Electricity generation - Fuel - Lignite - %": "generation__lignite__pct",
    "Electricity generation - Fuel - Lignite - TWh": "generation__lignite__twh",
    "Electricity generation - Fuel - Nuclear - %": "generation__nuclear__pct",
    "Electricity generation - Fuel - Nuclear - TWh": "generation__nuclear__twh",
    "Electricity generation - Fuel - Other Fossil - %": "generation__other_fossil__pct",
    "Electricity generation - Fuel - Other Fossil - TWh": "generation__other_fossil__twh",
    "Electricity generation - Fuel - Other Renewables - %": "generation__other_renewables__pct",
    "Electricity generation - Fuel - Other Renewables - TWh": "generation__other_renewables__twh",
    "Electricity generation - Fuel - Solar - %": "generation__solar__pct",
    "Electricity generation - Fuel - Solar - TWh": "generation__solar__twh",
    "Electricity generation - Fuel - Wind - %": "generation__wind__pct",
    "Electricity generation - Fuel - Wind - TWh": "generation__wind__twh",
    "Electricity generation - Total - Total generation - TWh": "generation__total__twh",
    "Electricity imports - Electricity imports - Net imports - TWh": "net_imports__total__twh",
    "Power sector emissions - Aggregate fuel - Clean - MtCO2": "emissions__clean__mtco2",
    "Power sector emissions - Aggregate fuel - Coal - MtCO2": "emissions__coal__mtco2",
    "Power sector emissions - Aggregate fuel - Fossil - MtCO2": "emissions__fossil__mtco2",
    "Power sector emissions - Aggregate fuel - Hydro, bioenergy and other renewables - MtCO2": "emissions__hydro__bioenergy_and_other_renewables__mtco2",
    "Power sector emissions - Aggregate fuel - Renewables - MtCO2": "emissions__renewables__mtco2",
    "Power sector emissions - Aggregate fuel - Wind and solar - MtCO2": "emissions__wind_and_solar__mtco2",
    "Power sector emissions - CO2 intensity - CO2 intensity - gCO2 per kWh": "emissions__co2_intensity__gco2_per_kwh",
    "Power sector emissions - Fuel - Bioenergy - MtCO2": "emissions__bioenergy__mtco2",
    "Power sector emissions - Fuel - Gas - MtCO2": "emissions__gas__mtco2",
    "Power sector emissions - Fuel - Hard Coal - MtCO2": "emissions__hard_coal__mtco2",
    "Power sector emissions - Fuel - Hydro - MtCO2": "emissions__hydro__mtco2",
    "Power sector emissions - Fuel - Lignite - MtCO2": "emissions__lignite__mtco2",
    "Power sector emissions - Fuel - Nuclear - MtCO2": "emissions__nuclear__mtco2",
    "Power sector emissions - Fuel - Other Fossil - MtCO2": "emissions__other_fossil__mtco2",
    "Power sector emissions - Fuel - Other Renewables - MtCO2": "emissions__other_renewables__mtco2",
    "Power sector emissions - Fuel - Solar - MtCO2": "emissions__solar__mtco2",
    "Power sector emissions - Fuel - Wind - MtCO2": "emissions__wind__mtco2",
    "Power sector emissions - Total - Total emissions - MtCO2": "emissions__total__mtco2",
}


def run(dest_dir: str) -> None:
    log.info("european_electricity_review.start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow: Dataset = paths.load_dependency("european_electricity_review")

    # Read table from meadow dataset.
    tb_meadow = ds_meadow["european_electricity_review"]

    # Create a dataframe with data from the table.
    df = pd.DataFrame(tb_meadow)

    #
    # Process data.
    #

    # Create a new column that combines all groupings.
    df["variable_units"] = (
        df["category"].astype(str)
        + " - "
        + df["subcategory"].astype(str)
        + " - "
        + df["variable"].astype(str)
        + " - "
        + df["unit"].astype(str)
    )

    # Transform long dataframe into a wide dataframe.
    df_wide = (
        df.pivot(index=["area", "year"], columns="variable_units", values="value")
        .reset_index()
        .rename_axis(None, axis=1)
    )

    # Select and rename columns.
    df_wide = df_wide.rename(columns=WIDE_DF_COLUMNS, errors="raise")[WIDE_DF_COLUMNS.values()]

    # Harmonize country names.
    df_wide = geo.harmonize_countries(df=df_wide, countries_file=paths.country_mapping_path)

    # Set an appropriate index and sort conveniently.
    df_wide = df_wide.set_index(["country", "year"], verify_integrity=True).sort_index().sort_index(axis=1)

    # Create a new table with the processed data.
    tb_garden = Table(df_wide, short_name="european_electricity_review")

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_garden], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("european_electricity_review.end")
