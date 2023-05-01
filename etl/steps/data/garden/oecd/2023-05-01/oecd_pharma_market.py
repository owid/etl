"""Load a meadow dataset and create a garden dataset."""

import pandas as pd
from owid.catalog import Dataset, Table, VariableMeta
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


MAPPING_MEASURE = {
    "% of total sales": {"unit": "%", "title": "Sales: % of total sales"},
    "% share of generics (value)": {"unit": "%", "title": "Market: % share of generics (value)"},
    "% share of generics (volume)": {"unit": "%", "title": "Market: % share of generics (volume)"},
    "/capita, US$ exchange rate": {"unit": "US$ per capita", "title": "Sales: US$ (exchange rate) per capita"},
    "/capita, US$ purchasing power parity": {
        "unit": "US$ per capita",
        "title": "Sales: US$ (purchasing power parity per capita",
    },
    "Defined daily dosage per 1 000 inhabitants per day": {
        "unit": "DDD per 1,000 inhabitants per day",
        "title": "Consumption: DDD per 1,000 inhabitants per day",
    },
    "Million US$ at exchange rate": {"unit": "million US$", "title": "Sales: Million US$ at exchange rate"},
    "Million US$, purchasing power parity": {
        "unit": "million US$",
        "title": "Sales: Million US$, purchasing power parity",
    },
    "Million of national currency units": {
        "unit": "million of national currency units",
        "title": "Sales: Million of national currency units",
    },
}


def run(dest_dir: str) -> None:
    log.info("oecd_pharma_market.start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow: Dataset = paths.load_dependency("oecd_pharma_market")

    # Read table from meadow dataset.
    tb_meadow = ds_meadow["oecd_pharma_market"]

    # Create a dataframe with data from the table.
    df = pd.DataFrame(tb_meadow)

    #
    # Process data.
    #
    # Harmonize country names
    log.info("oecd_pharma_market.harmonize_countries")
    df = geo.harmonize_countries(df=df, countries_file=paths.country_mapping_path)
    # Pivot (each variable has its own column)
    # This is done so that we can customize the metadata for each variable.
    df = df.pivot(index=["country", "year"], columns=["variable", "measure"], values="value")

    # Create a new table with the processed data.
    tb_garden = Table(df, short_name=paths.short_name)
    # Build variable metadata
    variable_metadata = []
    for col in tb_garden.columns:
        # If necessary, add description to variable metadata

        # Bake variable metadata
        variable_metadata.append(
            VariableMeta(
                title=f"{col[0]} ({MAPPING_MEASURE[col[1]]['title']})",
                unit=MAPPING_MEASURE[col[1]]["unit"],
            )
        )
    # Rename column names
    tb_garden.columns = [f"{col[0]} ({MAPPING_MEASURE[col[1]]['title']})" for col in tb_garden.columns]
    # Assign metadata to columns
    for col, metadata in zip(tb_garden.columns, variable_metadata):
        tb_garden[col].metadata = metadata

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_garden], default_metadata=ds_meadow.metadata)
    ds_garden.update_metadata(paths.metadata_path)
    # Save changes in the new garden dataset.
    ds_garden.save()
    log.info("oecd_pharma_market.end")
