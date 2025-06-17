"""Load multiple literacy datasets and create a combined garden dataset."""

import owid.catalog.processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden steps
    ds_1900_1950 = paths.load_dataset("literacy_1900_1950")
    ds_1950 = paths.load_dataset("literacy_1950")
    ds_1451_1800 = paths.load_dataset("literacy_1451_1800")
    ds_world = paths.load_dataset("oecd_education")
    ds_unesco = paths.load_dataset("education_sdgs")

    # Read tables from garden datasets
    tb_1900_1950 = ds_1900_1950.read("literacy_1900_1950")
    tb_1950 = ds_1950.read("literacy_1950")
    tb_1451_1800 = ds_1451_1800.read("literacy_1451_1800")
    tb_oecd = ds_world.read("oecd_education")
    tb_unesco = ds_unesco.read("education_sdgs")

    #
    # Process data.
    #
    # Process 1900-1950 data: drop age column
    tb_1900_1950 = tb_1900_1950.drop(columns=["age"])

    # Process 1950 data: rename columns
    tb_1950 = tb_1950.rename(columns={"literacy_est": "literacy_rate", "illiteracy_est": "illiteracy_rate"})

    # Process 1451-1800 data: rename and calculate illiteracy
    tb_1451_1800 = tb_1451_1800.rename(columns={"literate": "literacy_rate"})
    tb_1451_1800["illiteracy_rate"] = 100 - tb_1451_1800["literacy_rate"]

    # Process OECD data: filter and rename
    tb_oecd = tb_oecd[["country", "year", "literacy"]].copy()
    tb_oecd = tb_oecd.rename(columns={"literacy": "literacy_rate"})
    tb_oecd["illiteracy_rate"] = 100 - tb_oecd["literacy_rate"]
    tb_oecd = tb_oecd[tb_oecd["country"] == "World"]
    tb_oecd = tb_oecd[tb_oecd["year"] < 1970]

    # Process UNESCO data: filter and rename
    tb_unesco = tb_unesco[
        ["country", "year", "adult_literacy_rate__population_15plus_years__both_sexes__pct__lr_ag15t99"]
    ].copy()
    tb_unesco = tb_unesco.rename(
        columns={"adult_literacy_rate__population_15plus_years__both_sexes__pct__lr_ag15t99": "literacy_rate"}
    )
    tb_unesco["illiteracy_rate"] = 100 - tb_unesco["literacy_rate"]

    # Remove any rows with missing data to avoid duplicates
    tables = [tb_1950, tb_1900_1950, tb_1451_1800, tb_oecd, tb_unesco]
    tables_clean = []
    for table in tables:
        if not table.empty:
            # Drop rows with missing literacy_rate
            table_clean = table.dropna(subset=["literacy_rate"])
            if not table_clean.empty:
                tables_clean.append(table_clean)

    # Concatenate all tables
    tb = pr.concat(tables_clean, axis=0, ignore_index=True)
    tb = tb.format(["country", "year"], verify_integrity=True, short_name="historic_literacy_omm")

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_1900_1950.metadata)

    # Save garden dataset.
    ds_garden.save()
