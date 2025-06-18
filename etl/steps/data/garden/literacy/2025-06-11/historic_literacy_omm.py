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
    # Process 1900-1950 data: drop age column and add source
    tb_1900_1950 = tb_1900_1950.drop(columns=["age"])
    tb_1900_1950["source"] = "Progress of literacy in various countries (UNESCO)"

    # Process 1950 data: rename columns and add source
    tb_1950 = tb_1950.rename(columns={"literacy_est": "literacy_rate", "illiteracy_est": "illiteracy_rate"})
    tb_1950["source"] = "World illiteracy at mid-century (UNESCO)"

    # Process 1451-1800 data: rename, calculate illiteracy, and add source
    tb_1451_1800 = tb_1451_1800.rename(columns={"literate": "literacy_rate"})
    tb_1451_1800["illiteracy_rate"] = 100 - tb_1451_1800["literacy_rate"]
    tb_1451_1800["source"] = "Buringh and Van Zanden"

    # Process OECD data: filter, rename, and add source
    tb_oecd = tb_oecd[["country", "year", "literacy"]].copy()
    tb_oecd = tb_oecd.rename(columns={"literacy": "literacy_rate"})
    tb_oecd["illiteracy_rate"] = 100 - tb_oecd["literacy_rate"]
    tb_oecd = tb_oecd[tb_oecd["year"] < 1970]
    tb_oecd["source"] = "How Was Life? Global Well-being since 1820 (OECD)"

    # Process UNESCO data: filter, rename, and add source
    tb_unesco = tb_unesco[
        ["country", "year", "adult_literacy_rate__population_15plus_years__both_sexes__pct__lr_ag15t99"]
    ].copy()
    tb_unesco = tb_unesco.rename(
        columns={"adult_literacy_rate__population_15plus_years__both_sexes__pct__lr_ag15t99": "literacy_rate"}
    )
    tb_unesco["illiteracy_rate"] = 100 - tb_unesco["literacy_rate"]
    tb_unesco["source"] = "SDG 4 Education - Global and Thematic Indicators (UNESCO)"

    # Remove any rows with missing data to avoid duplicates
    tables = [tb_1950, tb_1900_1950, tb_1451_1800, tb_oecd, tb_unesco]
    tables_clean = []
    for table in tables:
        # Drop rows with missing literacy_rate
        tables_clean.append(table.dropna(subset=["literacy_rate"]))
    # Concatenate all tables
    tb = pr.concat(tables_clean, axis=0, ignore_index=True)
    tb["source"] = tb["literacy_rate"].copy_metadata(tb["literacy_rate"])
    tb = tb.format(["country", "year"], short_name="historic_literacy_omm")

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_1900_1950.metadata)

    # Save garden dataset.
    ds_garden.save()
