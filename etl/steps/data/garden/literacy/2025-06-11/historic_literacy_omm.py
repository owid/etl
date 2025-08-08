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
    tb_1900_1950["source"] = "UNESCO (1953)"
    tb_1900_1950["source_url"] = "https://unesdoc.unesco.org/ark:/48223/pf0000002898"

    # Process 1950 data: rename columns
    tb_1950 = tb_1950.rename(columns={"literacy_est": "literacy_rate", "illiteracy_est": "illiteracy_rate"})
    tb_1950["source"] = "UNESCO (1957)"
    tb_1950["source_url"] = "https://unesdoc.unesco.org/ark:/48223/pf0000002930"

    # Process 1451-1800 data: rename and calculate illiteracy
    tb_1451_1800 = tb_1451_1800.rename(columns={"literate": "literacy_rate"})
    tb_1451_1800["illiteracy_rate"] = 100 - tb_1451_1800["literacy_rate"]
    tb_1451_1800["source"] = "Buringh and Van Zanden (2009)"
    tb_1451_1800["source_url"] = (
        "https://www.researchgate.net/publication/46544350_Charting_the_Rise_of_the_West_Manuscripts_and_Printed_Books_in_Europe_A_Long-Term_Perspective_from_the_Sixth_through_Eighteenth_Centuries"
    )

    # Process OECD data: filter and rename
    tb_oecd = tb_oecd[["country", "year", "literacy"]].copy()
    tb_oecd = tb_oecd.rename(columns={"literacy": "literacy_rate"})
    tb_oecd["illiteracy_rate"] = 100 - tb_oecd["literacy_rate"]
    tb_oecd = tb_oecd[tb_oecd["year"] < 1970]
    tb_oecd["source"] = "van Zanden, J. et al. (2014)"
    tb_oecd["source_url"] = "https://www.oecd.org/en/publications/how-was-life_9789264214262-en.html"

    # Process UNESCO data: filter and rename
    tb_unesco = tb_unesco[
        ["country", "year", "adult_literacy_rate__population_15plus_years__both_sexes__pct__lr_ag15t99"]
    ].copy()
    tb_unesco = tb_unesco.rename(
        columns={"adult_literacy_rate__population_15plus_years__both_sexes__pct__lr_ag15t99": "literacy_rate"}
    )
    tb_unesco["illiteracy_rate"] = 100 - tb_unesco["literacy_rate"]
    tb_unesco["source"] = "UNESCO Institute for Statistics"
    tb_unesco["source_url"] = "https://databrowser.uis.unesco.org/resources/bulk"

    # Remove any rows with missing data to avoid duplicates
    tables = [tb_1950, tb_1900_1950, tb_1451_1800, tb_oecd, tb_unesco]
    tables_clean = []
    for table in tables:
        # Drop rows with missing literacy_rate
        tables_clean.append(table.dropna(subset=["literacy_rate"]))
    # Concatenate all tables
    tb = pr.concat(tables_clean, axis=0, ignore_index=True)
    tb = tb.format(["country", "year"], short_name="historic_literacy_omm")

    # Set metadata origins for source columns
    tb["source"].metadata.origins = tb["literacy_rate"].metadata.origins
    tb["source_url"].metadata.origins = tb["literacy_rate"].metadata.origins

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_1900_1950.metadata)

    # Save garden dataset.
    ds_garden.save()
