"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Regions to create aggregates for.
REGIONS = {
    # Continents.
    "Africa": {},
    "Asia": {},
    "North America": {},
    "South America": {},
    "Oceania": {},
    "Europe": {},
    # Income groups.
    "Low-income countries": {},
    "Lower-middle-income countries": {},
    "Upper-middle-income countries": {},
    "High-income countries": {},
    # Other groups.
    "European Union (27)": {},
    "World": {},
}

# List of known overlaps of historical regions in the data.
ACCEPTED_OVERLAPS = [{year: {"Aruba", "Netherlands Antilles"} for year in range(1990, 2011 + 1)}]


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("energy_statistics_database")

    # Read table from meadow dataset.
    tb = ds_meadow.read("energy_statistics_database")

    #
    # Process data.
    #
    # Harmonize country names.
    tb = paths.regions.harmonize_names(tb=tb)

    # Create region aggregates (including one for "World") for specific commodities.
    assert set(tb[tb["commodity"] == "Total Electricity"]["unit"]) == {"Gigawatt-hours"}
    tb_electricity = (
        tb[tb["commodity"] == "Total Electricity"].drop(columns=["commodity", "unit"]).reset_index(drop=True)
    )
    tb_electricity = paths.regions.add_aggregates(
        tb=tb_electricity,
        index_columns=["country", "year", "transaction"],
        regions=REGIONS,
        accepted_overlaps=ACCEPTED_OVERLAPS,
    )
    tb_electricity = tb_electricity.assign(**{"commodity": "Total Electricity", "unit": "Gigawatt-hours"})

    # Replace the old data for Total Electricity with the new one that contains region aggregates.
    tb = pr.concat([tb[tb["commodity"] != "Total Electricity"], tb_electricity], ignore_index=True)

    # Improve table format.
    tb = tb.format(["country", "year", "commodity", "transaction"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()
