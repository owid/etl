"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

from owid.datautils.dataframes import map_series

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Sources to include, and how to rename them.
SOURCES = {
    "Coal": "Coal",
    "Bioenergy": "Bioenergy",
    "Gas": "Gas",
    "Hydro": "Hydro",
    "Nuclear": "Nuclear",
    "Other fossil": "Oil",
    "Solar": "Solar",
    "Total": "Total",
    "Wind": "Wind",
    # 'Clean': 'Clean',
    # 'Fossil': 'Fossil',
    # 'Gas and other fossil': 'Gas and other fossil',
    # 'Hydro bioenergy and other renewables': 'Hydro bioenergy and other renewables',
    # 'Renewables': 'Renewables',
    # 'Wind and solar': 'Wind and solar',
}


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("global_historical_electricity_scaling")

    # Read table from garden dataset.
    tb = ds_garden.read("global_historical_electricity_scaling")

    #
    # Process data.
    #
    # Select and rename sources.
    tb = tb[tb["source"].isin(list(SOURCES))].reset_index(drop=True)
    tb["source"] = map_series(
        tb["source"], mapping=SOURCES, warn_on_missing_mappings=True, warn_on_unused_mappings=True
    )

    # Use sources as entities (replacing the "World" country column).
    tb = tb.drop(columns=["country"]).rename(columns={"source": "country"}, errors="raise")

    # Improve table format.
    tb = tb.format(["country", "year"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Initialize a new grapher dataset.
    ds_grapher = paths.create_dataset(tables=[tb], default_metadata=ds_garden.metadata)

    # Save grapher dataset.
    ds_grapher.save()
