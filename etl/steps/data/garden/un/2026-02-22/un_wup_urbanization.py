"""Process the UN World Urbanization Prospects data."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Table names to process
TABLE_NAMES = ["rural", "cities", "towns", "cities_and_towns"]


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("un_wup_urbanization")

    # Process each table
    tables = []
    for table_name in TABLE_NAMES:
        # Read table from meadow dataset.
        tb = ds_meadow[table_name].reset_index()

        #
        # Process data.
        #
        # Harmonize country names.
        tb = paths.regions.harmonize_names(tb)

        # Set index and format.
        tb = tb.format(["country", "year"], short_name=table_name)
        tb = tb.drop(columns=["locid", "iso3_code"], errors="ignore")
        tables.append(tb)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(tables=tables, check_variables_metadata=True, default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()
