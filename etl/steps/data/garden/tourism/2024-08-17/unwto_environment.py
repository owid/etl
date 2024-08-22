"""Load a meadow dataset and create a garden dataset."""

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("unwto_environment")

    # Read table from meadow dataset.
    tb = ds_meadow["unwto_environment"].reset_index()

    #
    # Process data.
    #
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    tb = tb.pivot(index=["country", "year"], columns="indicator", values="value").reset_index()

    tb = tb.format(["country", "year"])
    # Shorten column names.
    tb = tb.rename(
        columns={
            "implementation_of_standard_accounting_tools_to_monitor_the_economic_and_environmental_aspects_of_tourism__seea_tables": "seea_tables",
            "implementation_of_standard_accounting_tools_to_monitor_the_economic_and_environmental_aspects_of_tourism__tourism_satellite_account_tables": "tsa_tables",
            "implementation_of_standard_accounting_tools_to_monitor_the_economic_and_environmental_aspects_of_tourism__number_of_tables": "total_tables",
        }
    )
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
