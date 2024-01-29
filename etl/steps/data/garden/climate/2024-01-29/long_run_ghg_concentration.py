"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset on long-run antarctic ice core CO2 concentration, and read its main table.
    ds_co2_long = paths.load_dataset("antarctic_ice_core_co2_concentration")
    tb_co2_long = ds_co2_long["antarctic_ice_core_co2_concentration"].reset_index()

    # Load garden dataset on present-day ghg concentration, and read its main table.
    ds_ghg = paths.load_dataset("ghg_concentration")
    tb_ghg = ds_ghg["ghg_concentration"].reset_index()

    #
    # Process data.
    #
    # Select and rename columns.
    tb = tb[list(COLUMNS)].rename(columns=COLUMNS, errors="raise")

    # Since pandas datetime cannot handle such long past dates, for simplicity, round up years, and take average co2
    # concentrations of years that appear on more than one row.
    tb["year"] = tb["year"].round(0).astype(int)
    tb = tb.groupby("year", as_index=False).agg({"co2_concentration": "mean"})

    # Convert bp years to conventional years.
    tb["year"] = year_bp_to_year(tb["year"])

    # Add location column.
    tb["location"] = "World"

    # Set an appropriate index and sort conveniently.
    tb = tb.set_index(["location", "year"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)

    # Save changes in the new garden dataset.
    ds_garden.save()
