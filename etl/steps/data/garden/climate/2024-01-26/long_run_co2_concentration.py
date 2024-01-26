"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Columns to read from the data, and how to rename them.
COLUMNS = {
    "gasage__yr_bp": "year",
    "co2__ppmv": "co2_concentration",
    # "sigma_mean_co2__ppmv": "co2_concentration_standard_deviation",
}


def year_bp_to_year(year_bp):
    """Convert year before present (where present refers to 1950) into default years (in the Gregorian calendar),
    avoiding year zero.

    Parameters
    ----------
    year_bp : float or pd.Series
        Year before 1950.

    Returns
    -------
    year : pd.Series
        Regular year.

    """
    year = 1950 - year_bp

    # Skip year zero.
    year[year < 1] = year[year < 1] - 1

    return year


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its main table.
    ds_meadow = paths.load_dataset("long_run_co2_concentration")
    tb = ds_meadow["long_run_co2_concentration"].reset_index()

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
