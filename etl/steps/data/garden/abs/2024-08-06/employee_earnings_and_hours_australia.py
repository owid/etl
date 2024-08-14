"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define indicators and their new names
INDICATORS = {"50th percentile (median) (2nd quartile)": "p50", "90th percentile": "p90"}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("employee_earnings_and_hours_australia")

    # Read table from meadow dataset.
    tb = ds_meadow["employee_earnings_and_hours_australia"].reset_index()

    #
    # Process data.
    #
    tb = calculate_p90_p50_ratio(tb)

    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def calculate_p90_p50_ratio(tb: Table) -> Table:
    """
    Calculate P90/P50 from the values for 90th and 50th percentiles
    """

    # Make table wide
    tb = tb.pivot(index=["country", "year"], columns="indicator", values="value").reset_index()

    # Rename columns
    tb = tb.rename(columns=INDICATORS, errors="raise")

    # Calculate P90/P50
    tb["p90_p50_ratio"] = tb["p90"] / tb["p50"] * 100

    # Drop columns
    tb = tb.drop(columns=INDICATORS.values())

    return tb
