"""Garden step for Ireland Data Centres Metered Electricity Consumption.

This step processes the meadow data and creates a table with annual aggregates
by summing quarterly data for each year.
"""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    """Create garden dataset from meadow dataset."""

    # Load inputs
    ds_meadow = paths.load_dataset("ireland_metered_consumption")
    tb = ds_meadow["ireland_metered_consumption"].reset_index()

    # Store origin metadata
    origin = tb["value"].metadata.origins[0]

    # Pivot to get consumption categories as columns
    tb = tb.pivot_table(
        index=["year"], columns="electricity_consumption", values="value", aggfunc="sum", observed=True
    ).reset_index()

    # Clean column names
    tb.columns.name = None
    tb.columns = [col.lower().replace(" ", "_") for col in tb.columns]

    # Convert from GWh to TWh (divide by 1000)
    consumption_cols = ["all_metered_electricity_consumption", "customers_other_than_data_centres", "data_centres"]
    for col in consumption_cols:
        tb[col] = tb[col] / 1000

    # Add country
    tb["country"] = "Ireland"

    # Calculate percentage of data centre consumption
    tb["data_centres_pct"] = (tb["data_centres"] / tb["all_metered_electricity_consumption"]) * 100

    # Add metadata to all columns
    for col in tb.columns:
        if col not in ["country", "year"]:
            tb[col].metadata.origins = [origin]

    # Format table
    tb = tb.format(["country", "year"])

    # Save outputs
    ds_garden = create_dataset(
        dest_dir,
        tables=[tb],
        check_variables_metadata=True,
        default_metadata=ds_meadow.metadata,
    )
    ds_garden.save()
