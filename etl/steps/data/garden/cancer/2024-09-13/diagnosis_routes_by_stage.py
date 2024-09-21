"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("diagnosis_routes_by_stage")

    # Read table from meadow dataset.
    tb = ds_meadow["diagnosis_routes_by_stage"].reset_index()

    #
    # Process data.
    #
    tb["route"] = tb["route"].str.replace(r"^\d+\s", "", regex=True)

    # Group by the relevant columns and sum the 'count_by_stage'
    summed_tb = tb.groupby(["country", "year", "site", "route"], as_index=False)["count_by_stage"].sum()

    # Add a new 'route' value to indicate these are summed rows
    summed_tb["stage"] = "All stages of cancer"

    # Append the summed rows to the original DataFrame
    tb = pr.concat([tb, summed_tb], ignore_index=True)
    tb = tb.format(["country", "year", "site", "stage", "route"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
