"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("plastic_waste_2023_2024.csv")
    origins = [snap.metadata.origin]
    # Load data from snapshot.
    tb = snap.read(encoding="latin-1")

    # Reset index to treat it as a regular column
    tb = tb.reset_index()
    #
    # Process data.
    #

    # Create new column names - columns are offset by 1, so shift them properly
    # The data in each column position corresponds to the next column name
    old_columns = tb.columns.tolist()
    new_columns = old_columns[1:] + [old_columns[-1] + "_extra"]  # Shift left by 1
    tb.columns = new_columns

    # Now select the columns we need
    columns = ["refYear", "reporterDesc", "flowDesc", "partnerDesc", "qty", "motDesc"]
    tb = tb[columns]
    tb = tb.rename(
        columns={
            "refYear": "year",
            "reporterDesc": "country",
            "flowDesc": "export_vs_import",
            "partnerDesc": "partner_country",
            "motDesc": "mode_of_transport",
        }
    )

    # Keep exports/imports to/from World only - could be interesting in the future to create flow diagrams in which case will need to change this
    tb = tb[tb["partner_country"] == "World"]
    tb = tb.drop("partner_country", axis=1)

    for col in tb.columns:
        tb[col].metadata.origins = origins

    # Improve tables format.
    tables = [tb.format(["country", "year", "export_vs_import", "mode_of_transport"])]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables, check_variables_metadata=True, default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()
