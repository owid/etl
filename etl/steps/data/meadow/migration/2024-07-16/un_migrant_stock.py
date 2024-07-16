"""Load a snapshot and create a meadow dataset.
Meadow dataset is already very processed to """
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("un_migrant_stock_dest_origin.xlsx")

    # Load data from snapshot.
    # table 1
    tb = snap.read_excel(sheet_name="Table 1", header=10)

    tb = tb.drop(columns=["Notes of destination", "Location code of destination", "Location code of origin"])
    tb = tb.rename(
        columns={
            "Region, development group, country or area of destination": "country_destination",
            "Region, development group, country or area of origin": "country_origin",
            "Type of data of destination": "data_type",
        }
    )

    tb.columns = [str(col).strip() for col in tb.columns]

    #
    # Process data.
    #
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["index"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
