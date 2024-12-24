"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("funding.xlsx")

    # Load data from snapshot.
    tb = snap.read(engine="openpyxl")

    #
    # Process data.
    #
    # Combine Disease/Health area and Subcategory to be the entity
    tb["Subcategory"] = tb["Subcategory"].fillna("")

    tb["disease"] = tb["Disease/Health area"] + " - " + tb["Subcategory"]
    # Strip the trailing ' - ' if there is nothing after it
    tb["disease"] = tb["disease"].str.replace(r" - $", "", regex=True)
    tb = tb.drop(columns=["Disease/Health area", "Subcategory"])
    # Drop 'FY'from the year column
    tb["Year"] = tb["Year"].str.replace("FY ", "").astype(int)
    # Set index as disease and year, there are plenty of duplicates but we will aggregate in garden.
    tb = tb.format(["disease", "year"])
    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
