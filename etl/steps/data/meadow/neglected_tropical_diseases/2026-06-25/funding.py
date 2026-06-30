"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("funding.xlsx")

    # Load data from snapshot.
    tb = snap.read_excel()

    #
    # Process data.
    #
    # Combine Disease/Health area and Subcategory to be the entity
    tb["Subcategory"] = tb["Subcategory"].fillna("")

    # Combine Disease/Health area and Subcategory with a hyphen if subcategory is not empty, otherwise just use Disease/Health area

    tb["disease"] = tb.apply(lambda row: row["Disease/Health area"] + " - " + row["Subcategory"] if row["Subcategory"] else row["Disease/Health area"], axis=1)

    tb = tb.drop(columns=["Disease/Health area", "Subcategory"])
    # Drop 'FY'from the year column

    tb["Year"] = tb["Year"].str.replace("FY ", "").astype(int)

    # Set index as disease and year, there are plenty of duplicates but we will aggregate in garden.
    tb = tb.set_index(["disease", "Year"], verify_integrity=False)
    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
