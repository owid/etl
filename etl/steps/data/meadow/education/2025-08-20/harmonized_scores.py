"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("harmonized_scores.csv")

    tb = snap.read(low_memory=False)

    # Select relevant columns
    tb = tb[["REF_AREA_LABEL", "SEX_LABEL", "2010", "2017", "2018", "2020"]]

    # Rename columns
    tb = tb.rename(columns={"REF_AREA_LABEL": "country", "SEX_LABEL": "sex"})

    # Melt the table to have year as a column
    tb = tb.melt(
        id_vars=["country", "sex"],
        value_vars=["2010", "2017", "2018", "2020"],
        var_name="year",
        value_name="harmonized_test_scores",
    )

    # Convert year to integer
    tb["year"] = tb["year"].astype(int)

    tb = tb.format(["country", "year", "sex"])
    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
