"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("edstats.csv")

    # Load data from snapshot.
    tb = snap.read(low_memory=False)

    #
    # Process data.
    #
    # Remove redundant columns (SEX, URBANIZATION, AGE, COMP_BREAKDOWN_1, INDICATOR_ROOT, INDICATOR_ROOT_NAME, economy are already within the indicator_name)
    columns_to_drop = [
        "unit",
        "name",
        "SEX",
        "URBANIZATION",
        "AGE",
        "COMP_BREAKDOWN_1",
        "INDICATOR_ROOT",
        "INDICATOR_ROOT_NAME",
        "economy",
        "UNIT_TYPE",
        "source",
        "INDICATOR",
    ]

    tb = tb.drop(columns=columns_to_drop)

    # Identify columns that start with 'YR'
    year_columns = [col for col in tb.columns if col.startswith("YR")]

    # Melt the DataFrame to create a 'year' column
    tb = tb.melt(
        id_vars=[col for col in tb.columns if col not in year_columns],
        value_vars=year_columns,
        var_name="year",
        value_name="value",
    )

    # Remove 'YR' prefix from the 'year' column
    tb["year"] = tb["year"].str.replace("YR", "").astype(int)

    tb = tb.rename({"Country name": "country"}, axis=1)
    # Drop rows where 'country' is NaN
    tb = tb.dropna(subset=["country"])
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year", "indicator_name"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
