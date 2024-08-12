"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("invasive_species")

    # Read table from meadow dataset.
    tb = ds_meadow["invasive_species"].reset_index()

    #
    # Add cumulative sum for each species
    tables = []
    cols = tb.columns.drop(["continent", "year"]).tolist()
    regions = tb["continent"].unique().tolist()
    for col in cols:
        for region in regions:
            tb_slice = tb.loc[tb["continent"] == region].copy()  # Using .copy() to avoid SettingWithCopyWarning

            # Compute cumulative sum and assign it to a new column
            tb_slice.loc[:, f"{col}_cumulative"] = tb_slice[col].fillna(0).cumsum()

            # Append the modified slice to the tables list
            tables.append(tb_slice)
    tb = pr.concat(tables, ignore_index=True)
    # Process data.
    #
    tb = tb.format(["continent", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
