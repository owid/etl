"""Load a garden dataset and create a grapher dataset."""


from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("sea_ice_index")
    tb = ds_garden.read_table("sea_ice_index")

    #
    # Process data.
    #
    # Rename locations conveniently.
    tb = tb.astype({"location": "string"})
    tb.loc[tb["location"] == "Northern Hemisphere", "location"] = "Arctic Ocean"
    tb.loc[tb["location"] == "Southern Hemisphere", "location"] = "Antarctica"
    assert set(tb["location"]) == {"Antarctica", "Arctic Ocean"}, "Unexpected locations."

    # For visualization purposes, create one indicator for each year.
    # Each indicator will have "location" as the "entity" column, and one time stamp for each month in the "time" column.
    # TODO: Alternatively, consider creating only one indicator for arctic ocean, and another for antarctica, where the "entity" column is the year, and the "time" column is the month.
    # TODO: Consider creating also a dataset with decadal anomalies.

    # Create a month and a year columns.
    tb["year"] = tb["date"].dt.year
    tb["month"] = tb["date"].dt.month
    assert (tb.groupby(["location", "year"]).count()["sea_ice_extent"] <= 12).all(), "More than 12 months in a year."
    assert (tb.groupby(["location", "year", "month"]).count()["sea_ice_extent"] == 1).all(), "Repeated months."

    # Create a table with a column for each year.
    tb_months = tb.pivot(
        index=["location", "month"], columns=["year"], values="sea_ice_extent", join_column_levels_with=""
    )

    # To adjust to grapher, rename location column.
    tb_months = tb_months.rename(columns={"location": "country", "month": "year"}, errors="raise")

    # Ensure all column names are strings.
    tb_months = tb_months.rename(
        columns={
            column: f"sea_ice_extent_in_{column}" for column in tb_months.drop(columns=["country", "year"]).columns
        },
        errors="raise",
    )

    # Improve format.
    tb_months = tb_months.format()

    # Improve metadata.
    tb_months.metadata.title = "Monthly sea ice extent"
    for column in tb_months.columns:
        year = column[-4:]
        title = column.replace("_", " ").capitalize()
        if int(year) >= 2020:
            color = "#8e0f0f"
        else:
            color = "#a1abc3"
        tb_months[column].metadata.title = title
        tb_months[column].metadata.display = {"name": year, "color": color}
        tb_months[column].metadata.presentation.title_public = title

    #
    # Save outputs.
    #
    # Create a new grapher dataset.
    ds_grapher = create_dataset(dest_dir, tables=[tb_months], check_variables_metadata=True)
    ds_grapher.save()
