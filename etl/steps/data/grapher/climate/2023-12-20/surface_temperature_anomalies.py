"""Load a garden dataset and create a grapher dataset."""


from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("surface_temperature")
    tb = ds_garden["surface_temperature"].reset_index()
    tb["year"] = tb["time"].astype(str).str[0:4]
    tb["month"] = tb["time"].astype(str).str[5:7]

    #
    # Process data.
    #
    tb = tb.drop(columns=["time"], errors="raise")

    # Transpose the DataFrame to have a country column, a column identifying the measure, and year columns
    tb_pivot = tb[["country", "year", "month", "temperature_anomaly"]].pivot(
        index=["country", "month"], columns="year", values="temperature_anomaly", join_column_levels_with="_"
    )
    tb_pivot = tb_pivot.rename(columns={"month": "year"})
    tb_pivot = tb_pivot.set_index(["country", "year"])
    # Convert all column names to strings
    tb_pivot.columns = [str(col) for col in tb_pivot.columns]

    # Make sure indicator titles are correct.
    for column in tb_pivot.columns:
        tb_pivot[column].metadata.title = column

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(dest_dir, tables=[tb_pivot], default_metadata=ds_garden.metadata)
    ds_grapher.metadata.title = "Monthly surface temperature anomaly since 1950 by country"
    ds_grapher.save()
