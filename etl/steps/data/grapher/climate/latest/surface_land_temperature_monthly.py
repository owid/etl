"""Load a garden dataset and create a grapher dataset."""


import owid.catalog.processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("surface_land_temperature")
    tb = ds_garden["surface_land_temperature"].reset_index()
    tb["year"] = tb["time"].astype(str).str[0:4]
    tb["month"] = tb["time"].astype(str).str[5:7]
    #
    # Process data.
    #
    tb = tb.drop(columns=["time"], errors="raise")
    # Transpose the DataFrame to have a country column, a column identifying the measure, and year columns
    tb_pivot = tb[["country", "year", "month", "temperature_2m"]].pivot(
        index=["country", "month"], columns="year", values="temperature_2m", join_column_levels_with="_"
    )

    # Calculate decadal averages (first melt the Table to have a column for the year)
    tb_long = tb_pivot.melt(id_vars=["month", "country"], var_name="year", value_name="value")
    # Make sure year is an integer
    tb_long["year"] = tb_long["year"].astype(int)

    # Calculate the decade
    tb_long["decade"] = (tb_long["year"] // 10) * 10

    # Group by month, country, and decade, then calculate the mean
    decadal_averages = tb_long.groupby(["month", "country", "decade"])["value"].mean().reset_index()
    # Adjust the decade column to be a string (e.g., 1950s) for later transforming it into columns
    decadal_averages["decade"] = decadal_averages["decade"].astype(str) + "s"

    # Pivot to transform decades into columns
    pivoted_decadal = decadal_averages.pivot_table(
        index=["month", "country"], columns="decade", values="value"
    ).reset_index()
    # Merge the two dataframes
    tb_merged = pr.merge(pivoted_decadal, tb_pivot, on=["month", "country"], how="outer")

    tb_merged = tb_merged.rename(columns={"month": "year"})
    tb_merged = tb_merged.set_index(["country", "year"])
    # Convert all column names to strings
    tb_merged.columns = [str(col) for col in tb_merged.columns]
    # Make sure indicator titles are correct.
    for column in tb_merged.columns:
        tb_merged[column].metadata.title = column
        tb_merged[column].metadata.origins = tb["temperature_2m"].metadata.origins
        tb_merged[column].metadata.unit = tb["temperature_2m"].metadata.unit
        tb_merged[column].metadata.short_unit = tb["temperature_2m"].metadata.short_unit
        tb_merged[column].metadata.description_short = tb["temperature_2m"].metadata.description_short
        tb_merged[column].metadata.description_from_producer = tb["temperature_2m"].metadata.description_from_producer
        tb_merged[column].metadata.description_processing = tb["temperature_2m"].metadata.description_processing

    tb_merged.metadata = tb.metadata

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(dest_dir, tables=[tb_merged], default_metadata=ds_garden.metadata)
    ds_grapher.metadata.title = "Monthly land surface temperatures since 1950 by country"
    ds_grapher.save()
