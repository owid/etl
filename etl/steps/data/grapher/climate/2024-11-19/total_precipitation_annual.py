"""Load a garden dataset and create a grapher dataset."""


from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("total_precipitation")
    tb = ds_garden["total_precipitation"].reset_index()

    #
    # Process data.
    #

    # Get the year
    tb["year"] = tb["time"].astype(str).str[0:4]

    # Group by year and calculate the mean of the specified columns
    tb_annual_average = (
        tb.groupby(["year", "country"])
        .agg({"total_precipitation": "sum", "precipitation_anomaly": "sum"})
        .reset_index()
    )

    # Convert the 'year' column to integer type
    tb_annual_average["year"] = tb_annual_average["year"].astype(int)

    # Set the index to 'year' and 'country'
    tb_annual_average = tb_annual_average.set_index(["year", "country"], verify_integrity=True)

    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir, tables=[tb_annual_average], default_metadata=ds_garden.metadata, check_variables_metadata=True
    )

    ds_grapher.save()
