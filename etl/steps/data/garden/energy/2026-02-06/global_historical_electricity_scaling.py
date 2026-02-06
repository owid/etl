"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("global_historical_electricity")

    # Read table from garden dataset.
    tb = ds_garden.read("global_historical_electricity")

    #
    # Process data.
    #
    # Create a new table of years since producing 100TWh.
    production_start = 100
    year_column = f"years_since_{production_start}_twh"
    tb_scaling = (
        tb[tb["total_production"] >= production_start][["country", "total_production"]]
        .reset_index(drop=True)
        .reset_index()
        .rename(columns={"index": year_column}, errors="raise")
    )
    columns_sources = [column for column in tb.columns if "production" in column if column != "total_production"]
    for column in columns_sources:
        mask = tb[column] >= production_start
        tb_source_production = (
            tb[mask][["year", column]]
            .reset_index(drop=True)
            .reset_index()
            .rename(columns={"index": year_column}, errors="raise")
            .rename(columns={"year": column.replace("_production", "_year")})
        )
        tb_scaling = tb_scaling.merge(tb_source_production, on=year_column, how="outer")

    # Drop unnecessary columns (for sources that have never reached 100TWh of production).
    tb_scaling = tb_scaling.dropna(axis=1, how="all")

    # Improve table format.
    tb_scaling = tb_scaling.format(keys=["country", year_column], short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Initialize a new grapher dataset.
    ds_grapher = paths.create_dataset(tables=[tb_scaling], default_metadata=ds_garden.metadata)

    # Save grapher dataset.
    ds_grapher.save()
