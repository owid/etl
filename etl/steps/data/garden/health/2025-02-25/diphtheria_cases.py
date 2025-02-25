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
    ds_meadow_cdc = paths.load_dataset("diphtheria_cases", namespace="cdc")
    ds_meadow_census = paths.load_dataset("diphtheria_cases", namespace="us_census_bureau")
    ds_population = paths.load_dataset("population")
    # Read table from meadow dataset.
    tb_cdc = ds_meadow_cdc.read("diphtheria_cases")
    tb_census = ds_meadow_census.read("diphtheria_cases")
    tb_population = ds_population.read("population")
    # Combine the data from the two sources
    tb = pr.concat([tb_cdc, tb_census], short_name="diphtheria_cases").sort_values("year").reset_index(drop=True)
    # Combine with population data
    tb = pr.merge(
        tb,
        tb_population,
        on=["country", "year"],
        how="left",
    )
    # Calculate case rate
    tb["case_rate"] = tb["cases"] / tb["population"] * 1000000
    tb = tb.drop(columns=["population", "source_x", "source_y", "world_pop_share"])
    # Process data.
    #
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)

    # Save changes in the new garden dataset.
    ds_garden.save()
