"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
REGIONS = ["North America", "South America", "Europe", "Africa", "Asia", "Oceania", "World"]


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("gbd_risk_cancer")

    # Read table from meadow dataset.
    tb = ds_meadow["gbd_risk_cancer"].reset_index()
    tb = tb.drop(columns=["population_group_name"])
    #
    # Process data.
    #
    tb = paths.regions.harmonize_names(tb)

    # Format the tables
    tb = tb.format(["country", "year", "metric", "measure", "rei", "age", "cause", "sex"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()
