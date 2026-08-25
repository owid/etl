"""Load the harmonized-distributions garden dataset and push its summary table to grapher.

Only the global MLD decomposition goes to the database (as the entity "World", covering the common
PIP-and-WID sample); the bin-level distributions and the model/audit tables stay garden-only.
"""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    ds_garden = paths.load_dataset("harmonized_income_distributions")
    tb = ds_garden.read("inequality_decomposition")

    #
    # Process data.
    #
    # The decomposition is global (over the common country sample); grapher needs an entity.
    tb["country"] = "World"
    tb = tb.format(["country", "year", "series"], short_name="inequality_decomposition")

    #
    # Save outputs.
    #
    ds_grapher = paths.create_dataset(tables=[tb], default_metadata=ds_garden.metadata)
    ds_grapher.save()
