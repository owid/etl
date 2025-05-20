"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Default collection config
    config = paths.load_collection_config()

    # Load grapher dataset.
    ds = paths.load_dataset("enterprise_surveys")
    tb = ds.read("enterprise_surveys")

    # Assuming your DataFrame is named df
    tb = pr.melt(
        tb,
        id_vars=["year", "country"],
        var_name="indicator_name",
        value_name="value",
    )

    tb["value"].m.dimensions = "indicator"
    print(tb)

    #
    # (optional) Adjust dimensions if needed
    #

    #
    # Create collection object
    #
    c = paths.create_collection(
        config=config,
        short_name="enterprise_surveys",
        tb=tb,
        # indicator_names=[],
        # dimensions={},
    )

    #
    # (optional) Edit views
    #
    for view in c.views:
        # if view.dimension["sex"] == "male":
        #     view.config["title"] = "Something else"
        pass

    #
    # Save garden dataset.
    #
    c.save()
