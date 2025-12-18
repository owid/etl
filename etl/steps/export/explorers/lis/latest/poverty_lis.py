"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define indicators to use
INDICATOR_NAMES = [
    "headcount_ratio",
    "headcount",
    "total_shortfall",
    "avg_shortfall",
    "income_gap_ratio",
    "poverty_gap_index",
]

DIMENSIONS_CONFIG = {
    "poverty_line": [
        "1",
        "2",
        "5",
        "10",
        "20",
        "30",
        "40",
        "40% of the median",
        "50% of the median",
        "60% of the median",
    ],
    "welfare_type": ["dhi", "mi"],
    "equivalence_scale": ["per capita", "square root"],
}


def run() -> None:
    #
    # Load inputs.
    #
    # Default collection config
    config = paths.load_collection_config()

    # Load grapher dataset.
    ds = paths.load_dataset("luxembourg_income_study")
    tb = ds.read("poverty", load_data=False)

    #
    # Create collection object
    #
    c = paths.create_collection(
        config=config,
        short_name="lis_poverty",
        tb=tb,
        indicator_names=INDICATOR_NAMES,
        # dimensions=DIMENSIONS_CONFIG,
        explorer=True,
    )

    #
    # (optional) Edit views
    #
    for view in c.views:
        if (
            view.dimensions["indicator"] == "heacount_ratio"
            and view.dimensions["poverty_line"] == "5"
            and view.dimensions["welfare_type"] == "dhi"
            and view.dimensions["equivalence_scale"] == "per capita"
        ):
            view.config["defaultView"] = "true"
        pass

    #
    # Save garden dataset.
    #
    c.save()
