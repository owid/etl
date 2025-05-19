"""Load a meadow dataset and create a garden dataset."""

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
    ds = paths.load_dataset("corruption_barometer")
    tb = ds.read("corruption_barometer", load_data=False)
    config = paths.load_collection_config()
    #
    # (optional) Adjust dimensions if needed
    #
    # config["views"] = multidim.expand_config(
    #    tb,
    #    dimensions=["question", "answer", "institution"],
    #    additional_config={"chartTypes": ["LineChart"], "hasMapTab": True, "tab": "map"},
    # )

    #
    # Create collection object
    #
    c = paths.create_collection(
        config=config,
        short_name="corruption_barometer",
        tb=tb,
    )
    print(c)

    #
    # Save garden dataset.
    #
    c.save()
