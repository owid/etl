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
    ds = paths.load_dataset("wdi")
    tb = ds.read("wdi", load_data=False)

    #
    # Create collection object
    #
    c = paths.create_collection(
        config=config,
        short_name="labor_force_participation_rate",
    )

    # Group sex views
    c.group_views(
        groups=[
            {
                "dimension": "sex",
                "choices": ["male", "female"],
                "choice_new_slug": "male_vs_female",
                "view_config": {
                    "hideRelativeToggle": "false",
                    "selectedFacetStrategy": "entity",
                    "hasMapTab": False,
                    "chartTypes": ["LineChart"],
                },
            },
        ]
    )

    #
    # Edit views
    #
    for view in c.views:
        # Initialize config and metadata if they're None
        if view.config is None:
            view.config = {}
        if view.metadata is None:
            view.metadata = {}

        if view.dimensions["sex"] == "male_vs_female":
            # Get the catalog path of the first indicator
            if view.indicators.y:
                first_indicator_path = view.indicators.y[0].catalogPath
                # Extract the column name from the catalog path
                indicator_col = first_indicator_path.split("#")[-1]

            # Define meta object and render jinja templates
            meta = tb[indicator_col].metadata.render({})

            # Remove "Male " from beginning of title. Also capitalize first letter. We need to extract this from the table's metadata (Grapher step)
            # It is only male because of how we grouped the views
            view.config["title"] = (
                meta.presentation.grapher_config["title"].replace("Male ", "").capitalize() + ": males vs. females"
            )
            view.config["subtitle"] = meta.presentation.grapher_config["subtitle"].replace("male ", "")
            view.metadata["description_short"] = meta.description_short.replace("male ", "")

    #
    # Save garden dataset.
    #
    c.save()
