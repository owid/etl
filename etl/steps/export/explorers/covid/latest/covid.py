"""Load a grapher dataset and create an explorer dataset with its tsv file.

This step contains tooling that should moved to a more general module so that it can be easily used by other explorer steps!

# optional
        fields_optional = [
            "title",
            "subtitle",
            "type",  # NO
            "hasMapTab",
            "hideAnnotationFieldsInTitle",
            "sortBy",
            "sortColumnSlug",
            "hideTotalValueLabel",
            "selectedFacetStrategy",
            "facetYDomain",  # NO
            "timelineMinTime",
            "note",
            "defaultView",  # NO
            "relatedQuestionText",  # NO
            "relatedQuestionUrl",  # NO
            "tab",
        ]
"""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load grapher config from YAML
    config = paths.load_collection_config()

    # Create explorer
    c = paths.create_collection(config=config, explorer=True)

    # Create a new explorers dataset and tsv file.
    c.save(tolerate_extra_indicators=True)
