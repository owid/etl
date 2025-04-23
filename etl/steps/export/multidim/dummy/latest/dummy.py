"""This is an example on how you can read another MDIM and create a new one based on it.

TODO:

    - Transform MDIM into Explorer easily
    - Combine with YML metadata
    - Ease setting of catalog_path
    - Combine multiple MDIMs
"""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


# etlr multidim
def run() -> None:
    # Load configuration from adjacent yaml file.
    mdims = paths.load_mdims("covid")
    mdim = mdims.read("covid_cases")

    mdim.title["title"] = "This is a duplicate"
    mdim.catalog_path = f"{paths.namespace}/{paths.version}/{paths.short_name}#{paths.short_name}"

    # Save & upload
    mdim.save()
