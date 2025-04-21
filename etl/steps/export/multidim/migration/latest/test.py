from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


# etlr multidim
def run() -> None:
    # Load configuration from adjacent yaml file.
    mdims = paths.load_mdims("covid")
    mdim = mdims.read("covid_cases")

    mdim.catalog_path = f"{paths.namespace}/{paths.version}/{paths.short_name}#{paths.short_name}"

    # Save & upload
    mdim.save()
