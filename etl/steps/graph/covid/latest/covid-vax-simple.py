"""Simple multidim graph step - COVID vaccinations."""

from etl.collection.core.create import create_collection
from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    """Create COVID vaccination multidim collection."""
    paths.log.info("graph.covid_vax_simple", message="Creating COVID vax collection")

    # Load the YAML config (which has the multidim structure)
    config = paths.load_config(path=paths.metadata_path)

    # Create collection directly from config
    # The YAML has views with catalogPath - no table needed
    c = create_collection(
        config_yaml=config,
        dependencies=paths.dependencies,
        catalog_path="graph/covid-vax-simple#covid-vax-simple",
    )
    c.save()

    paths.log.info("graph.covid_vax_simple.done", message="Collection created successfully")
