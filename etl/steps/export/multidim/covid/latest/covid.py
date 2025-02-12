from etl import multidim
from etl.db import get_engine
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
# Default config for GOOGLE MOBILITY
MOBILITY_CONFIG_DEFAULT = {
    "subtitle": "This data shows how community movement in specific locations has changed relative to the period before the pandemic.",
    "note": "It's not recommended to compare levels across countries; local differences in categories could be misleading.",
    "originUrl": "ourworldindata.org/coronavirus",
    "minTime": "earliest",
    "maxTime": "latest",
    "hideAnnotationFieldsInTitle": {"time": True},
    "addCountryMode": "change-country",
}


def run(dest_dir: str) -> None:
    engine = get_engine()

    filenames = [
        "covid.cases.yml",
        "covid.deaths.yml",
        "covid.hospital.yml",
        "covid.vax.yml",
        "covid.xm.yml",
        "covid.covax.yml",
        "covid.models.yml",
        "covid.xm_models.yml",
        "covid.vax_breakdowns.yml",
        "covid.cases_tests.yml",
        "covid.mobility.yml",
    ]
    # Load YAML file
    for fname in filenames:
        paths.log.info(fname)
        config = paths.load_mdim_config(fname)
        slug = fname_to_slug(fname)
        multidim.upsert_multidim_data_page(
            slug,
            config,
            engine,
            paths.dependencies,
        )


def fname_to_slug(fname: str) -> str:
    return f"mdd-{fname.replace('.yml', '').replace('.', '-').replace('_', '-')}"
