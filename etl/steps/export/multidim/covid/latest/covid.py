# Force re-run
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


def run() -> None:
    # PART 1: Collections entirely from YAML files (no programmatic config extracted from table)
    filenames = [
        "covid.cases.yml",
        "covid.deaths.yml",
        "covid.hospital.yml",
        "covid.vax.yml",
        "covid.xm.yml",
        "covid.covax.yml",
        "covid.models.yml",
        "covid.xm_models.yml",
        "covid.cases_tests.yml",
        # "covid.vax_breakdowns.yml",
    ]

    for fname in filenames:
        ## Load config
        paths.log.info(fname)
        config = paths.load_collection_config(fname)

        ## Create and save collection
        c = paths.create_collection(config=config, short_name=fname_to_short_name(fname))
        c.save()

    # PART 2: Collection hybridly generated (YAML file + programmatic config)
    ## Load data
    ds = paths.load_dataset("google_mobility")
    tb = ds.read("google_mobility", load_data=False)

    ## Create and save collection
    fname = "covid.mobility.yml"
    c = paths.create_collection(
        config=paths.load_collection_config("covid.mobility.yml"),
        short_name=fname_to_short_name("covid.mobility.yml"),
        tb=tb,
        common_view_config=MOBILITY_CONFIG_DEFAULT,
    )
    c.save()


def fname_to_short_name(fname: str) -> str:
    """Custom MDIM name generator."""
    return f"{fname.replace('.yml', '').replace('.', '_')}"
