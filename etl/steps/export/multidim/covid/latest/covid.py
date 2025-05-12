from etl.collection import multidim
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
    # PART 1: MDIMs entirely from YAML files
    # Load MDIM configurations from YAML files
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
        paths.log.info(fname)
        config = paths.load_mdim_config(fname)

        mdim = paths.create_collection_legacy(config, short_name=fname_to_short_name(fname))
        mdim.save()

    # PART 2: MDIMs hybridly generated (mix of YAML file + data)
    ds = paths.load_dataset("google_mobility")
    tb = ds.read("google_mobility", load_data=False)

    # Simple multidim
    config = paths.load_mdim_config("covid.mobility.yml")

    # Generate config from indicator
    config_new = multidim.expand_config(
        tb=tb,
        common_view_config=MOBILITY_CONFIG_DEFAULT,
    )

    # Combine dimension info from YAML + programmatically obtained
    config["dimensions"] = multidim.combine_config_dimensions(
        config_dimensions=config_new["dimensions"],
        config_dimensions_yaml=config["dimensions"],
    )

    # Combine views info from YAML + programmatically obtained
    config["views"] = config["views"] + config_new["views"]

    # WIP: DEBUGGING
    # multidim.adjust_mdim_views(config, paths.dependencies_by_table_name)

    # Upsert to DB
    mdim = paths.create_collection_legacy(
        config=config,
        short_name=fname_to_short_name("covid.mobility.yml"),
    )

    mdim.save()


def fname_to_short_name(fname: str) -> str:
    """Custom MDIM name generator."""
    return f"{fname.replace('.yml', '').replace('.', '_')}"
