from etl import multidim
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
mdim_handler = multidim.MDIMHandler(paths)

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
        config = mdim_handler.load_config_from_yaml(fname)

        mdim_handler.upsert_data_page(
            fname_to_slug(fname),
            config,
        )

    # PART 2: MDIMs hybridly generated (mix of YAML file + data)
    ds = paths.load_dataset("google_mobility")
    tb = ds.read("google_mobility")

    # Simple multidim
    config = mdim_handler.load_config_from_yaml("covid.mobility.yml")

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

    # Upsert to DB
    mdim_handler.upsert_data_page(
        fname_to_slug("covid.mobility.yml"),
        config,
    )


def fname_to_slug(fname: str) -> str:
    return f"mdd-{fname.replace('.yml', '').replace('.', '-').replace('_', '-')}"
