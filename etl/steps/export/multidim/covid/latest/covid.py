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
        # "covid.cases.yml",
        # "covid.deaths.yml",
        # "covid.hospital.yml",
        # "covid.vax.yml",
        # "covid.xm.yml",
        # "covid.covax.yml",
        # "covid.models.yml",
        # "covid.xm_models.yml",
        # "covid.vax_breakdowns.yml",
        "covid.cases_tests.yml",
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

    # TODO: Make it work for long-formatted
    # Automatic ones (they have dimensions in the tables)
    ## Read table
    ds_grapher = paths.load_dataset("google_mobility")
    tb = ds_grapher.read("google_mobility")

    ## Read config
    fname = "covid.mobility.yml"
    config = paths.load_mdim_config(fname)
    slug = fname_to_slug(fname)
    # config["views"] += multidim.expand_views(
    #     config,
    #     {"place": "*"},
    #     "grapher/covid/latest/google_mobility/google_mobility",
    #     engine,
    # )  # type: ignore

    ## Add views
    config["views"] += multidim.generate_views_for_dimensions(
        dimensions=config["dimensions"],
        tables=[tb],
        dimensions_order_in_slug=["place"],
        warn_on_missing_combinations=False,
        additional_config=MOBILITY_CONFIG_DEFAULT,
    )
    ### Add config view for each place
    for view in config["views"]:
        view["config"] = {**MOBILITY_CONFIG_DEFAULT, **view.get("config", {})}

    multidim.upsert_multidim_data_page(slug, config, engine, paths.dependencies)


def fname_to_slug(fname: str) -> str:
    return f"mdd-{fname.replace('.yml', '').replace('.', '-').replace('_', '-')}"
