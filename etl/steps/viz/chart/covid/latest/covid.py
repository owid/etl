# Force re-run
from etl.helpers import PathFinder
from etl.viz.chart.download_link import add_download_link

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


# Interim download link for the COVID-19 data explorer (see etl/viz/chart/download_link.py).
COVID_DOWNLOAD_URL = "https://docs.owid.io/projects/etl/api/covid/"
COVID_DOWNLOAD_TEXT = "Download our complete COVID-19 dataset [from our data catalog]({url})."


def run() -> None:
    # PART 1: Charts entirely from YAML files (no programmatic config extracted from table)
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
        "covid.explorer.yml",
        # "covid.vax_breakdowns.yml",
    ]

    for fname in filenames:
        ## Load config
        paths.log.info(fname)
        config = paths.load_config(fname)

        ## Create and save chart
        c = paths.create_chart(config=config, short_name=fname_to_short_name(fname))
        if fname == "covid.explorer.yml":
            # The explorer it replaces has a download button; keep pointing at the same page
            # until grapher links to the multidim's own download package.
            add_download_link(c, url=COVID_DOWNLOAD_URL, text=COVID_DOWNLOAD_TEXT)
        c.save()

    # PART 2: Chart hybridly generated (YAML file + programmatic config)
    ## Load data
    ds = paths.load_dataset("google_mobility")
    tb = ds.read("google_mobility", load_data=False)

    ## Create and save chart
    fname = "covid.mobility.yml"
    c = paths.create_chart(
        config=paths.load_config("covid.mobility.yml"),
        short_name=fname_to_short_name("covid.mobility.yml"),
        tb=tb,
        common_view_config=MOBILITY_CONFIG_DEFAULT,
    )
    c.save()


def fname_to_short_name(fname: str) -> str:
    """Custom MDIM name generator."""
    return f"{fname.replace('.yml', '').replace('.', '_')}"
