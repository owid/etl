from etl import multidim
from etl.db import get_engine
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    engine = get_engine()

    filenames = [
        "covid.cases.yml",
        "covid.deaths.yml",
        "covid.hospital.yml",
        "covid.boosters.yml",
        "covid.vax_breakdowns.yml",
        "covid.xm.yml",
        "covid.cases_tests.yml",
        "covid.covax.yml",
        "covid.models.yml",
        "covid.xm_models.yml",
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
        )

    # Automatic ones (they have dimensions in the tables)
    fname = "covid.mobility.yml"
    config = paths.load_mdim_config(fname)
    slug = fname_to_slug(fname)
    table = "grapher/covid/latest/google_mobility/google_mobility"
    config["views"] += multidim.expand_views(config, {"place": "*"}, table, engine)  # type: ignore
    multidim.upsert_multidim_data_page(slug, config, engine)


def fname_to_slug(fname: str) -> str:
    return f"mdd-{fname.replace('.yml', '').replace('.', '-').replace('_', '-')}"
