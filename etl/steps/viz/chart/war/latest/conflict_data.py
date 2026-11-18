"""Conflict multidim, shown as a data explorer (replaces the conflict-data explorer).

It shows the same views as the conflict-data-source multidim, with the dropdowns in the order of
this step's config (Conflict type first, Data source fourth) and its own title.
"""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    # Load the conflict-data-source multidim built upstream.
    cs = paths.load_chartset("conflict_data_source")
    c_source = cs.read("conflict_data_source")

    # Create this multidim from its config, and take the upstream views.
    config = paths.load_config()
    c = paths.create_chart(config=config, short_name="conflict_data")
    c.views = c_source.views

    # NOTE: no download package until it handles indicators that share a title across sources
    # (UCDP, UCDP + PRIO and PRIO); building it fails on duplicate column names. See owid/etl#7336.
    c.save(download_package=False)
