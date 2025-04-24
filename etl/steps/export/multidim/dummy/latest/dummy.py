"""This is an example on how you can read another MDIM and create a new one based on it.

TODO:

    - Transform MDIM into Explorer easily
    - Combine with YML metadata
    - Ease setting of catalog_path
    - Combine multiple MDIMs
"""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


# etlr multidim
def run() -> None:
    # Load configuration from adjacent yaml file.
    mdims = paths.load_mdims("covid")
    mdim_cases = mdims.read("covid_cases")
    mdim_deaths = mdims.read("covid_deaths")

    mdims = [
        mdim_cases,
        mdim_deaths,
    ]
    mdim_name = "test_combined"

    # Combine
    mdim = combine_mdims(mdims, mdim_name)

    # Save & upload
    mdim.save()


from copy import deepcopy
from typing import List

from structlog import get_logger

from etl.collections.beta import (
    _build_df_choices,
    _combine_dimensions,
    _extract_choice_slug_changes,
    _update_choice_slugs_in_views,
)
from etl.collections.multidim import Multidim

log = get_logger()


def combine_mdims(mdims: List[Multidim], mdim_name: str):
    # Check that there are at least 2 MDIMs to combine
    assert len(mdims) > 0, "No MDIMs to combine."
    assert len(mdims) > 1, "At least two MDIMs should be provided."

    # Check that all MDIMs have the same dimensions (slug, name, etc.)
    mdims_dims = None
    for m in mdims:
        dimensions_flatten = [{k: v for k, v in dim.to_dict().items() if k != "choices"} for dim in m.dimensions]
        if mdims_dims is None:
            mdims_dims = dimensions_flatten
        else:
            assert (
                mdims_dims == dimensions_flatten
            ), "Dimensions are not the same across MDIMs. Please review that dimensions are listed in the same order, have the same slugs, names, description, etc."

    # 0) Preliminary work #
    # Create dictionary with MDIMs, so to have identifiers for them
    mdims_by_id = {str(i): deepcopy(mdim) for i, mdim in enumerate(mdims)}

    # Build dataframe with all choices. Each row provides details of a choices, and explorer identifier and the dimension slug
    df_choices, cols_choices = _build_df_choices(mdims_by_id)

    # 1) Combine dimensions (use first explorer as container/reference) #
    dimensions = _combine_dimensions(
        df_choices=df_choices,
        cols_choices=cols_choices,
        collection=mdims[0].copy(),
    )

    # 2) Combine views #
    # Track modifications (useful later for views)
    choice_slug_changes = _extract_choice_slug_changes(df_choices)
    # Update explorer views (based on changes on choice slugs)
    mdims_by_id = _update_choice_slugs_in_views(choice_slug_changes, mdims_by_id)
    # Collect views
    views = []
    for _, explorer in mdims_by_id.items():
        explorer_views = explorer.views
        views.extend(explorer_views)

    # 3) Ad-hoc change: update explorer_name #
    assert isinstance(mdims[0].catalog_path, str), "Catalog path is not set. Please set it before saving."
    catalog_path = mdims[0].catalog_path.split("#")[0] + "#" + mdim_name

    # 4) Create final explorer #
    mdim = Multidim(
        title={"title": "test", "subtitle": "test"},
        default_selection=mdims[0].default_selection,
        dimensions=dimensions,
        views=views,
        catalog_path=catalog_path,
    )

    # 5) Announce conflicts
    df_conflict = df_choices.loc[df_choices["in_conflict"]]
    if not df_conflict.empty:
        log.warning("Choice slug conflicts resolved")
        for (dimension_slug, choice_slug), group in df_conflict.groupby(["dimension_slug", "slug_original"]):
            # Now group by 'value' to see which col3 values correspond to each unique 'value'
            log.warning(f"(dimension={dimension_slug}, choice={choice_slug})")
            for _, subgroup in group.groupby("choice_slug_id"):
                explorer_ids = subgroup["collection_id"].unique().tolist()
                explorer_names = [mdims_by_id[i].name for i in explorer_ids]
                record = subgroup[cols_choices].drop_duplicates().to_dict("records")
                assert len(record) == 1, "Unexpected, please report!"
                log.warning(f" MDIMs {explorer_names} map to {record[0]}")

    return mdim
