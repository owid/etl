"""There is some work to filter only those indicators and dimensions that are relevant for the grapher.

That is, we may just want a subset of the indicators, or just fewer dimensions (e.g. we don't want 10-year age groups, but 5-years are enough)
"""

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
# Relevant indicators
INDICATORS_RELEVANT = [
    "central_death_rate",
    "life_expectancy",
    "life_expectancy_fm_diff",
    "life_expectancy_fm_ratio",
    "probability_of_death",
]
# Single-age groups to preserve
AGES_SINGLE = [
    0,
    10,
    15,
    25,
    45,
    65,
    80,
]
AGES_SINGLE = list(map(str, AGES_SINGLE))


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("hmd")

    # Read table from garden dataset.
    tb = ds_garden["hmd"]

    #
    # Process data.
    #
    ## Reset index
    column_index = list(tb.index.names)
    tb = tb.reset_index()

    ## Only keep 5-year age groups, and 1-year observation periods
    tb = keep_only_relevant_dimensions(tb)

    ## Set dtype of year to int
    tb["year"] = tb["year"].astype("Int64")

    ## Set index back
    tb = tb.set_index(column_index, verify_integrity=True).sort_index()

    ## Only keep subset of columns
    tb = tb[INDICATORS_RELEVANT]

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_garden.metadata
    )

    # Save changes in the new grapher dataset.
    ds_grapher.save()


def keep_only_relevant_dimensions(tb: Table) -> Table:
    """Keep only relevant dimensions.

    - We only preserve 5-year age groups, and specific 1-year age groups.
    - We only preserve 1-year observation periods.

    """
    # Keep 5-year age groups + 1-year observation periods
    tb_5 = tb[tb["format"] == "5x1"]

    # Keep 1-year age groups + 1-year observation periods, for specific age groups.
    tb_1 = tb[tb["format"] == "1x1"]
    tb_1 = tb_1[tb_1["age"].isin(AGES_SINGLE)]

    ## Combine
    tb = pr.concat([tb_5, tb_1], ignore_index=True)

    return tb
