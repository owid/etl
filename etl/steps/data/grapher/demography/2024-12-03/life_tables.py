"""There is some work to filter only those indicators and dimensions that are relevant for the grapher.

That is, we may just want a subset of the indicators, and few single-age groups.
"""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
# Relevant indicators
INDICATORS_RELEVANT = [
    "central_death_rate",
    "life_expectancy",
    "probability_of_death",
]
INDICATORS_RELEVANT_REL = [
    "life_expectancy_fm_diff",
    "life_expectancy_fm_ratio",
    "central_death_rate_mf_ratio",
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
    ds_garden = paths.load_dataset("life_tables")

    # Read table from garden dataset.
    tb = ds_garden.read("life_tables")
    tb_diff = ds_garden.read("diff_ratios")

    #
    # Process data.
    #
    ## Only keep particular ages
    tb = tb.loc[tb["age"].isin(AGES_SINGLE)]
    tb_diff = tb_diff.loc[tb_diff["age"].isin(AGES_SINGLE)]

    ## Set index back
    tb = tb.format(["country", "year", "sex", "age", "type"])
    tb_diff = tb_diff.format(["country", "year", "age", "type"])

    ## Only keep subset of columns
    tb = tb[INDICATORS_RELEVANT]
    tb_diff = tb_diff[INDICATORS_RELEVANT_REL]

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir, tables=[tb, tb_diff], check_variables_metadata=True, default_metadata=ds_garden.metadata
    )

    # Save changes in the new grapher dataset.
    ds_grapher.save()
