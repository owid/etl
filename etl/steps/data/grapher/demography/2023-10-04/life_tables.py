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
    "life_expectancy_fm_diff",
    "life_expectancy_fm_ratio",
    "central_death_rate_mf_ratio",
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
    ds_garden = paths.load_dataset("life_tables")

    # Read table from garden dataset.
    tb = ds_garden["life_tables"]

    #
    # Process data.
    #
    ## Reset index
    column_index = list(tb.index.names)
    tb = tb.reset_index()

    ## Only keep 5-year age groups, and 1-year observation periods
    tb = tb[tb["age"].isin(AGES_SINGLE)]

    ## Set dtype of year to int
    tb["year"] = tb["year"].astype("Int64")

    ## Set index back
    tb = tb.set_index(column_index, verify_integrity=True).sort_index()

    ## Only keep subset of columns
    tb = tb[INDICATORS_RELEVANT]

    # Rename location -> country
    tb = tb.rename_index_names({"location": "country"})

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_garden.metadata
    )

    # Save changes in the new grapher dataset.
    ds_grapher.save()
