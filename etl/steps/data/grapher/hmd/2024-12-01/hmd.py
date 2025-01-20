"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

INDICATORS_RELEVANT_LT = [
    "central_death_rate",
    "life_expectancy",
    "probability_of_death",
]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("hmd")

    # Read table from garden dataset.
    tb_lt = ds_garden.read("life_tables")
    tb_exposure = ds_garden.read("exposures")
    tb_deaths = ds_garden.read("deaths")
    tb_pop = ds_garden.read("population")
    tb_births = ds_garden.read("births")
    tb_ratios = ds_garden.read("diff_ratios")
    tb_deaths_agg = ds_garden.read("deaths_agg")

    # Filter relevant dimensions
    tb_lt = keep_only_relevant_dimensions(tb_lt)
    tb_exposure = keep_only_relevant_dimensions(tb_exposure)
    tb_deaths = keep_only_relevant_dimensions(tb_deaths)
    tb_pop = keep_only_relevant_dimensions(tb_pop)
    tb_ratios = keep_only_relevant_dimensions(tb_ratios)
    tb_deaths_agg = keep_only_relevant_dimensions(tb_deaths_agg)

    #
    # Save outputs.
    #
    cols_index = ["country", "year", "sex", "age", "type"]
    tables = [
        tb_lt.format(cols_index),
        tb_exposure.format(cols_index),
        tb_deaths.format(["country", "year", "sex", "age"]),
        tb_pop.format(["country", "year", "sex", "age"]),
        tb_births.format(["country", "year", "sex"]),
        tb_ratios.format(["country", "year", "age", "type"]),
        tb_deaths_agg.format(["country", "year", "sex"]),
    ]
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir, tables=tables, check_variables_metadata=True, default_metadata=ds_garden.metadata
    )

    # Save changes in the new grapher dataset.
    ds_grapher.save()


def keep_only_relevant_dimensions(tb):
    """Keep only relevant dimensions.

    - We only preserve 5-year age groups, and specific 1-year age groups.
    - We only preserve 1-year observation periods.

    """
    AGES_SINGLE = [
        0,
        10,
        15,
        25,
        45,
        65,
        80,
        "total",
    ]
    AGES_SINGLE = list(map(str, AGES_SINGLE)) + ["110+"]
    flag_1 = tb["age"].isin(AGES_SINGLE)
    flag_2 = tb["age"].str.contains(
        "-",
    )

    tb = tb.loc[flag_1 | flag_2]

    return tb
