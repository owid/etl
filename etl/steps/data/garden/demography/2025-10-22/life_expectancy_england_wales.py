"""Load life tables and create England & Wales life expectancy dataset with detailed age groups."""

from owid.catalog import Table

from etl.helpers import PathFinder

paths = PathFinder(__file__)

COUNTRY = "England and Wales"

# Ages to include (as strings, matching the life_tables dataset)
AGES_STR = ["0", "1", "5", "10", "20", "30", "40", "50", "60", "70"]
AGES_INT = {0, 1, 5, 10, 20, 30, 40, 50, 60, 70}


def run() -> None:
    #
    # Load inputs.
    #
    ds_lt = paths.load_dataset("hmd")
    tb_lt = ds_lt.read("life_tables")

    #
    # Process data.
    #
    tb = process_lt(tb_lt)

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(
        tables=[tb.format(["country", "year", "sex", "age"], short_name=paths.short_name)],
        check_variables_metadata=True,
        default_metadata=ds_lt.metadata,
    )

    ds_garden.save()


def process_lt(tb: Table) -> Table:
    """Process LT data for England & Wales with detailed age groups.

    Output format: country, year, sex, age | life_expectancy.
    Life expectancy is total (conditional on reaching age), i.e. age + remaining life expectancy.
    """
    tb = tb.loc[
        (tb["country"] == COUNTRY)
        & (tb["age"].isin(AGES_STR))
        & (tb["type"] == "period"),
        ["country", "year", "sex", "age", "life_expectancy"],
    ]

    # Assign integer dtype
    tb["age"] = tb["age"].astype("Int64")

    # Convert remaining life expectancy to total life expectancy
    tb["life_expectancy"] = tb["life_expectancy"] + tb["age"]

    # Check column values
    _check_column_values(tb, "sex", {"total", "female", "male"})
    _check_column_values(tb, "age", AGES_INT)

    return tb


def _check_column_values(tb: Table, column: str, expected_values: set) -> None:
    """Check that a column has only expected values."""
    unexpected_values = set(tb[column]) - expected_values
    assert not unexpected_values, f"Unexpected values found in column {column}: {unexpected_values}"
