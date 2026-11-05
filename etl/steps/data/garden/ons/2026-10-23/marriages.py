"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Columns expected in the meadow table.
EXPECTED_COLUMNS = {"year", "age", "gender", "cumulative_percentage_per_1000"}
# Exact ages (in years) covered by tables 14a and 14b.
EXPECTED_AGES = set(range(17, 51))


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("marriages")

    # Read table from meadow dataset.
    tb = ds_meadow.read("marriages")
    sanity_check_inputs(tb)

    #
    # Process data.
    #
    # Convert "per 1,000" into a percentage.
    tb["cumulative_percentage_per_100"] = tb["cumulative_percentage_per_1000"] / 10
    tb = tb.drop(columns=["cumulative_percentage_per_1000"])

    sanity_check_outputs(tb)
    tb = tb.format(["year", "age", "gender"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()


def sanity_check_inputs(tb) -> None:
    assert set(tb.columns) == EXPECTED_COLUMNS, f"Unexpected meadow columns: {sorted(tb.columns)}"
    assert set(tb["gender"].unique()) == {"Men", "Women"}, f"Unexpected genders: {set(tb['gender'].unique())}"
    assert set(tb["age"].unique()) == EXPECTED_AGES, f"Unexpected ages: {sorted(tb['age'].unique())}"


def sanity_check_outputs(tb) -> None:
    column = "cumulative_percentage_per_100"
    values = tb[column].dropna()
    assert len(values) > 0, "All values are missing."
    out_of_bounds = tb[(tb[column] < 0) | (tb[column] > 100)]
    assert out_of_bounds.empty, f"Shares outside [0, 100]:\n{out_of_bounds}"

    # The share ever married can only grow with age within a birth cohort.
    tb_sorted = tb.dropna(subset=[column]).sort_values(["gender", "year", "age"])
    decreases = tb_sorted[tb_sorted.groupby(["gender", "year"], observed=True)[column].diff() < 0]
    assert decreases.empty, f"Share ever married decreases with age:\n{decreases}"
