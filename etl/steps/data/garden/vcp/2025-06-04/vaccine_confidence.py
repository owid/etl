"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table
from owid.catalog import processing as pr
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
logging = get_logger()


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("vaccine_confidence")

    # Read table from meadow dataset.
    tb_beliefs = ds_meadow.read("beliefs")
    tb_effective = ds_meadow.read("effective")
    tb_imp_children = ds_meadow.read("impchildren")
    tb_safe = ds_meadow.read("safe")

    tb = pr.concat(
        [
            tb_beliefs,
            tb_effective,
            tb_imp_children,
            tb_safe,
        ],
        ignore_index=True,
    )
    # Process data.

    # Harmonize country names.
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    tb = clean_data(tb)
    tb = weighted_mean_by_country(tb)
    tb = convert_to_percentages(tb)  # Convert to percentages
    tb = combine_strongly_and_tend_to(tb)
    # Improve table format.
    tb = tb.format(["country", "year", "question"], short_name="vaccine_confidence")

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()


def combine_strongly_and_tend_to(tb: Table) -> Table:
    """
    Combine the 'strongly agree' and 'tend to agree' columns into a single 'agree' column.
    Combine the 'strongly disagree' and 'tend to disagree' columns into a single 'disagree' column.

    Args:
        tb: Table with vaccine confidence data

    Returns:
        Table with combined agree/disagree columns
    """
    tb["agree"] = tb["strongly_agree"] + tb["tend_to_agree"]
    tb["disagree"] = tb["strongly_disagree"] + tb["tend_to_disagree"]

    return tb


def convert_to_percentages(tb: Table) -> Table:
    """
    Convert proportions to percentages by multiplying all value columns by 100.

    Args:
        tb: Table with proportional values (0-1)

    Returns:
        Table with percentage values (0-100)
    """
    value_columns = [
        "strongly_agree",
        "tend_to_agree",
        "tend_to_disagree",
        "strongly_disagree",
        "dont_know__prefer_not_to_say",
    ]

    # Multiply each value column by 100
    for col in value_columns:
        tb[col] = tb[col] * 100

    return tb


def clean_data(tb: Table) -> Table:
    """Check the values columns sum to 1"""
    # Check that the sum of the values columns is 1 for each row
    value_columns = [
        "strongly_agree",
        "tend_to_agree",
        "tend_to_disagree",
        "strongly_disagree",
        "dont_know__prefer_not_to_say",
    ]
    tb["sum_values"] = tb[value_columns].sum(axis=1)
    nrows_before = tb.shape[0]
    # remove rows where the sum is not equal to 1 +/- 2%
    msk = (tb["sum_values"] >= 0.98) & (tb["sum_values"] <= 1.02)
    tb = tb[msk]
    nrows_after = tb.shape[0]

    logging.info(f"Removed {nrows_before - nrows_after} rows where the sum of values was not equal to 1 +/- 2%.")
    return tb.drop(columns=["sum_values"])


# Alternative approach using pandas aggregation
def weighted_mean_by_country(tb: Table):
    agg_funcs = {
        col: lambda x: (x * tb.loc[x.index, "n"]).sum() / tb.loc[x.index, "n"].sum()
        for col in [
            "strongly_agree",
            "tend_to_agree",
            "tend_to_disagree",
            "strongly_disagree",
            "dont_know__prefer_not_to_say",
        ]
    }
    agg_funcs["n"] = "sum"  # Sum the sample sizes

    # Apply groupby with aggregation
    tb_combined = tb.groupby(["country", "year", "question"]).agg(agg_funcs).reset_index()
    tb_combined = tb_combined.drop(columns="n")
    return tb_combined
