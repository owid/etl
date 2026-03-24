"""
Load a meadow dataset and create a garden dataset.

NOTE: To extract the log of the process (to review sanity checks, for example), follow these steps:
    1. Define LONG_FORMAT as True.
    2. Run the following command in the terminal:
        nohup uv run etl run government_revenue_dataset > government_revenue_dataset.log 2>&1 &
"""

from owid.catalog import Table
from structlog import get_logger
from tabulate import tabulate

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Set table format when printing
TABLEFMT = "pretty"

# Define if I show the full table or just the first 5 rows for assertions
LONG_FORMAT = False


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("government_revenue_dataset")

    # Read table from meadow dataset.
    tb = ds_meadow["government_revenue_dataset"].reset_index()

    # Delete country column and rename iso column to country.
    tb = tb.drop(columns=["country"]).rename(columns={"iso": "country"})

    #
    # Process data.
    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
    )
    tb = drop_flagged_rows_and_unnecessary_columns(tb)

    tb = tb.set_index(["country", "year"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def drop_flagged_rows_and_unnecessary_columns(tb: Table) -> Table:
    """
    Process data, changing column names, dropping columns and dropping flagged rows.
    """

    # Define caution variables. We will remove the rows with caution values.
    caution_variables = [
        "caution1accuracyqualityorco",  # Caution 1 Accuracy, Quality or Comparability of data questionable
        # "caution2resourcerevenuestax",  # Caution 2 Un-excluded Resource Revenues/ taxes are significant but cannot be isolated from total revenue/ taxes
        # "caution3unexcludedresourcere",  # Caution 3 Un-excluded Resource Revenue/ taxes are marginal but non-negligible and cannot be isolated from total revenue/ taxes
        # "caution4inconsistencieswiths",  # Caution 4 Inconsistencies with Social Contributions
    ]

    # Count the number of rows before dropping caution variables.
    rows_before = len(tb)

    # Remove trailing whitespaces from these columns and drop row with non-empty values ('').
    for col in caution_variables:
        tb[col] = tb[col].str.strip()
        tb = tb[tb[col] == ""].reset_index(drop=True)

    # Count the number of rows after dropping caution variables.
    rows_after = len(tb)
    rows_dropped = rows_before - rows_after
    rows_dropped_pct = round(rows_dropped / rows_before * 100, 2)

    paths.log.info(
        f"Dropped {rows_dropped} rows out of {rows_before} ({rows_dropped_pct}%), because of them being flagged."
    )

    # Drop caution variables and identifiers.
    tb = tb.drop(
        columns=[
            "identifier",
            "general",
            "source",
            "id",
            "reg",
            "inc",
            "historicalinc",
            "generalnotes",
            "cautionnotes",
            "resourcerevenuenotes",
            "socialcontributionsnotes",
        ]
    )

    tb = sanity_checks(tb)

    # Remove all caution columns
    tb = tb.drop(
        columns=[
            "caution1accuracyqualityorco",
            "caution2resourcerevenuestax",
            "caution3unexcludedresourcere",
            "caution4inconsistencieswiths",
        ]
    )

    return tb


def sanity_checks(tb: Table) -> None:
    """
    Perform sanity checks on the data.
    """

    tb = tb.copy()

    tb = check_negative_values(tb)

    return tb


def check_negative_values(tb: Table):
    """
    Check if there are negative values in the variables
    """

    tb = tb.copy()

    # Define columns as all the columns minus country and year
    variables = [
        col
        for col in tb.columns
        if col
        not in ["country", "year"]
        + [
            "caution1accuracyqualityorco",
            "caution2resourcerevenuestax",
            "caution3unexcludedresourcere",
            "caution4inconsistencieswiths",
        ]
    ]

    for v in variables:
        # Create a mask to check if any value is negative
        mask = tb[v] < 0
        any_error = mask.any()

        if any_error:
            tb_error = tb[mask].reset_index(drop=True).copy()
            paths.log.warning(
                f"""{len(tb_error)} observations for {v} are negative:
                {_tabulate(tb_error[['country', 'year', 'caution1accuracyqualityorco', 'caution2resourcerevenuestax','caution3unexcludedresourcere','caution4inconsistencieswiths',v]], long_format=LONG_FORMAT)}"""
            )

    return tb


def _tabulate(tb: Table, long_format: bool, headers="keys", tablefmt=TABLEFMT, **kwargs):
    if long_format:
        return tabulate(tb, headers=headers, tablefmt=tablefmt, **kwargs)
    else:
        return tabulate(tb.head(5), headers=headers, tablefmt=tablefmt, **kwargs)
