"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


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
    tb = drop_flagged_rows_and_unnecessary_columns(tb)

    #
    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
    )
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
            "caution2resourcerevenuestax",
            "caution3unexcludedresourcere",
            "caution4inconsistencieswiths",
        ]
        + caution_variables
    )

    return tb
