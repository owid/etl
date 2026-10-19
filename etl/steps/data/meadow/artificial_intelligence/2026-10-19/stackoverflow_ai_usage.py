"""Load Stack Overflow Developer Survey responses on AI tool usage into meadow."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    """Create meadow dataset."""
    #
    # Load inputs.
    #
    snap = paths.load_snapshot("stackoverflow_ai_usage.parquet")
    tb = snap.read()

    #
    # Process data.
    #
    # Responses have no identifier in the snapshot, so number them within each survey year to build a unique key.
    tb["respondent"] = tb.groupby("year").cumcount()

    # Use snake_case column names, and categoricals for the (low-cardinality) answer columns.
    tb = tb.rename(columns={"MainBranch": "main_branch", "CodeMain": "code_main", "AISelect": "ai_select"})
    for column in ["main_branch", "code_main", "ai_select"]:
        tb[column] = tb[column].astype("category")

    tb = tb.format(["year", "respondent"])

    #
    # Save outputs.
    #
    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)
    ds_meadow.save()
