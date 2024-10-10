"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    snap = paths.load_snapshot("important_issues.csv")

    tb = snap.read_csv()

    tb = tb.rename(
        columns={
            "Issue": "country",
            "You Personally (%)": "share_issue_personal",
            "Britain (%)": "share_issue_britain",
        },
        errors="raise",
    )

    tb["year"] = 2024

    tb = tb.format(["country", "year"])

    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()
