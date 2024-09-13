"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("diagnosis_routes_survival")

    # Read table from meadow dataset.
    tb = ds_meadow["diagnosis_routes_survival"].reset_index()

    #
    # Process data.
    #

    # Extract the last year from the 'year' column which is in the format '2006-2010'.
    tb["year"] = tb["year"].apply(lambda x: int(x.split("-")[0])).astype(int)
    tb["route"] = tb["route"].str.replace(r"^\d+\s", "", regex=True)
    print(tb["gender"].unique())

    tb = tb.format(["country", "year", "site", "gender", "route", "length"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
