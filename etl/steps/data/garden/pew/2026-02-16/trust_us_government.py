"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("trust_us_government")

    # Read table from meadow dataset.
    tb = ds_meadow.read("trust_us_government")

    #
    # Process data.
    #
    # Harmonize country names.
    tb = paths.regions.harmonize_names(tb=tb)

    # Average both indicators by year.
    tb["year"] = tb["date"].astype(str).str.extract(r"(\d{4})$").astype(int)
    tb = tb.groupby(["country", "year"], observed=True, as_index=False)[["individual_polls", "smoothed_trend"]].mean()

    # Rename columns.
    tb = tb.rename(columns={"individual_polls": "trust_us_government", "smoothed_trend": "trust_us_government_smoothed"}, errors="raise")

    # Improve table format.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()
