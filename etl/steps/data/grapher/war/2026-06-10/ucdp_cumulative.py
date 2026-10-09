"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    ds_garden = paths.load_dataset("ucdp_cumulative")
    tb = ds_garden.read("ucdp_cumulative")

    #
    # Process data.
    #
    # Remove the suffix in UCDP region names ("Africa (UCDP)" -> "Africa"), as the UCDP grapher dataset does.
    tb["country"] = tb["country"].str.replace(r" \(UCDP\)$", "", regex=True)
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    ds_grapher = paths.create_dataset(tables=[tb], default_metadata=ds_garden.metadata)
    ds_grapher.save()
