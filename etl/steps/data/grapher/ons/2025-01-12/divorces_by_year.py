"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("divorces")

    # Read table from garden dataset.
    tb = ds_garden.read("divorces", reset_index=True)
    tb = tb.drop(columns=["country"])
    tb = tb[tb["anniversary_year"].isin([5, 10, 20, 30, 40, 50])]
    # Convert anniversary_year to string type
    tb["anniversary_year"] = tb["anniversary_year"].astype(str)

    # Rename anniversary_year values
    tb["anniversary_year"] = tb["anniversary_year"].replace(
        {
            "5": "< 5 years",
            "10": "< 10 years",
            "20": "< 20 years",
            "30": "< 30 years",
            "40": "< 40 years",
            "50": "< 50 years",
        }
    )
    tb = tb.rename(columns={"anniversary_year": "country"})

    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=ds_garden.metadata)

    # Save changes in the new grapher dataset.
    ds_grapher.save()
