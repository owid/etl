"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("ai_investment")

    # Read table from garden dataset.
    tb = ds_garden["ai_investment"].reset_index()

    industries = {
        "merger_acquisition": " Merger/acquisition",
        "minority_stake": "Minority stake",
        "private_investment": "Private investment",
        "public_offering": "Public offering",
    }
    tb = tb[list(industries.keys()) + ["year", "country"]]

    tb = tb.melt(id_vars=["year", "country"], var_name="investment_type", value_name="value")
    tb["investment_type"] = tb["investment_type"].replace(industries)
    tb = tb.pivot(index=["year", "investment_type"], columns="country", values="value").reset_index()
    tb = tb.rename(columns={"investment_type": "country"})
    # Only keep the world data (others are NaN for corporate investment)
    tb = tb[["year", "country", "World"]]

    tb = tb.format(["year", "country"])
    tb.metadata.short_name = paths.short_name

    #
    # Save outputs.
    #

    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_garden.metadata
    )

    # Save changes in the new grapher dataset.
    ds_grapher.save()
