"""Load a meadow dataset and create a garden dataset."""
import owid.catalog.processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds = paths.load_dataset("faostat_rl")
    tb = ds["faostat_rl"].reset_index()
    tb = tb[["country", "year", "element", "item", "unit", "value"]]
    tb_primary = tb[(tb["item"] == "Primary Forest") & (tb["unit"] == "hectares")]
    tb_land_area = tb[(tb["item"] == "Land area") & (tb["unit"] == "hectares")]

    tb = pr.merge(
        tb_primary,
        tb_land_area,
        how="left",
        suffixes=("_primary_forest", "_land_area"),
        validate="one_to_one",
        on=["country", "year", "element", "unit"],
    )
    tb["primary_forest_share"] = (tb["value_primary_forest"] / tb["value_land_area"]) * 100
    tb = tb.drop(columns=["item_primary_forest", "item_land_area", "unit", "element"])
    tb = tb.format(
        [
            "country",
            "year",
        ],
        short_name="primary_forest",
    )
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    # Save changes in the new garden dataset.
    ds_garden.save()
