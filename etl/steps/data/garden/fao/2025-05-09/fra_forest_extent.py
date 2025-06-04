"""Load a meadow dataset and create a garden dataset."""

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("fra_forest_extent")
    ds_meadow_fao_rl = paths.load_dataset("faostat_rl")

    # Read table from meadow dataset.
    tb = ds_meadow.read("fra_forest_area")
    tb_fao_rl = ds_meadow_fao_rl["faostat_rl_flat"].reset_index()
    tb_fao_rl = tb_fao_rl[["country", "year", "country_area__00006600__area__005110__hectares"]]
    tb_fao_rl = tb_fao_rl.rename(columns={"country_area__00006600__area__005110__hectares": "country_area"}).dropna(
        subset=["country_area"]
    )
    #
    # Process data.
    #
    # Harmonize country names.
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    tb = tb.merge(
        tb_fao_rl,
        how="left",
        left_on=["country", "year"],
        right_on=["country", "year"],
    )
    # Convert units from 1000 hectares to hectares.
    tb["forest_area"] = tb["forest_area"] * 1000
    tb["forest_share"] = (tb["forest_area"] / tb["country_area"]) * 100
    assert tb["forest_share"].max() <= 100, "Forest share cannot be greater than 100%"
    assert tb["forest_share"].min() >= 0, "Forest share cannot be less than 0%"
    # Improve table format.
    tb = tb.drop(columns=["country_area", "forest_area"])
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()
