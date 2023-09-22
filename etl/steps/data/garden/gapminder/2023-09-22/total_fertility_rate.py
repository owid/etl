"""Load a meadow dataset and create a garden dataset."""

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("total_fertility_rate")
    ds_child_mortality = paths.load_dataset("long_run_child_mortality")
    # Read table from meadow dataset.
    tb = ds_meadow["total_fertility_rate"].reset_index()
    tb_cm = ds_child_mortality["long_run_child_mortality_selected"].reset_index().sort_values(["country", "year"])

    #
    # Process data.
    #
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Merge with child mortality

    tbm = tb.merge(tb_cm, on=["country", "year"], how="inner")
    tbm["under_five_mortality"] = tbm["under_five_mortality"] / 1000
    tbm["children_dying_before_five_per_woman"] = tbm["under_five_mortality"] * tbm["fertility_rate"]
    tbm["children_surviving_past_five_per_woman"] = tbm["fertility_rate"] - tbm["children_dying_before_five_per_woman"]
    tbm = tbm.drop(columns=["under_five_mortality"])

    tbm = tbm.set_index(["country", "year"], verify_integrity=True)

    #

    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tbm], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
