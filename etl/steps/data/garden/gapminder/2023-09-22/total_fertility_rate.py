"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
from owid.catalog import Table

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
    ds_fertility = paths.load_dataset("un_wpp")
    # Read Gapminder table from meadow dataset.
    tb = ds_meadow["total_fertility_rate"].reset_index()
    tb["source"] = "Gapminder"
    # Read long run child mortality table from meadow dataset.
    tb_cm = ds_child_mortality["long_run_child_mortality_selected"].reset_index().sort_values(["country", "year"])
    # Reaf UN WPP fertility data from meadow dataset.
    tb_fertility = ds_fertility["fertility"].reset_index().sort_values(["location", "year"])
    tb_fertility = tb_fertility[
        (tb_fertility["variant"] == "estimates")
        & (tb_fertility["age"] == "all")
        & (tb_fertility["metric"] == "fertility_rate")
    ]
    tb_fertility = tb_fertility.rename(columns={"location": "country", "value": "fertility_rate"}).drop(
        columns=["variant", "age", "metric", "sex"]
    )
    tb_fertility["source"] = "un_wpp"

    # Process data.
    #
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Combine the two fertility datasets with a preference for the UN WPP data when there is a conflict.
    tb = combine_datasets(tb_wpp=tb_fertility, tb_gap=tb, table_name="fertility_rate", preferred_source="un_wpp")
    tb = tb.drop(columns=["source"])
    # Merge with child mortality
    tbm = tb.merge(tb_cm, on=["country", "year"], how="inner")
    tbm["under_five_mortality"] = tbm["under_five_mortality"] / 100
    # Calculate the number of children that die before age five, per woman
    tbm["children_dying_before_five_per_woman"] = tbm["under_five_mortality"] * tbm["fertility_rate"]
    # Calculate the number of children that survive past age five, per woman
    tbm["children_surviving_past_five_per_woman"] = tbm["fertility_rate"] - tbm["children_dying_before_five_per_woman"]
    # tidy up
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


def combine_datasets(tb_wpp: Table, tb_gap: Table, table_name: str, preferred_source: str) -> Table:
    """
    Combine UN WPP and Gapminder data.
    """
    tb_combined = pr.concat([tb_wpp, tb_gap]).sort_values(["country", "year", "source"])
    assert preferred_source in tb_combined["source"].unique()
    tb_combined.metadata.short_name = table_name
    tb_combined = remove_duplicates(tb_combined, preferred_source=preferred_source)

    return tb_combined


def remove_duplicates(tb: Table, preferred_source: str) -> Table:
    """
    Removing rows where there are overlapping years with a preference for a particular source of data.

    """
    assert tb["source"].str.contains(preferred_source).any()

    duplicate_rows = tb.duplicated(subset=["country", "year"], keep=False)

    tb_no_duplicates = tb[~duplicate_rows]

    tb_duplicates = tb[duplicate_rows]

    tb_duplicates_removed = tb_duplicates[tb_duplicates["source"] == preferred_source]

    tb = pr.concat([tb_no_duplicates, tb_duplicates_removed])

    assert len(tb[tb.duplicated(subset=["country", "year"], keep=False)]) == 0

    return tb
