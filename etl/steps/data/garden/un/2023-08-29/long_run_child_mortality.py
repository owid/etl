"""Load a meadow dataset and create a garden dataset."""
import owid.catalog.processing as pr
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_igme = paths.load_dataset("igme")
    ds_gapminder = paths.load_dataset("under_five_mortality")
    # Read table from meadow dataset.
    tb_igme = ds_igme["igme"].reset_index()

    # Select out columns of interest.
    cols = [
        "country",
        "year",
        "observation_value_deaths_per_1_000_live_births_under_five_mortality_rate_both_sexes_all_wealth_quintiles",
    ]
    tb_igme = tb_igme[cols]
    tb_igme = tb_igme.rename(
        columns={
            "observation_value_deaths_per_1_000_live_births_under_five_mortality_rate_both_sexes_all_wealth_quintiles": "under_five_mortality"
        }
    )

    tb_igme["source"] = "igme"
    # Load full Gapminder data
    tb_gap = ds_gapminder["under_five_mortality"].reset_index()
    tb_gap["source"] = "gapminder"
    #
    tb_gap_sel = ds_gapminder["under_five_mortality_selected"].reset_index()
    tb_gap_sel["source"] = "gapminder"

    # Combine IGME and Gapminder data
    tb_combined = combine_datasets(tb_igme, tb_gap, "long_run_child_mortality")
    tb_combined_sel = combine_datasets(tb_igme, tb_gap_sel, "long_run_child_mortality_selected")

    #
    # Save outputs.
    tb_combined = tb_combined.drop(columns=["source"]).set_index(["country", "year"], verify_integrity=True)
    tb_combined_sel = tb_combined_sel.drop(columns=["source"]).set_index(["country", "year"], verify_integrity=True)
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_combined, tb_combined_sel], check_variables_metadata=True)
    ds_garden.update_metadata(metadata_path=paths.metadata_path)
    # Save changes in the new garden dataset.
    ds_garden.save()


def combine_datasets(tb_igme: Table, tb_gap: Table, table_name: str) -> Table:
    """
    Combine IGME and Gapminder data.
    """
    tb_combined = pr.concat([tb_igme, tb_gap]).sort_values(["country", "year", "source"])
    tb_combined.metadata.short_name = table_name
    tb_combined = remove_duplicates(tb_combined, preferred_source="igme")

    return tb_combined


def remove_duplicates(tb: Table, preferred_source: str) -> Table:
    """
    Removing rows where there are overlapping years with a preference for IGME data.

    """
    assert tb["source"].str.contains(preferred_source).any()

    duplicate_rows = tb.duplicated(subset=["country", "year"], keep=False)

    tb_no_duplicates = tb[~duplicate_rows]

    tb_duplicates = tb[duplicate_rows]

    tb_duplicates_removed = tb_duplicates[tb_duplicates["source"] == preferred_source]

    tb = pr.concat([tb_no_duplicates, tb_duplicates_removed])

    assert len(tb[tb.duplicated(subset=["country", "year"], keep=False)]) == 0

    return tb
