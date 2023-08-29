"""Load a meadow dataset and create a garden dataset."""
import owid.catalog.processing as pr
from owid.catalog import Dataset, Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset
from etl.paths import DATA_DIR

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Gapminder dataset location - it doesn't seem to load through regular dependencies as open_numbers isn't included as a channel
DATASET_GAPMINDER_CHILD_MORTALITY = DATA_DIR / "open_numbers" / "open_numbers" / "latest" / "gapminder__child_mortality"

GAPMINDER_SOURCE_NAME = "gapminder"


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_igme = paths.load_dataset("igme")

    # Read table from meadow dataset.
    tb = ds_igme["igme"].reset_index()

    # Select out columns of interest.
    cols = [
        "country",
        "year",
        "observation_value__deaths_per_1_000_live_births__under_five_mortality_rate__both_sexes__all_wealth_quintiles",
    ]
    tb = tb[cols]
    tb = tb.rename(
        columns={
            "observation_value__deaths_per_1_000_live_births__under_five_mortality_rate__both_sexes__all_wealth_quintiles": "under_five_mortality_rate"
        }
    )

    tb["source"] = "igme"
    # Load Gapminder data for years before 2016 - after this predictions are used from UN WPP
    ds_gapminder, tb_gapminder = load_gapminder_data(maximum_year=2016)
    #
    # Process Gapminder data.
    #
    tb_gapminder = geo.harmonize_countries(
        df=tb_gapminder,
        countries_file=paths.country_mapping_path,
    )

    # Combine IGME and Gapminder data
    tb_combined = pr.concat([tb, tb_gapminder]).sort_values(["country", "year", "source"])
    tb_combined.metadata.short_name = "long_run_child_mortality"
    # For overlapping years, prefer IGME data
    tb_combined = remove_duplicates(tb_combined, preferred_source="igme")
    #
    # Save outputs.
    tb_combined = tb_combined.drop(columns=["source"]).set_index(["country", "year"], verify_integrity=True)

    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb_combined], check_variables_metadata=True, default_metadata=ds_gapminder.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def load_gapminder_data(maximum_year: int) -> Table:
    """
    Load gapminder dataset and subset it to years before `maximum_year`.
    See: https://www.gapminder.org/data/documentation/gd005/
    """
    ds = Dataset(DATASET_GAPMINDER_CHILD_MORTALITY)
    tb = ds["child_mortality_0_5_year_olds_dying_per_1000_born"]

    # reset index
    tb = tb.reset_index()

    # add source
    tb["source"] = GAPMINDER_SOURCE_NAME
    # columns
    tb = tb.rename(
        columns={
            "time": "year",
            "geo": "country",
            "child_mortality_0_5_year_olds_dying_per_1000_born": "under_five_mortality_rate",
        }
    )
    # tb = tb.copy(deep=True)
    msk = tb["year"] <= maximum_year
    tb = tb[msk]

    # output columns
    tb = tb[["country", "year", "under_five_mortality_rate", "source"]]

    return ds, tb


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
