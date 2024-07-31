from owid.catalog import Table
from owid.catalog import processing as pr
from owid.catalog.utils import underscore
from owid.datautils import dataframes
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

log = get_logger()

paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    # Load data from fast track
    ds_eisner = paths.load_dataset("long_term_homicide_rates_in_europe")
    # Get the required table
    tb_eisner = ds_eisner["long_term_homicide_rates_in_europe"]
    tb_eisner = tb_eisner.rename(
        columns={"homicide_rate_in_europe_over_long_term__per_100_000": "death_rate_per_100_000_population"}
    )
    tb_eisner["source"] = "Eisner"

    eisner_entities = tb_eisner["country"].drop_duplicates()
    # Get both the WHO and UNODC datasets from garden
    tb_who = get_who_mortality_db()
    tb_unodc = get_unodc()

    # Combine both datasets with Eisner
    tb_who_long = combine_datasets(tb_eisner, tb_who)
    tb_unodc_long = combine_datasets(tb_eisner, tb_unodc)

    tb_eisner = tb_eisner.rename(
        columns={"death_rate_per_100_000_population": "death_rate_per_100_000_population_eisner"}
    ).drop(columns="source")
    tb_combined = dataframes.multi_merge([tb_eisner, tb_who_long, tb_unodc_long], on=["country", "year"], how="outer")

    # We only want entities for which long-run data is available in grapher - so let's drop all the others

    tb_combined = tb_combined[tb_combined["country"].isin(eisner_entities)]

    tb = tb_combined.format(["country", "year"], short_name="homicide_long_run_omm")
    # Save outputs
    ds_garden = create_dataset(
        dest_dir,
        tables=[tb],
        check_variables_metadata=True,
        default_metadata=ds_eisner.metadata,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def get_who_mortality_db() -> Table:
    """
    Get the homicide rate from the WHO Mortality Database Dataset
    """

    ds_who_db = paths.load_dataset("who_mort_db")
    tb_who = ds_who_db["who_mort_db"].reset_index()
    # Just pull out the rows for all ages and both sexes
    tb_who = tb_who[(tb_who["age_group"] == "All ages") & (tb_who["sex"] == "Both Sexes")]
    assert tb_who.set_index(["country", "year"]).index.is_unique and tb_who.shape[0] > 0
    # Grab the columns we need
    tb_who = tb_who[["country", "year", "death_rate_per_100_000_population"]]
    tb_who = tb_who.dropna(subset="death_rate_per_100_000_population")
    tb_who["source"] = "WHO"
    return tb_who


def get_unodc() -> Table:
    """
    Get the homicide rate from the UNODC
    """

    ds_unodc = paths.load_dataset("unodc")
    tb_unodc = ds_unodc["total"].reset_index()
    tb_unodc = tb_unodc[["country", "year", "rate_per_100_000_population_both_sexes_all_ages"]]
    tb_unodc = tb_unodc.dropna(subset="rate_per_100_000_population_both_sexes_all_ages")
    tb_unodc = tb_unodc.rename(
        columns={"rate_per_100_000_population_both_sexes_all_ages": "death_rate_per_100_000_population"}
    )
    tb_unodc["source"] = "UNODC"
    return tb_unodc


def combine_datasets(eisner: Table, recent_df: Table) -> Table:
    # Combine the Eisner dataset with a more recent dataset
    df_combined = pr.merge(
        eisner, recent_df, how="outer", on=["country", "year", "death_rate_per_100_000_population", "source"]
    )

    assert df_combined.shape[0] <= eisner.shape[0] + recent_df.shape[0]

    # Remove duplicates with a preference for either WHO or UNODC over Eisner
    df_combined = df_combined.sort_values("source")
    # Dropping values which are duplicated for country and year and where Eisner is the source.
    df_combined = df_combined[
        ~((df_combined.duplicated(subset=["country", "year"], keep=False)) & (df_combined["source"] == "Eisner"))
    ].reset_index(drop=True)

    colname = "death_rate_per_100_000_population_" + underscore(
        "_".join(df_combined["source"].drop_duplicates().to_list())
    )

    df_combined = df_combined.rename(columns={"death_rate_per_100_000_population": colname})
    df_combined = df_combined.drop(columns="source")

    return df_combined
