import pandas as pd
from owid import catalog
from owid.catalog.utils import underscore
from owid.datautils import dataframes
from structlog import get_logger

from etl.helpers import PathFinder

log = get_logger()

paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    # Load data from fast track
    ds_eisner = paths.load_dependency("long_term_homicide_rates_in_europe")
    # Get the required table
    tb_eisner = ds_eisner["long_term_homicide_rates_in_europe"]
    # Create a convenient dataframe
    tb_eisner = tb_eisner.rename(
        columns={"homicide_rate_in_europe_over_long_term__per_100_000": "death_rate_per_100_000_population"}
    )
    tb_eisner["source"] = "Eisner"

    eisner_entities = tb_eisner["country"].drop_duplicates()
    # Get both the WHO and UNODC datasets from garden
    df_who = get_who_mortality_db()
    df_unodc = get_unodc()

    # Combine both datasets with Eisner
    df_who_long = combine_datasets(tb_eisner, df_who)
    df_unodc_long = combine_datasets(tb_eisner, df_unodc)

    df_eisner = df_eisner.rename(
        columns={"death_rate_per_100_000_population": "death_rate_per_100_000_population_eisner"}
    ).drop(columns="source")
    df_combined = dataframes.multi_merge([df_eisner, df_who_long, df_unodc_long], on=["country", "year"], how="outer")

    # We only want entities for which long-run data is available in grapher - so let's drop all the others

    df_combined = df_combined[df_combined["country"].isin(eisner_entities)]

    # Create Table and add short_name
    df_combined = catalog.Table(df_combined.reset_index(drop=True), short_name="homicide_long_run_omm")

    # Save outputs
    ds_garden = catalog.Dataset.create_empty(dest_dir)

    ds_garden.add(df_combined)

    ds_garden.update_metadata(paths.metadata_path, if_source_exists="append")

    ds_garden.save()


def get_who_mortality_db() -> pd.DataFrame:
    """
    Get the homicide rate from the WHO Mortality Database Dataset
    """

    ds_who_db = paths.load_dataset("who_mort_db")
    df_who = ds_who_db["who_mort_db"].reset_index()
    df_who = pd.DataFrame(df_who[["country", "year", "death_rate_per_100_000_population_both_sexes_all_ages"]])
    df_who = df_who.dropna(subset="death_rate_per_100_000_population_both_sexes_all_ages")
    df_who = df_who.rename(
        columns={"death_rate_per_100_000_population_both_sexes_all_ages": "death_rate_per_100_000_population"}
    )
    df_who["source"] = "WHO"
    return df_who


def get_unodc() -> pd.DataFrame:
    """
    Get the homicide rate from the UNODC
    """

    ds_unodc: catalog.Dataset = paths.load_dependency("unodc")
    df_unodc = ds_unodc["total"]
    df_unodc = pd.DataFrame(df_unodc["rate_per_100_000_population_both_sexes_all_ages"]).reset_index()
    df_unodc = df_unodc.dropna(subset="rate_per_100_000_population_both_sexes_all_ages")
    df_unodc = df_unodc.rename(
        columns={"rate_per_100_000_population_both_sexes_all_ages": "death_rate_per_100_000_population"}
    )
    df_unodc["source"] = "UNODC"
    return df_unodc


def combine_datasets(eisner: pd.DataFrame, recent_df: pd.DataFrame) -> pd.DataFrame:
    # Combine the Eisner dataset with a more recent dataset
    df_combined = pd.merge(
        eisner, recent_df, how="outer", on=["country", "year", "death_rate_per_100_000_population", "source"]
    )

    assert df_combined.shape[0] <= eisner.shape[0] + recent_df.shape[0]

    # Remove duplicates with a preference for either WHO or UNODC over Eisner
    df_combined = df_combined.sort_values("source")
    # Dropping values which are duplicated for country and year and where Eisner is the source.
    df_combined = df_combined[
        ~((df_combined.duplicated(subset=["country", "year"], keep=False)) & (df_combined["source"] == "Eisner"))
    ]

    colname = "death_rate_per_100_000_population_" + underscore(
        "_".join(df_combined["source"].drop_duplicates().to_list())
    )

    df_combined = df_combined.rename(columns={"death_rate_per_100_000_population": colname})
    df_combined = df_combined.drop(columns="source")

    return df_combined
