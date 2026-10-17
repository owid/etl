from owid.catalog import Table, VariableMeta
from owid.catalog import processing as pr
from owid.catalog.utils import underscore
from structlog import get_logger

from etl.helpers import PathFinder

log = get_logger()

paths = PathFinder(__file__)


def run() -> None:
    # Load data from fast track
    snap_eisner = paths.load_snapshot("long_term_homicide_rates_in_europe.csv")
    # Get the required table
    tb_eisner = snap_eisner.read()
    tb_eisner = tb_eisner.rename(
        columns={"homicide_rate_in_europe_over_long_term__per_100_000": "death_rate_per_100_000_population"}
    )
    tb_eisner["source"] = "Eisner"

    eisner_entities = tb_eisner["country"].drop_duplicates()
    # Get both the WHO and UNODC datasets from garden
    tb_who = get_who_mortality_db()

    # Combine both datasets with Eisner
    tb_who_long = combine_datasets(tb_eisner, tb_who)

    tb_eisner = tb_eisner.rename(
        columns={"death_rate_per_100_000_population": "death_rate_per_100_000_population_eisner"}
    ).drop(columns="source")
    tb_combined = pr.merge(tb_eisner, tb_who_long, on=["country", "year"], how="outer")

    # We only want entities for which long-run data is available in grapher - so let's drop all the others

    tb_combined = tb_combined[tb_combined["country"].isin(eisner_entities)]

    tb = tb_combined.format(["country", "year"], short_name="homicide_long_run_omm")
    # Save outputs
    ds_garden = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=snap_eisner.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()


def get_who_mortality_db() -> Table:
    """
    Get the homicide rate from the WHO Mortality Database Dataset
    """

    # Violence (assault, ICD-10 X85-Y09 and Y871) is WHO's name for homicide; it sits in the merged WHO Mortality
    # Database as a drill-down inside Intentional injuries.
    ds_who_db = paths.load_dataset("mortality_database")
    tb_who = ds_who_db.read("intentional_injuries")
    # The merged dataset names the metric `death_rate`; this step keeps the longer name its own indicators are built on.
    tb_who = tb_who.rename(columns={"death_rate": "death_rate_per_100_000_population"}, errors="raise")
    # Just pull out the rows for violence, all ages and both sexes
    tb_who = tb_who[
        (tb_who["cause"] == "Violence") & (tb_who["age_group"] == "all ages") & (tb_who["sex"] == "Both sexes")
    ]
    assert tb_who.set_index(["country", "year"]).index.is_unique and tb_who.shape[0] > 0
    # Grab the columns we need
    tb_who = tb_who[["country", "year", "death_rate_per_100_000_population"]]
    tb_who = tb_who.dropna(subset="death_rate_per_100_000_population")
    # The merged dataset's text is templated per cause, sex and age group and is rendered by its own grapher step;
    # here the column has no dimensions left, so keep only its origins and let this step's metadata describe it.
    rate = tb_who["death_rate_per_100_000_population"]
    tb_who["death_rate_per_100_000_population"] = rate.copy_metadata(rate)  # a fresh Variable
    tb_who["death_rate_per_100_000_population"].metadata = VariableMeta(origins=rate.metadata.origins)
    tb_who["source"] = "WHO"
    return tb_who


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
