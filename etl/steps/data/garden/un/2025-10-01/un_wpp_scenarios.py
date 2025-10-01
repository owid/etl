# from deaths import process as process_deaths
# from demographics import process as process_demographics
# from dep_ratio import process as process_depratio
# from fertility import process as process_fertility

from owid.catalog import Table
from owid.catalog import processing as pr

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("un_wpp_scenarios")
    ds_meadow_med = paths.load_dataset("un_wpp")
    # Load tables - population
    tb = ds_meadow.read("un_wpp_demographic_indicators_scenarios")
    tb = geo.harmonize_countries(
        tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )
    tb_med = ds_meadow_med.read("population")
    tb_med = get_medium_population(tb_med)
    # Load tables - life expectancy
    tb_le = ds_meadow_med.read("life_expectancy")
    tb_le = get_medium_life_expectancy(tb_le)
    # Load tables fertility
    tb_fert = ds_meadow_med.read("fertility_rate")
    tb_fert = get_medium_fertility_rate(tb_fert)
    # Load tables net migration
    tb_mig = ds_meadow_med.read("migration")
    tb_mig = get_net_migration(tb_mig)
    # Process data.
    tb_scen = pr.multi_merge(
        [tb_med, tb_le, tb_fert, tb_mig],
        on=[
            "country",
            "year",
            "variant",
        ],
        how="left",
    )
    tb = pr.concat([tb, tb_scen], ignore_index=True)

    tb = tb.format(["country", "year", "variant"])

    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata, repack=False
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def get_net_migration(tb_mig: Table) -> Table:
    tb_mig = tb_mig[tb_mig["variant"].isin(["medium", "estimates"])]
    tb_mig = tb_mig[(tb_mig["age"] == "all") & (tb_mig["sex"] == "all")]
    tb_mig = tb_mig.drop(columns=["sex", "net_migration_rate", "age"])
    tb_mig = tb_mig.replace({"variant": {"estimates": "Estimates", "medium": "Medium"}})
    return tb_mig


def get_medium_fertility_rate(tb_fert: Table) -> Table:
    tb_fert = tb_fert[tb_fert["variant"].isin(["medium", "estimates"])]
    tb_fert = tb_fert[(tb_fert["age"] == "all") & (tb_fert["sex"] == "all")]
    tb_fert = tb_fert.drop(
        columns=[
            "age",
        ]
    )
    tb_fert = tb_fert.drop(columns=["sex"])
    tb_fert = tb_fert.replace({"variant": {"estimates": "Estimates", "medium": "Medium"}})
    tb_fert = tb_fert.rename(columns={"fertility_rate": "total_fertility_rate"})
    return tb_fert


def get_medium_population(tb_med: Table) -> Table:
    tb_med = tb_med[tb_med["variant"].isin(["medium", "estimates"])]
    tb_med = tb_med[tb_med["age"] == "all"]
    tb_med = tb_med.drop(columns=["population_change", "population_density"])

    pivoted = tb_med.pivot_table(
        index=["country", "year", "variant"],  # keep these as identifiers
        columns="sex",  # values to spread across columns
        values="population",
        aggfunc="first",  # no aggregation needed, just take values
    ).reset_index()

    # Rename columns for clarity
    tb_med = pivoted.rename(
        columns={"all": "population_july", "male": "population_male_july", "female": "population_female_july"}
    )
    tb_med = tb_med.replace({"variant": {"estimates": "Estimates", "medium": "Medium"}})
    return tb_med


def get_medium_life_expectancy(tb_le: Table) -> Table:
    tb_le = tb_le[tb_le["variant"].isin(["medium", "estimates"])]
    tb_le = tb_le[(tb_le["age"] == 0) & (tb_le["sex"] == "all")]
    tb_le = tb_le.drop(columns=["age", "sex"])
    tb_le = tb_le.replace({"variant": {"estimates": "Estimates", "medium": "Medium"}})
    return tb_le
