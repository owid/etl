from typing import Dict, List, Optional, Tuple, cast

import numpy as np
import owid.catalog.processing as pr

# from deaths import process as process_deaths
# from demographics import process as process_demographics
# from dep_ratio import process as process_depratio
# from fertility import process as process_fertility
from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

YEAR_SPLIT = 2024
COLUMNS_INDEX = ["country", "year", "sex", "age", "variant"]
COLUMNS_INDEX_MONTH = COLUMNS_INDEX + ["month"]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("un_wpp")

    # Load tables
    tb_population = ds_meadow.read("population")
    tb_population_density = ds_meadow.read("population_density")
    tb_growth_rate = ds_meadow.read("growth_rate")
    tb_nat_change = ds_meadow.read("natural_change_rate")
    tb_fertility = ds_meadow.read("fertility_rate")
    tb_fertility_births_single = ds_meadow.read("fertility_births_single")
    tb_migration = ds_meadow.read("net_migration")
    tb_migration_rate = ds_meadow.read("net_migration_rate")
    tb_deaths = ds_meadow.read("deaths")
    tb_death_rate = ds_meadow.read("death_rate")
    tb_births = ds_meadow.read("births")
    tb_birth_rate = ds_meadow.read("birth_rate")
    tb_median_age = ds_meadow.read("median_age")
    tb_le = ds_meadow.read("life_expectancy")
    tb_mortality = ds_meadow.read("mortality_rate")
    tb_childbearing_age = ds_meadow.read("mean_age_childbearing")

    #
    # Process data.
    #

    ## Population, Sex ratio
    tb_population_jan, tb_population, tb_sex_ratio = process_population_sex_ratio(tb_population, tb_population_density)
    tb_population = tb_population.format(COLUMNS_INDEX)
    tb_population_jan = tb_population_jan.format(COLUMNS_INDEX, short_name="population_january")  # January data

    ## Sex ratio
    tb_sex_ratio = set_variant_to_estimates(tb_sex_ratio)
    tb_sex_ratio = tb_sex_ratio.format(COLUMNS_INDEX, short_name="sex_ratio")

    ## Dependency ratio
    tb_dependency = process_dependency(tb_population)
    tb_dependency = tb_dependency.format(COLUMNS_INDEX, short_name="dependency_ratio")

    ## Growth rate
    tb_growth_rate = process_standard(tb_growth_rate)
    tb_growth_rate = set_variant_to_estimates(tb_growth_rate)
    tb_growth_rate = tb_growth_rate.format(COLUMNS_INDEX)

    ## Natural growth rate
    tb_nat_change = process_standard(tb_nat_change)
    tb_nat_change = set_variant_to_estimates(tb_nat_change)
    tb_nat_change["natural_change_rate"] /= 10
    tb_nat_change = tb_nat_change.format(COLUMNS_INDEX)

    ## Migration
    tb_migration = process_migration(tb_migration, tb_migration_rate)
    del tb_migration_rate
    tb_migration = set_variant_to_estimates(tb_migration)
    tb_migration = tb_migration.format(COLUMNS_INDEX, short_name="migration")

    ## Deaths
    tb_deaths = process_deaths(tb_deaths, tb_death_rate)
    del tb_death_rate
    tb_deaths = tb_deaths.format(COLUMNS_INDEX, short_name="deaths")

    ## Births
    tb_births = process_births(tb_births, tb_birth_rate)
    del tb_birth_rate
    tb_births = set_variant_to_estimates(tb_births)
    tb_births = tb_births.format(COLUMNS_INDEX, short_name="births")

    ## Median age
    tb_median_age = process_standard(tb_median_age)
    tb_median_age = set_variant_to_estimates(tb_median_age)
    tb_median_age = tb_median_age.format(COLUMNS_INDEX)

    ## Fertility
    tb_fertility = process_fertility(tb_fertility)
    tb_fertility = set_variant_to_estimates(tb_fertility)
    tb_fertility = tb_fertility.format(COLUMNS_INDEX)

    ## Fertility by age (distribution)
    tb_fertility_births_single = process_fertility_births_single(tb_fertility_births_single)

    ## Life Expectancy
    tb_le = process_le(tb_le)
    tb_le = set_variant_to_estimates(tb_le)
    tb_le = tb_le.format(COLUMNS_INDEX)

    ## Mortality
    tb_mortality = process_mortality(tb_mortality)
    tb_mortality = set_variant_to_estimates(tb_mortality)
    tb_mortality = tb_mortality.format(COLUMNS_INDEX)

    ## Mean age at childbearing
    tb_childbearing_age = process_standard(tb_childbearing_age)
    tb_childbearing_age = set_variant_to_estimates(tb_childbearing_age)
    tb_childbearing_age = tb_childbearing_age.format(COLUMNS_INDEX)

    # Build tables list for dataset
    tables = [
        tb_population_jan,
        tb_population,
        tb_growth_rate,
        tb_nat_change,
        tb_fertility,
        tb_migration,
        tb_deaths,
        tb_births,
        tb_median_age,
        tb_le,
        tb_sex_ratio,
        tb_mortality,
        tb_dependency,
        tb_childbearing_age,
        tb_fertility_births_single,
    ]

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir,
        tables=tables,
        check_variables_metadata=True,
        default_metadata=ds_meadow.metadata,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def process_population_sex_ratio(tb: Table, tb_density: Table) -> Tuple[Table, Table, Table]:
    """Process the population table.

    Also estimate sex ratio.
    """
    paths.log.info("Processing population variables...")

    # Sanity check
    assert tb.notna().all(axis=None), "Some NaNs detected"

    # Scale
    tb["population"] *= 1000

    # As integer
    tb["population"] = tb["population"].astype(int)

    # Estimate sex ratio
    tb_sex = estimate_sex_ratio(tb)

    # Estimate age groups
    tb = estimate_age_groups(tb)

    # Add population change
    tb = add_population_change(tb)

    # Add sex ratio
    tb_sex = add_sex_ratio_all(tb_sex, tb)

    # Harmonize country names
    tb = harmonize_country_names(tb)
    tb_sex = harmonize_country_names(tb_sex)

    # Harmonize dimensions
    tb = harmonize_dimension(
        tb,
        "variant",
        mapping={
            "Medium": "medium",
            "Low": "low",
            "High": "high",
            "Constant fertility": "constant_fertility",
        },
    )
    tb_sex = harmonize_dimension(
        tb_sex,
        "variant",
        mapping={
            "Medium": "medium",
            "Low": "low",
            "High": "high",
            "Constant fertility": "constant_fertility",
        },
    )
    tb = harmonize_dimension(
        tb,
        "sex",
        {
            "female": "female",
            "male": "male",
            "all": "all",
        },
    )
    tb = harmonize_dimension(
        tb,
        "month",
        {
            "July": "july",
            "January": "january",
        },
    )

    # Multiply sex_ratio x 100
    tb_sex["sex_ratio"] *= 100
    # Ensure no infinit values
    tb_sex["sex_ratio"] = tb_sex["sex_ratio"].replace([float("inf"), float("-inf")], np.nan)

    # Set variants
    tb = set_variant_to_estimates(tb)

    # Add population density
    tb_density = process_standard(tb_density)
    tb = tb.merge(tb_density, on=COLUMNS_INDEX_MONTH, how="left")
    del tb_density

    # Age as sting
    tb["age"] = tb["age"].astype("string")
    tb_sex["age"] = tb_sex["age"].astype("string")
    # Separate january and july data
    msk_jan = tb["month"] == "january"
    tb_jan = tb[msk_jan]
    tb = tb[~msk_jan]
    tb = tb.drop(columns=["month"])
    tb_jan = tb_jan.drop(columns=["month"])
    # Remove january data for sex ratio
    tb_sex_msk = tb_sex["month"] == "January"
    tb_sex = tb_sex[~tb_sex_msk]
    tb_sex = tb_sex.drop(columns=["month"])

    return tb_jan, tb, tb_sex


def process_dependency(tb: Table) -> Table:
    """Get dependency ratios.

    Total: (0-14 + 65+) / 15-64
    Youth: 0-14 / 15-64
    Old: 65+ / 15-64
    """
    # Get relevant rows
    ages = ["0-14", "15-64", "65+"]
    tb_dependency = tb["population"].reset_index().copy()
    tb_dependency = tb_dependency.loc[tb_dependency["age"].isin(ages)]

    # Pivot table
    tb_dependency = tb_dependency.pivot(
        columns="age", index=[col for col in COLUMNS_INDEX if col != "age"], values="population"
    ).reset_index()

    # Estimate ratios
    tb_dependency["total"] = (tb_dependency["0-14"] + tb_dependency["65+"]) / tb_dependency["15-64"]
    tb_dependency["youth"] = tb_dependency["0-14"] / tb_dependency["15-64"]
    tb_dependency["old"] = tb_dependency["65+"] / tb_dependency["15-64"]

    # Drop unused column
    tb_dependency = tb_dependency.drop(columns=ages)

    # Unpivot
    tb_dependency = tb_dependency.melt(
        id_vars=[col for col in COLUMNS_INDEX if col != "age"],
        var_name="age",
        value_name="dependency_ratio",
    )

    # Scale
    tb_dependency["dependency_ratio"] *= 100

    return tb_dependency


def process_migration(tb_mig: Table, tb_mig_rate: Table) -> Table:
    """Process the migration tables.

    Clean the individual tables and combine them into one with two indicators.
    """
    # Basic processing
    tb_mig = process_standard(tb_mig)
    tb_mig_rate = process_standard(tb_mig_rate)

    # Standardise sex dimension values
    tb_mig = harmonize_dimension(
        tb_mig,
        "sex",
        mapping={
            # "female": "female",
            # "male": "male",
            "all": "all",
        },
    )

    # Merge
    tb = tb_mig.merge(tb_mig_rate, on=COLUMNS_INDEX, how="left")

    # Scale
    tb["net_migration"] *= 1000

    # Age as string
    tb["age"] = tb["age"].astype("string")

    # Zero value for "World"
    tb.loc[tb["country"] == "World", ["net_migration", "net_migration_rate"]] = 0

    return tb


def process_deaths(tb: Table, tb_rate: Table) -> Table:
    """Process the migration tables.

    Clean the individual tables and combine them into one with two indicators.
    """
    # Basic processing
    tb = process_standard(tb)
    tb = set_variant_to_estimates(tb)
    tb_rate = process_standard(tb_rate)

    # Standardise sex dimension values
    tb = harmonize_dimension(
        tb,
        "sex",
        mapping={
            "female": "female",
            "male": "male",
            "all": "all",
        },
    )
    tb = harmonize_dimension(
        tb,
        "age",
        mapping={
            "all": "all",
        },
        strict=False,
    )

    # Total
    tb_total = tb.groupby(["country", "year", "sex", "variant"], as_index=False, observed=True)["deaths"].sum()
    tb_total = tb_total.assign(age="all")

    # Get 5-year age groups from 0 to 100
    age_group_mapping = {str(i): f"{i//5 * 5}-{i//5 * 5 + 4}" for i in range(0, 100, 1)}
    tb_5 = tb.copy()
    tb_5["age"] = tb_5["age"].map(age_group_mapping)
    tb_5 = cast(Table, tb_5.dropna(subset=["age"]))
    tb_5 = tb_5.groupby(COLUMNS_INDEX, as_index=False, observed=True)["deaths"].sum()

    # Get 10-year age groups from 0 to 100
    age_group_mapping = {str(i): f"{i//10 * 10}-{i//10 * 10 + 9}" for i in range(0, 100, 1)}
    tb_10 = tb.copy()
    tb_10["age"] = tb_10["age"].map(age_group_mapping)
    tb_10 = cast(Table, tb_10.dropna(subset=["age"]))
    tb_10 = tb_10.groupby(COLUMNS_INDEX, as_index=False, observed=True)["deaths"].sum()

    # Special groups
    age_group_mapping = {
        "1": "1-4",
        "2": "1-4",
        "3": "1-4",
        "4": "1-4",
    }
    tb_x = tb.copy()
    tb_x["age"] = tb_x["age"].map(age_group_mapping)
    tb_x = cast(Table, tb_x.dropna(subset=["age"]))
    tb_x = tb_x.groupby(COLUMNS_INDEX, as_index=False, observed=True)["deaths"].sum()
    tb_limits = tb.loc[tb["age"].isin(["0", "100+"])].copy()

    # Combine
    tb = pr.concat([tb_total, tb_5, tb_10, tb_x, tb_limits], ignore_index=True)

    # Scale
    tb["deaths"] *= 1000

    # Merge
    tb = tb.merge(tb_rate, on=COLUMNS_INDEX, how="outer")

    # Age as string
    tb["age"] = tb["age"].astype("string")
    return tb


def process_births(tb: Table, tb_rate: Table) -> Table:
    """Process the migration tables.

    Clean the individual tables and combine them into one with two indicators.
    """
    # Basic processing
    tb = process_standard(tb)
    tb_rate = process_standard(tb_rate)

    # Standardise sex/age dimension values
    tb = harmonize_dimension(
        tb,
        "sex",
        mapping={
            # "female": "female",
            # "male": "male",
            "all": "all",
        },
    )
    tb = harmonize_dimension(
        tb,
        "age",
        mapping={
            "all": "all",
        },
        strict=False,
    )

    # Scale
    tb["births"] *= 1000

    # Merge
    tb = tb.merge(tb_rate, on=COLUMNS_INDEX, how="left")

    # Drop 55-59 age group in births (is all zero!)
    assert (tb.loc[tb["age"] == "55-59", "births"] == 0).all(), "Unexpected non-zero births values for age group 55-59."
    tb = tb.loc[tb["age"] != "55-59"]

    return tb


def process_fertility_births_single(tb: Table) -> Table:
    LAST_YEAR = 2023

    # Standard processing
    tb = process_standard(tb)
    # Keep only data pre-2023 (no projections)
    tb = tb.loc[tb["year"] <= LAST_YEAR]
    # Check
    assert set(tb["sex"].unique()) == {"all"}, "Unexpected sex group"
    # Drop unused columns
    tb = tb.drop(columns=["variant", "sex"])
    # Rename year column (will be used as secondary dimension)
    tb = tb.rename(
        columns={
            "year": "year_as_dimension",
        }
    )

    # Scale
    tb["fertility_rate"] /= 1_000

    # Keep data only every 5 years, and 2023
    tb = tb.loc[(tb["year_as_dimension"] % 5 == 0) | (tb["year_as_dimension"] == LAST_YEAR)]

    # Format
    tb = tb.format(["country", "age", "year_as_dimension"], short_name="fertility_single")

    # Keep relevant columns
    tb = tb.loc[:, ["fertility_rate"]]

    return tb


def process_fertility(tb: Table) -> Table:
    # Basic processing
    tb = process_standard(tb)

    # Standardise sex dimension values
    tb = harmonize_dimension(
        tb,
        "age",
        mapping={
            "Total": "all",
        },
        strict=False,
    )

    # Drop 55-59 age group in fertility (is all zero!)
    assert (
        tb.loc[tb["age"] == "55-59", "fertility_rate"] == 0
    ).all(), "Unexpected non-zero fertility rate values for age group 55-59."
    tb = tb.loc[tb["age"] != "55-59"]

    # Age as string
    tb["age"] = tb["age"].astype("string")

    return tb


def process_le(tb: Table) -> Table:
    """Process the life expectancy table."""
    # Basic processing
    tb = process_standard(tb)

    # Standardise sex dimension values
    tb = harmonize_dimension(
        tb,
        "sex",
        mapping={
            "female": "female",
            "male": "male",
            "all": "all",
        },
    )

    assert set(tb["age"].unique()) == {"0", "15", "65", "80"}, "Unexpected age values!"
    # assert set(tb["age"].unique()) == {str(i) for i in range(0, 100, 5)} | {"1", "100+"}, "Unexpected age values!"

    # Replace 100+ with 100
    tb["age"] = tb["age"].astype(int)
    # tb["age"] = tb["age"].replace("100+", "100").astype(int)

    # Estimate actual life expectancy (not years left)
    tb["life_expectancy"] += tb["age"]

    return tb


def process_mortality(tb: Table) -> Table:
    """Process the mortality table."""
    # Basic processing
    tb = process_standard(tb)

    # Standardise sex dimension values
    tb = harmonize_dimension(
        tb,
        "sex",
        mapping={
            # "female": "female",
            # "male": "male",
            "all": "all",
        },
    )

    # per 1,000 -> per 100
    tb["mortality_rate"] /= 10

    # Age as string
    tb["age"] = tb["age"].astype("string")

    return tb


def process_standard(tb: Table, allowed_nans: Optional[Dict[str, int]] = None) -> Table:
    """Process the population table."""
    paths.log.info("Processing population variables...")

    # Sanity check
    if allowed_nans:
        for colname, num_nans in allowed_nans.items():
            assert (
                num_nans_real := tb[colname].isna().sum()
            ) == num_nans, f"Unexpected number ({num_nans_real}) of NaNs for column {colname}"
        assert (
            tb[[col for col in tb.columns if col not in allowed_nans.keys()]].notna().all(axis=None)
        ), "Some NaNs detected"
    else:
        assert tb.notna().all(axis=None), "Some NaNs detected"

    # Harmonize country names
    tb = harmonize_country_names(tb)

    # Harmonize dimensions
    tb = harmonize_dimension(
        tb,
        "variant",
        mapping={
            "Estimates": "estimates",
            "Medium": "medium",
            "Low": "low",
            "High": "high",
        },
        strict=False,
    )

    # Add missing dimensions
    if "sex" not in tb.columns:
        tb["sex"] = "all"
    if "age" not in tb.columns:
        tb["age"] = "all"

    tb["age"] = tb["age"].astype("string")

    return tb


def estimate_sex_ratio(tb: Table, age_groups: Optional[List[str]] = None):
    # Select relevant age groups
    if age_groups is None:
        age_groups = ["0", "5", "10", "15"] + [str(i) for i in range(20, 100, 10)] + ["100+"]

    tb_sex = tb.loc[(tb["age"].isin(age_groups)) & (tb["sex"] != "Total")].copy()
    # Pivot
    tb_sex = tb_sex.pivot(
        columns="sex", index=[col for col in COLUMNS_INDEX_MONTH if col != "sex"], values="population"
    )
    # Estimate ratio
    tb_sex["sex_ratio"] = tb_sex["male"] / tb_sex["female"]
    # Reset index
    tb_sex = tb_sex.reset_index()
    # Add missing sex column
    tb_sex["sex"] = "all"
    # Keep relevant columns
    tb_sex = tb_sex[COLUMNS_INDEX_MONTH + ["sex_ratio"]]

    return tb_sex


def set_variant_to_estimates(tb: Table) -> Table:
    """For data before YEAR_SPLIT, make sure to have variant = 'estimates'."""
    tb["variant"] = tb["variant"].astype("string")
    tb.loc[tb["year"] < YEAR_SPLIT, "variant"] = "estimates"
    return tb


# Population
def estimate_age_groups(tb: Table) -> Table:
    """Estimate population values for all age groups."""
    tb_ = tb.copy()

    # List with all agge groups
    tbs = []

    # 1/ Basic age groups
    age_map = {
        **{str(i): f"{i - i%5}-{i + 4 - i%5}" for i in range(0, 100)},
        **{"100+": "100+"},
    }
    tb_basic = tb_.assign(age=tb_.age.map(age_map))
    tb_basic = tb_basic.groupby(
        COLUMNS_INDEX_MONTH,
        as_index=False,
        observed=True,
    )["population"].sum()
    tbs.append(tb_basic)

    # 2/ Additional age groups (NOTE: they are not disjoint!)
    age_groups = [
        0,
        1,
        (1, 4),  # 1-4
        (0, 14),  # 0-14
        (0, 24),  # 0-24
        (5, 14),  # 5-14
        (15, 24),  # 15-24
        (15, 64),  # 15-64
        (20, 29),  # 20-29
        (25, 64),  # 25-64
        (30, 39),  # 30-39
        (40, 49),  # 40-49
        (50, 59),  # 50-59
        (60, 69),  # 60-69
        (70, 79),  # 70-79
        (80, 89),  # 80-89
        (90, 99),  # 90-99
        (15, 200, "15+"),  # 15+
        (18, 200, "18+"),  # 18+
        (65, 200, "65+"),  # 65+
    ]
    for age_group in age_groups:
        if isinstance(age_group, int):
            tb_age = tb_[tb_.age == str(age_group)].copy()
        else:
            tb_age = _add_age_group(tb_, *age_group)

        tbs.append(tb_age)

    # 3/ All-age group
    tb_all = (
        tb_.groupby(
            ["country", "year", "sex", "variant", "month"],
            as_index=False,
            observed=True,
        )["population"]
        .sum()
        .assign(age="all")
    )
    tbs.append(tb_all)

    # 4/ Merge all age groups
    tb_population = pr.concat(
        tbs,
        ignore_index=True,
    )

    return tb_population


def _add_age_group(tb: Table, age_min: int, age_max: int, age_group: Optional[str] = None) -> Table:
    """Estimate a new age group."""
    # Get subset of entries, apply groupby-sum if needed
    if age_min == age_max:
        tb_age = tb.loc[tb["age"] == str(age_min)].copy()
    else:
        ages_accepted = [str(i) for i in range(age_min, age_max + 1)]
        tb_age = tb.loc[tb["age"].isin(ages_accepted)].drop(columns="age").copy()

        tb_age = tb_age.groupby(
            ["country", "year", "sex", "variant", "month"],
            as_index=False,
            observed=True,
        )["population"].sum()

    # Assign age group name
    if age_group:
        tb_age["age"] = age_group
    else:
        tb_age["age"] = f"{age_min}-{age_max}"

    return tb_age


def add_population_change(tb: Table) -> Table:
    """Estimate anual population change."""
    column_pop_change = "population_change"

    # Sort by year
    tb = tb.sort_values("year")

    # Estimate population change # Fiona: I'm not sure this is done in the same way as WPP do for their population change variable. They have data for 1950 and it shows the difference between 1950 and 1951
    pop_change = tb.groupby(["country", "sex", "age", "variant", "month"])["population"].diff()
    tb[column_pop_change] = pop_change

    # Hotfix to estimate year 2024 - only Medium variant available
    tb_2023 = (
        tb.loc[tb["year"] == YEAR_SPLIT - 1].copy().assign(year=YEAR_SPLIT).drop(columns=["variant", column_pop_change])
    )
    # Something weird going on here
    tb_merge = tb.merge(
        tb_2023, on=[col for col in COLUMNS_INDEX_MONTH if col != "variant"], how="left", suffixes=("", "_2023")
    )
    mask = tb_merge["year"] == YEAR_SPLIT
    tb_merge.loc[mask, column_pop_change] = tb_merge.loc[mask, "population"] - tb_merge.loc[mask, "population_2023"]
    tb = tb_merge.drop(columns=["population_2023"])

    # Sanity check
    assert (years := set(tb.loc[tb[column_pop_change].isna()]["year"])) == {
        1950
    }, f"Other than year 1950 detected: {years}"

    return tb


def add_sex_ratio_all(tb_sex: Table, tb: Table) -> Table:
    """Estimate anual population change."""
    tb_sex_all = estimate_sex_ratio(tb, age_groups=["all"])
    tb_sex = pr.concat([tb_sex, tb_sex_all])
    del tb_sex_all

    return tb_sex


def harmonize_dimension(tb: Table, column_name: str, mapping: Dict[str, str], strict: bool = True) -> Table:
    """Harmonize a dimension in a table using a mapping.

    tb: Table to harmonize.
    column_name: Column name to harmonize.
    mapping: Mapping to harmonize the column.
    """
    if strict:
        # Assert column_name does not contain any other column but those in mapping
        assert set(tb[column_name].unique()) == set(mapping.keys())

    # Replace values in column_name
    tb[column_name] = tb[column_name].replace(mapping)

    return tb


def harmonize_country_names(tb: Table):
    tb = geo.harmonize_countries(
        tb,
        countries_file=paths.country_mapping_path,
        excluded_countries_file=paths.excluded_countries_path,
    )
    return tb
