"""NOTE: This meadow step is relatively complete. Why? Because the snapshot steps are quite big, and we want to extract the esential data for next steps. Otherwise, we would be making Garden steps quite slow.

What do we do here?

- Read the XLSX files
- Keep relevant columns
- Format the tables to have them in long format
- Set indices and verify integrity
"""

import owid.catalog.processing as pr
from owid.catalog import Table
from owid.catalog.tables import concat

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__=__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    paths.log.info("reading snapshots...")

    # Population
    paths.log.info("reading population...")
    tb_population = paths.read_snap_table("un_wpp_population.csv")
    tb_population_density = read_estimates_and_projections_from_snap("un_wpp_population_density.xlsx")
    # Population doubling times
    tb_population_doubling = read_estimates_and_projections_from_snap("un_wpp_population_doubling.xlsx")
    # Growth rate
    tb_growth_rate = read_estimates_and_projections_from_snap("un_wpp_growth_rate.xlsx")
    # Natural change rate
    tb_nat_change = read_estimates_and_projections_from_snap("un_wpp_nat_change_rate.xlsx")
    # Fertility rate
    tb_fertility_tot = read_estimates_and_projections_from_snap("un_wpp_fert_rate_tot.xlsx")
    tb_fertility_age = read_estimates_and_projections_from_snap("un_wpp_fert_rate_age.xlsx")
    tb_fertility = combine_fertility_tables(tb_fertility_tot, tb_fertility_age)
    del tb_fertility_tot, tb_fertility_age
    # Childbearing age
    tb_childbearing_age = read_estimates_and_projections_from_snap("un_wpp_childbearing_age.xlsx")
    # Migration
    tb_migration = read_estimates_and_projections_from_snap("un_wpp_migration.xlsx")
    tb_migration = to_long_format_migration(tb_migration)
    tb_migration_rate = read_estimates_and_projections_from_snap("un_wpp_migration_rate.xlsx")
    # Deaths
    tb_deaths_tot = read_estimates_and_projections_from_snap("un_wpp_deaths.xlsx")
    tb_deaths_age = read_estimates_and_projections_from_snap("un_wpp_deaths_age.xlsx")
    tb_deaths_age_fem = read_estimates_and_projections_from_snap("un_wpp_deaths_age_fem.xlsx")
    tb_deaths_age_male = read_estimates_and_projections_from_snap("un_wpp_deaths_age_male.xlsx")
    tb_deaths = combine_deaths(tb_deaths_tot, tb_deaths_age, tb_deaths_age_fem, tb_deaths_age_male)
    del tb_deaths_tot, tb_deaths_age, tb_deaths_age_fem
    # Death rate
    tb_death_rate = read_estimates_and_projections_from_snap("un_wpp_death_rate.xlsx")
    # Mortality rates
    tb_child_mort = read_estimates_and_projections_from_snap("un_wpp_child_mortality.xlsx")
    tb_infant_mort = read_estimates_and_projections_from_snap("un_wpp_infant_mortality.xlsx")
    tb_mortality = combine_mortality(tb_child_mort, tb_infant_mort)
    del tb_child_mort, tb_infant_mort
    # Births
    tb_births_age = read_estimates_and_projections_from_snap("un_wpp_births_age.xlsx")
    tb_births_sex = read_estimates_and_projections_from_snap("un_wpp_births_sex.xlsx")
    tb_births = combine_births(tb_births_age, tb_births_sex)
    del tb_births_age, tb_births_sex
    # Birth rate
    tb_birth_rate = read_estimates_and_projections_from_snap("un_wpp_birth_rate.xlsx")
    # Median age
    tb_median_age = read_estimates_and_projections_from_snap("un_wpp_median_age.xlsx")
    # Life Expectancy
    tb_le = read_estimates_and_projections_from_snap("un_wpp_le.xlsx")
    tb_le_f = read_estimates_and_projections_from_snap("un_wpp_le_f.xlsx")
    tb_le_m = read_estimates_and_projections_from_snap("un_wpp_le_m.xlsx")
    tb_le = combine_life_expectancy(tb_le, tb_le_f, tb_le_m)
    del tb_le_f, tb_le_m

    #
    # Process data.
    #
    # Process tables
    tb_population = clean_table(tb_population, "population")
    tb_population_density = clean_table(tb_population_density, "population_density")
    tb_population_doubling = clean_table(tb_population_doubling, "population_doubling_time")
    tb_growth_rate = clean_table(tb_growth_rate, "growth_rate")
    tb_nat_change = clean_table(tb_nat_change, "natural_change_rate")
    tb_fertility = clean_table(tb_fertility, "fertility_rate")
    tb_childbearing_age = clean_table(tb_childbearing_age, "childbearing_age")
    tb_migration = clean_table(tb_migration, "net_migration")
    tb_migration_rate = clean_table(tb_migration_rate, "net_migration_rate")
    tb_deaths = clean_table(tb_deaths, "deaths")
    tb_death_rate = clean_table(tb_death_rate, "death_rate")
    tb_births = clean_table(tb_births, "births")
    tb_birth_rate = clean_table(tb_birth_rate, "birth_rate")
    tb_median_age = clean_table(tb_median_age, "median_age")
    tb_le = clean_table(tb_le, "life_expectancy")
    tb_mortality = clean_table(tb_mortality, "mortality_rate")

    #
    # Save outputs.
    #
    tables = [
        tb_population,
        tb_population_density,
        tb_population_doubling,
        tb_growth_rate,
        tb_nat_change,
        tb_fertility,
        tb_childbearing_age,
        tb_migration,
        tb_migration_rate,
        tb_deaths,
        tb_death_rate,
        tb_births,
        tb_birth_rate,
        tb_median_age,
        tb_le,
        tb_mortality,
    ]
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=tables, check_variables_metadata=True)

    # Save changes in the new meadow dataset.
    ds_meadow.save()


def read_estimates_and_projections_from_snap(short_name: str) -> Table:
    paths.log.info(f"reading {short_name}...")
    # Read snap
    snap = paths.load_snapshot(short_name)
    # Read tables
    # TODO: Add support for Low, and High variants
    tb_estimates = snap.read(safe_types=False, sheet_name="Estimates")
    tb_projections_medium = snap.read(safe_types=False, sheet_name="Medium")
    # tb_projections_low = snap.read(safe_types=False, sheet_name="Low")
    # tb_projections_high = snap.read(safe_types=False, sheet_name="High")
    # Merge tables
    tb = concat(
        [
            tb_estimates,
            tb_projections_medium,
            # tb_projections_low,
            # tb_projections_high,
        ],
        ignore_index=True,
    )
    return tb


def combine_fertility_tables(tb_tot: Table, tb_age: Table) -> Table:
    columns = set(tb_tot.columns).intersection(set(tb_age.columns))
    tb_age = tb_age.melt(list(columns), var_name="Age", value_name="Value")
    tb_tot["Age"] = "Total"
    tb_fertility = concat([tb_age, tb_tot], ignore_index=True)

    return tb_fertility


def combine_deaths(tb_tot: Table, tb_age: Table, tb_age_fem: Table, tb_age_male: Table) -> Table:
    # Drop column 'Sex'
    tb_age = tb_age.drop(columns=["Sex"])
    tb_age_fem = tb_age_fem.drop(columns=["Sex"])
    tb_age_male = tb_age_male.drop(columns=["Sex"])

    # Get common columns
    columns = set(tb_tot.columns).intersection(set(tb_age.columns))

    # Add missing dimension to general population
    tb_tot = tb_tot.melt(list(columns), var_name="Sex", value_name="Value")
    tb_tot["Age"] = "Total"

    # Unpivot age table (all sex)
    tb_age = tb_age.melt(list(columns), var_name="Age", value_name="Value")
    tb_age["Sex"] = "Total"

    # Unpivot age table (female)
    tb_age_fem = tb_age_fem.melt(list(columns), var_name="Age", value_name="Value")
    tb_age_fem["Sex"] = "Female"

    # Unpivot age table (male)
    tb_age_male = tb_age_male.melt(list(columns), var_name="Age", value_name="Value")
    tb_age_male["Sex"] = "Male"

    # Combine
    tb_deaths = concat([tb_tot, tb_age, tb_age_fem, tb_age_male], ignore_index=True)

    return tb_deaths


def combine_births(tb_age, tb_sex) -> Table:
    # Get common columns
    columns = set(tb_age.columns).intersection(set(tb_sex.columns))

    # sanity check
    assert {col for col in tb_sex.columns if col not in columns} == {
        "Total",
        "Female",
        "Male",
    }, "Unknown columns in sex table!"
    assert {col for col in tb_age.columns if col not in columns} == {
        f"{i}-{i+4}" for i in range(10, 60, 5)
    }, "Unknown columns in age table!"

    # total births by age of mother
    tb_age = tb_age.melt(list(columns), var_name="Age", value_name="Value")
    tb_age["Sex"] = "Total"

    # fem/male total births
    tb_sex = tb_sex.melt(list(columns), var_name="Sex", value_name="Value")
    tb_sex["Age"] = "Total"

    # Combine
    tb_deaths = concat([tb_age, tb_sex], ignore_index=True)

    return tb_deaths


def combine_life_expectancy(tb_le: Table, tb_le_f: Table, tb_le_m: Table) -> Table:
    # Drop column 'Sex'
    tb_le = tb_le.drop(columns=["Sex"])
    tb_le_f = tb_le_f.drop(columns=["Sex"])
    tb_le_m = tb_le_m.drop(columns=["Sex"])

    age_groups = {str(i) for i in range(0, 100, 5)} | {"1", "100+"}
    columns = {
        "ISO2_code",
        "ISO3_code",
        "LocTypeID",
        "LocTypeName",
        "LocationID",
        "LocationName",
        "Notes",
        "ParentID",
        "SDMX_code",
        "SortOrder",
        "VariantID",
        "VariantName",
        "Year",
    }
    # Sanity check
    assert set(tb_le.columns) == age_groups | columns, "LE contains unknown columns"
    assert set(tb_le_f.columns) == age_groups | columns, "LE_F contains unknown columns"
    assert set(tb_le_m.columns) == age_groups | columns, "LE_M contains unknown columns"

    # Unpivot total table (all sex)
    tb_le = tb_le.melt(list(columns), var_name="Age", value_name="Value")
    tb_le["Sex"] = "Total"

    # Unpivot total table (all sex)
    tb_le_f = tb_le_f.melt(list(columns), var_name="Age", value_name="Value")
    tb_le_f["Sex"] = "Female"

    # Unpivot total table (all sex)
    tb_le_m = tb_le_m.melt(list(columns), var_name="Age", value_name="Value")
    tb_le_m["Sex"] = "Male"

    # Combine
    tb_le = concat([tb_le, tb_le_f, tb_le_m], ignore_index=True)

    return tb_le


def to_long_format_migration(tb: Table) -> Table:
    """Convert migration table to long format."""
    # Melt
    tb = tb.melt(
        id_vars=[col for col in tb.columns if col not in {"Male", "Female", "Total"}],
        var_name="Sex",
        value_name="Value",
    )
    return tb


def combine_mortality(tb_infant: Table, tb_child: Table) -> Table:
    """Convert migration table to long format."""
    # Melt
    tb_infant = tb_infant.melt(
        id_vars=[col for col in tb_infant.columns if col not in {"Male", "Female", "Total"}],
        var_name="Sex",
        value_name="Value",
    )
    tb_infant["Age"] = "0"
    tb_child = tb_child.melt(
        id_vars=[col for col in tb_child.columns if col not in {"Male", "Female", "Total"}],
        var_name="Sex",
        value_name="Value",
    )
    tb_child["Age"] = "0-4"

    tb = pr.concat([tb_infant, tb_child], ignore_index=True)
    return tb


def clean_table(tb: Table, indicator_name: str) -> Table:
    """Process growth rate data.

    From snapshot table to ETL-ready-cleaned table.
    """
    paths.log.info(f"processing {indicator_name} data...")

    COLUMNS = {
        "LocationName": "country",
        "Year": "year",
        "LocTypeName": "location_type",
        "VariantName": "variant",
        "Sex": "sex",
        "Age": "age",
        "Value": indicator_name,
    }
    COLUMNS = {k: v for k, v in COLUMNS.items() if k in tb.columns}
    COLUMNS_INDEX = [v for k, v in COLUMNS.items() if v not in {indicator_name}]

    # Column rename
    tb = tb.rename(columns=COLUMNS)
    # Keep relevant columns
    tb = tb.loc[:, COLUMNS.values()]

    # Keep relevant location types
    location_types = ["Country/Area", "Region", "Income Group", "Development Group", "World"]
    tb = tb.loc[tb["location_type"].isin(location_types)]

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(COLUMNS_INDEX, short_name=indicator_name)

    return tb
