"""Load a meadow dataset and create a garden dataset."""

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
# Columns index
COLUMNS_INDEX = [
    "country",
    "year",
    "scenario",
    "sex",
    "age",
    "education",
]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("wittgenstein_human_capital")

    # Read table from meadow dataset.
    tb = ds_meadow.read("main").reset_index(drop=True)
    tb_age = ds_meadow.read("by_age").reset_index(drop=True)
    tb_sex = ds_meadow.read("by_sex").reset_index(drop=True)
    tb_edu = ds_meadow.read("by_edu").reset_index(
        drop=True
    )  # TODO: add metadata field for table explaining the different education levels
    tb_sex_age = ds_meadow.read("by_sex_age").reset_index(drop=True)
    # tb_age_edu = ds_meadow.read("by_age_edu")
    # tb_age_sex_edu = ds_meadow.read("by_age_sex_edu")

    #
    # Process data.
    #

    # 1/ MAKE MAIN TABLE
    # Fix year
    tb = consolidate_year_single_and_ranges(
        tb=tb,
        cols_single=["tdr", "ggapmys25", "mage", "ydr", "ggapmys15", "odr"],
        cols_range=["growth", "imm", "emi", "cbr", "nirate", "cdr"],
    )

    # Harmonize scenarios, country names
    tb = harmonize_columns(tb=tb)

    # Scale factor
    tb = scale_values(tb, per_100=["tdr", "odr", "ydr"], per_1000=["emi", "imm"])

    # 2.1/ MAKE BY AGE TABLE (sexratio)
    # Fix year type
    tb_age["year"] = tb_age["year"].astype("Int32")

    # Harmonize scenarios, country names
    tb_age = harmonize_columns(tb=tb_age)

    # Scale factor
    tb_age = scale_values(tb_age, per_100=["sexratio"])

    # 2.2/ BY SEX
    # Fix year type
    assert tb_sex["year"].str.contains("-").all(), "Some years are not ranges!"
    tb_sex["year"] = tb_sex["year"].str.extract(r"(\d{4}\.?0?)$").astype("Float32").astype("Int32")

    # Harmonize scenarios, country names
    tb_sex = harmonize_columns(tb=tb_sex)

    # 2.3/ BY EDU
    # Fix year
    tb_edu = consolidate_year_single_and_ranges(
        tb_edu,
        cols_single=["ggapedu15", "ggapedu25"],
        cols_range=["macb", "tfr", "net"],
    )

    # Harmonize scenarios, country names
    tb_edu = harmonize_columns(tb=tb_edu)

    # Scale factor
    tb_edu = scale_values(tb_edu, per_1000=["net"])

    # 3.1/ BY SEX+AGE
    # Fix year
    tb_sex_age["year"] = tb_sex_age["year"].astype("Int32")

    # Harmonize scenarios, country names
    tb_sex_age = harmonize_columns(tb=tb_sex_age)

    #
    # Save outputs.
    #
    # Format
    tables = [
        tb.format(["country", "year", "scenario"], short_name="main"),
        tb_age.format(["country", "scenario", "age", "year"], short_name="by_age"),
        tb_sex.format(["country", "scenario", "sex", "year"], short_name="by_sex"),
        tb_edu.format(["country", "scenario", "education", "year"], short_name="by_edu"),
        tb_sex_age.format(["country", "scenario", "sex", "age", "year"], short_name="by_sex_age"),
    ]
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir,
        tables=tables,
        check_variables_metadata=True,
        default_metadata=ds_meadow.metadata,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def scale_values(tb, per_100=None, per_1000=None):
    if per_100 is not None:
        for col in per_100:
            tb[col] *= 100
    if per_1000 is not None:
        for col in per_1000:
            tb[col] *= 1000
    return tb


def harmonize_columns(tb):
    # Ensure expected scenario IDs
    assert set(tb["scenario"].unique()) == set(range(1, 6))
    # tb["scenario"] = "SSP" + tb["scenario"].astype("string[pyarrow]")

    # Harmonize country names
    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
    )
    return tb


def consolidate_year_single_and_ranges(tb, cols_single, cols_range):
    # 1) Periods like "2020-2025" are mapped to 2025
    # 2) While doing 1, we should make sure that the tables are properly aligned
    flag = tb["year"].str.contains("-")

    # Check columns for single year data
    single_year_cols = set(tb.loc[~flag].dropna(axis=1, how="all").columns) - set(COLUMNS_INDEX)
    assert single_year_cols == set(cols_single), f"Unexpected columns in single year data: {single_year_cols}"

    # Check columns for range year data
    range_year_cols = set(tb.loc[flag].dropna(axis=1, how="all").columns) - set(COLUMNS_INDEX)
    assert range_year_cols == set(cols_range), f"Unexpected columns in range year data: {range_year_cols}"

    # Fix year type
    tb["year"] = tb["year"].str.extract(r"(\d{4}\.?0?)$").astype("Float32").astype("Int32")

    # Create two tables: year range and single year
    cols_index = list(tb.columns.intersection(COLUMNS_INDEX))
    tb_single = tb[cols_index + cols_single].dropna(subset=cols_single, how="all")
    tb_range = tb[cols_index + cols_range].dropna(subset=cols_range, how="all")

    # Merge back
    tb = tb_single.merge(tb_range, on=cols_index, how="outer")

    return tb
