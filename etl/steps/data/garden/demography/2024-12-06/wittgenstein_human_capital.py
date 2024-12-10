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
    tb = ds_meadow.read("main")

    #
    # Process data.
    #
    # Fix year
    COLS_SINGLE = ["growth", "imm", "emi", "cbr", "nirate", "cdr"]
    COLS_RANGE = ["tdr", "ggapmys25", "mage", "ydr", "ggapmys15", "odr"]

    # 1) Periods like "2020-2025" are mapped to 2025
    # 2) While doing 1, we should make sure that the tables are properly aligned
    flag = tb["year"].str.contains("-")

    # Check columns for single year data
    single_year_cols = set(tb.loc[flag].dropna(axis=1, how="all").columns) - set(COLUMNS_INDEX)
    assert single_year_cols == set(COLS_SINGLE), f"Unexpected columns in single year data: {single_year_cols}"

    # Check columns for range year data
    range_year_cols = set(tb.loc[~flag].dropna(axis=1, how="all").columns) - set(COLUMNS_INDEX)
    assert range_year_cols == set(COLS_RANGE), f"Unexpected columns in range year data: {range_year_cols}"

    # Fix year type
    tb["year"] = tb["year"].str.extract(r"(\d{4}\.?0?)$").astype("Float32").astype("Int32")

    # Create two tables: year range and single year
    cols_index = list(tb.columns.intersection(COLUMNS_INDEX))
    tb_single = tb[cols_index + COLS_SINGLE].dropna(subset=COLS_SINGLE, how="all")
    tb_range = tb[cols_index + COLS_RANGE].dropna(subset=COLS_RANGE, how="all")

    # Merge back
    tb = tb_single.merge(tb_range, on=cols_index, how="outer")

    # Rename scenario
    assert set(tb["scenario"].unique()) == set(range(1, 6))
    tb["scenario"] = "SSP" + tb["scenario"].astype("string")

    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
    )

    # Scalings
    SCALINGS = {
        100: ["tdr", "odr", "ydr"],
        1_000: ["emi", "imm"],
    }
    for scale, cols in SCALINGS.items():
        for col in cols:
            tb[col] *= scale

    # Format
    tables = [
        tb.format(["country", "year", "scenario"]),
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
