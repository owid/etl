from etl.data_helpers import geo

# Columns index
COLUMNS_INDEX = [
    "country",
    "year",
    "scenario",
    "sex",
    "age",
    "education",
]


def make_table(
    tb,
    country_mapping_path,
    dtypes=None,
    all_single=False,
    all_range=False,
    cols_single=None,
    cols_range=None,
    per_100=None,
    per_1000=None,
):
    dtypes = {**{"scenario": "UInt8", "country": "category"}, **(dtypes or {})}
    tb = tb.astype(dtypes)

    if all_single:
        tb["year"] = tb["year"].astype("Int32")
    elif all_range:
        assert tb["year"].str.contains("-").all(), "Some years are not ranges!"
        tb["year"] = tb["year"].str.extract(r"(\d{4}\.?0?)$").astype("Float32").astype("Int32")
    else:
        tb = consolidate_year_single_and_ranges(
            tb=tb,
            cols_single=cols_single,
            cols_range=cols_range,
        )

    # Ensure expected scenario IDs
    assert set(tb["scenario"].unique()) == set(range(1, 6))

    # Harmonize country names
    tb = geo.harmonize_countries(
        df=tb,
        countries_file=country_mapping_path,
        show_full_warning=False,
    )

    # Scale
    tb = scale_values(tb, per_100=per_100, per_1000=per_1000)
    return tb


def scale_values(tb, per_100=None, per_1000=None):
    if per_100 is not None:
        for col in per_100:
            tb[col] *= 100
    if per_1000 is not None:
        for col in per_1000:
            tb[col] *= 1000
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
