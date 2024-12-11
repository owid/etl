import owid.catalog.processing as pr

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
    per_10=None,
    per_100=None,
    per_1000=None,
    div_10=None,
    div_100=None,
    div_1000=None,
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
    tb = scale_values(
        tb, per_10=per_10, per_100=per_100, per_1000=per_1000, div_10=div_10, div_100=div_100, div_1000=div_1000
    )
    return tb


def scale_values(tb, per_10=None, per_100=None, per_1000=None, div_10=None, div_100=None, div_1000=None):
    if per_10 is not None:
        for col in per_10:
            tb[col] *= 10
    if per_100 is not None:
        for col in per_100:
            tb[col] *= 100
    if per_1000 is not None:
        for col in per_1000:
            tb[col] *= 1000
    if div_10 is not None:
        for col in div_10:
            tb[col] /= 10
    if div_100 is not None:
        for col in div_100:
            tb[col] /= 100
    if div_1000 is not None:
        for col in div_1000:
            tb[col] /= 1000
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


def add_dim_some_education(tb):
    """Add dimension "some education" to sex+age+education table.

    It only adds it for sex=total and age=total.
    """
    SOME_EDUCATION = "some_education"
    # Add education="some_education" (only for sex=total and age=total, and indicator 'pop')
    cols_index = ["country", "year", "age", "sex", "scenario"]
    tb_tmp = tb.loc[tb["education"].isin(["total", "no_education"]), cols_index + ["education", "pop"]]
    tb_tmp = tb_tmp.pivot(index=cols_index, columns="education", values="pop").reset_index().dropna()
    tb_tmp["some_education"] = tb_tmp["total"] - tb_tmp["no_education"]
    assert (tb_tmp["some_education"] >= 0).all()
    tb_tmp = tb_tmp.melt(id_vars=cols_index, value_vars=SOME_EDUCATION, var_name="education", value_name="pop")

    # Add new education
    tb["education"] = tb["education"].cat.add_categories([SOME_EDUCATION])

    dtypes = tb.dtypes
    tb = pr.concat([tb, tb_tmp], ignore_index=True)
    tb = tb.astype(dtypes)

    return tb


def add_dim_15plus(tb):
    # Pivot table to have two columns: "0-14" and "total"
    tb_adults = tb.loc[tb["age"].isin(["0-4", "5-9", "10-14", "total"]) & (tb["education"] != "total")]
    cols_index = ["country", "scenario", "sex", "education", "year"]
    tb_adults = tb_adults.pivot(index=cols_index, columns="age", values="pop").reset_index()
    # Only estimate values for adults when "total" is not NA
    tb_adults = tb_adults.dropna(subset=["total"])
    # Estimate adults as "0-14" - 15+
    # Fill with zero NAs of agr group "0-14". NAs mostly come from 'doesn't apply' (e.g. primary education for 0-14)
    tb_adults["15+"] = (
        tb_adults["total"] - tb_adults["0-4"].fillna(0) - tb_adults["5-9"].fillna(0) - tb_adults["10-14"].fillna(0)
    )
    # Drop columns
    tb_adults = tb_adults.drop(columns=["0-4", "5-9", "10-14", "total"])
    # Replace negative values for zero
    flag = tb_adults["15+"] < 0
    tb_adults.loc[flag, "15+"] = 0
    # Shape table
    tb_adults = tb_adults.melt(id_vars=cols_index, value_name="pop", var_name="age")
    # Concatenate with original table
    tb = pr.concat([tb, tb_adults], ignore_index=True)
    return tb


def get_index_columns(tb):
    cols_index = list(tb.columns.intersection(COLUMNS_INDEX))
    return cols_index


def add_prop(tb):
    # Add
    tbx = tb[tb["education"] == "total"].drop(columns=["education", "assr"])
    tb = tb.merge(tbx, on=["country", "year", "age", "sex", "scenario"], suffixes=["", "_total"])
    tb["prop"] = (100 * tb["pop"] / tb["pop_total"]).copy_metadata(tb["pop"])
    tb = tb.drop(columns=["pop_total"])
    return tb
