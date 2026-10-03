"""Load a meadow dataset and create a garden dataset."""

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

COLUMNS_EXPECTED = {
    "abr",
    "co2_prod",
    "coef_ineq",
    "country",
    "diff_hdi_phdi",
    "eys",
    "eys_f",
    "eys_m",
    "gdi",
    "gdi_group",
    "gii",
    "gii_rank",
    "gni_pc_f",
    "gni_pc_m",
    "gnipc",
    "hdi",
    "hdi_f",
    "hdi_m",
    "hdi_rank",
    "ihdi",
    "ineq_edu",
    "ineq_inc",
    "ineq_le",
    "le",
    "le_f",
    "le_m",
    "lfpr_f",
    "lfpr_m",
    "loss",
    "mf",
    "mmr",
    "mys",
    "mys_f",
    "mys_m",
    "phdi",
    "pop_total",
    "pr_f",
    "pr_m",
    "rankdiff_hdi_phdi",
    "se_f",
    "se_m",
    "year",
}

# Columns with dimension sex
COLUMNS_SEX = [
    "eys",
    "eys_f",
    "eys_m",
    "gni_pc",
    "gni_pc_f",
    "gni_pc_m",
    "hdi",
    "hdi_f",
    "hdi_m",
    "le",
    "le_f",
    "le_m",
    "lfpr_f",
    "lfpr_m",
    "mys",
    "mys_f",
    "mys_m",
    "pr_f",
    "pr_m",
    "se_f",
    "se_m",
]


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("undp_hdr")
    ds_regions = paths.load_dataset("regions")
    ds_income_groups = paths.load_dataset("income_groups")

    # Read table from meadow dataset.
    tb = ds_meadow.read("undp_hdr")

    #
    # Process data.
    #
    # Harmonize country names.
    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
    )

    # Drop irrelevant columns
    tb = tb.drop(columns=["iso3", "hdicode", "region"])

    # Re-shape table to get (country, year) as index and variables as columns.
    tb = tb.melt(id_vars=["country"])
    tb[["variable", "year"]] = tb["variable"].str.extract(r"(.*)_(\d{4})")
    tb = tb.pivot(index=["country", "year"], columns="variable", values="value").reset_index()

    # Check columns are as expected
    assert set(tb.columns) == COLUMNS_EXPECTED, f"Columns in table are not as expected: {tb.columns}"

    # Make Atkinson indices not percentages
    atkinson_cols = ["ineq_edu", "ineq_inc", "ineq_le", "coef_ineq"]
    for col in atkinson_cols:
        tb[col] /= 100

    # Set dtypes
    tb = tb.astype({"year": int})

    # Convert population data from millions
    tb["pop_total"] *= 1e6

    # Add regional aggregates (continents and income groups)
    cols_avg = ["country", "year", "gii_rank", "hdi_rank", "loss", "rankdiff_hdi_phdi", "gdi_group"]
    tb = region_avg(
        tb=tb,
        ds_regions=ds_regions,
        ds_income_groups=ds_income_groups,
        columns=cols_avg,
    )

    # Minor columns rename
    tb = tb.rename(
        columns={
            "gnipc": "gni_pc",
        }
    )

    # Table with dimension sex
    tb_sex = make_table_with_dimension_sex(tb, COLUMNS_SEX)
    tb_sex = tb_sex.format(["country", "year", "sex"], short_name="undp_hdr_sex")

    # Main table
    tb = tb.drop(columns=COLUMNS_SEX)
    tb = tb.format(["country", "year"])

    # Build tables list
    tables = [
        tb,
        tb_sex,
    ]

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(
        tables=tables,
        check_variables_metadata=True,
        default_metadata=ds_meadow.metadata,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def region_avg(tb, ds_regions, ds_income_groups, columns):
    """Calculate regional averages for the table, this includes continents and WB income groups"""
    # remove columns where regional average does not make sense
    tb_cols = tb.columns
    rel_cols = [col for col in tb.columns if col not in columns]

    # calculate population weighted columns (helper columns)
    rel_cols_pop = []
    for col in rel_cols:
        tb[col + "_pop"] = tb[col] * tb["pop_total"]
        rel_cols_pop.append(col + "_pop")

    # Define aggregations only for the columns I need
    aggregations = dict.fromkeys(
        rel_cols + rel_cols_pop,
        "sum",
    )

    tb = geo.add_regions_to_table(
        tb,
        ds_regions=ds_regions,
        ds_income_groups=ds_income_groups,
        aggregations=aggregations,
        frac_allowed_nans_per_year=0.2,
    )

    # calculate regional averages
    for col in rel_cols:
        tb[col] = tb[col + "_pop"] / tb["pop_total"]

    # Add description_processing only to rel_cols
    for col in rel_cols:
        tb[
            col
        ].m.description_processing = "We calculated averages over continents and income groups by taking the population-weighted average of the countries in each group. If less than 80% of countries in an area report data for a given year, we do not calculate the average for that area."

    return tb[tb_cols]


def make_table_with_dimension_sex(tb, columns):
    # Copy columns
    tb_sex = tb[["country", "year"] + columns].copy()

    # Reshape
    tb_sex = tb_sex.melt(id_vars=["country", "year"])
    tb_sex["sex"] = tb_sex["variable"].apply(lambda x: "female" if "_f" in x else "male" if "_m" in x else "total")
    tb_sex["variable"] = tb_sex["variable"].str.replace(r"_f|_m", "", regex=True)
    tb_sex = tb_sex.pivot(index=["country", "year", "sex"], columns="variable", values="value").reset_index()

    return tb_sex
