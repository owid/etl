"""Load a meadow dataset and create a garden dataset."""

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("undp_hdr")
    ds_regions = paths.load_dataset("regions")
    ds_income_groups = paths.load_dataset("income_groups")

    # Read table from meadow dataset.
    tb = ds_meadow["undp_hdr"].reset_index()

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

    # Make Atkinson indices not percentages
    atkinson_cols = ["ineq_edu", "ineq_inc", "ineq_le", "coef_ineq"]
    for col in atkinson_cols:
        tb[col] /= 100

    # Set dtypes
    tb = tb.astype(
        {
            "country": "category",
            "year": int,
            **{col: "Float64" for col in tb.columns if col not in ["country", "year"]},
        }
    )

    # Convert population data from millions
    tb["pop_total"] *= 1e6

    tb = region_avg(tb, ds_regions, ds_income_groups)

    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def region_avg(tb, ds_regions, ds_income_groups):
    """Calculate regional averages for the table, this includes continents and WB income groups"""
    # remove columns where regional average does not make sense
    tb_cols = tb.columns
    ind_wo_avg = ["country", "year", "gii_rank", "hdi_rank", "loss", "rankdiff_hdi_phdi", "gdi_group"]
    rel_cols = [col for col in tb.columns if col not in ind_wo_avg]

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
