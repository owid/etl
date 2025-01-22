"""Load a meadow dataset and create a garden dataset."""

import pandas as pd
from owid.catalog import Origin, Table
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
    ds_meadow = paths.load_dataset("vaccine_stock_out")

    # Read table from meadow dataset.
    tb = ds_meadow.read("vaccine_stock_out")

    #
    # Process data.
    origins = tb["value"].origins
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    tb = clean_data(tb)

    # Calculate derived metrics
    tb_agg, tb_cause, tb_global, tb_global_cause = calculate_derived_metrics(tb, origins)
    # Format tables
    tb = tb.format(["country", "year", "description"])
    tb_agg = tb_agg.format(["country", "year"], short_name="derived_metrics")
    tb_cause = tb_cause.format(["country", "year", "reason_for_stockout"], short_name="reason_for_stockout")
    tb_global = tb_global.format(["country", "year", "description"], short_name="global_stockout")
    tb_global_cause = tb_global_cause.format(["year", "reason_for_stockout"], short_name="global_cause")
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir,
        tables=[tb, tb_agg, tb_cause, tb_global, tb_global_cause],
        check_variables_metadata=True,
        default_metadata=ds_meadow.metadata,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def clean_data(tb: Table) -> Table:
    """
    - Drop extraneous columns
    - Replace 'ND' and 'NR' with NA
    - There are two variables for the yellow fever vaccine, which do not overlap in time-coverage.
    I believe they are just two spellings of the same vaccine, so I will merge them.
    """
    tb = tb.drop(columns=["iso_3_code", "who_region", "indcode", "indcatcode", "indcat_description", "indsort"])
    tb = tb.replace({"value": {"ND": pd.NA, "NR": pd.NA}})
    tb["description"] = tb["description"].str.replace("YF (Yellow fever)", "Yellow fever")

    return tb


### Derived metrics


def calculate_derived_metrics(tb: Table, origin: Origin) -> list[Table]:
    tb_nat = national_stockout_for_any_vaccine(tb)
    tb_district = district_level_stockout_for_any_vaccine(tb)
    tb_cause, tb_global_cause, tb_cause_number = derive_stockout_variables(tb, origin)
    tb_global = how_many_countries_had_stockouts(tb, origin)

    # Aggregate the data with similar formats
    tb_agg = tb_district.merge(tb_nat, on=["country", "year"], how="inner")
    tb_agg = tb_agg.merge(tb_cause_number, on=["country", "year"], how="inner")

    return [tb_agg, tb_cause, tb_global, tb_global_cause]


def derive_stockout_variables(tb: Table, origin: Origin) -> list[Table]:
    tb_cause = reason_for_stockout(tb, origin)
    tb_global_cause = countries_with_stockouts_per_cause(tb_cause)
    tb_cause_number = number_of_reasons_for_stockout(tb_cause, origin)
    return [tb_cause, tb_global_cause, tb_cause_number]


def countries_with_stockouts_per_cause(tb_cause: Table) -> Table:
    """
    How many countries had stockouts for each reason in the given year?
    """
    tb_cause = tb_cause[tb_cause["stockout"] == "Yes"]
    tb_cause = tb_cause.groupby(["year", "reason_for_stockout"])["country"].nunique().reset_index(name="num_countries")
    tb_cause["country"] = "World"
    return tb_cause


def number_of_reasons_for_stockout(tb_cause: Table, origin: Origin) -> Table:
    tb_stockouts = tb_cause[tb_cause["stockout"] == "Yes"].reset_index()
    tb_stockouts = (
        tb_stockouts.groupby(["country", "year"])
        .agg(num_causes_of_stockout=("reason_for_stockout", "nunique"))
        .reset_index()
    )
    tb_stockouts["num_causes_of_stockout"].metadata.origins = origin
    return tb_stockouts


def reason_for_stockout(tb: Table, origin: Origin) -> Table:
    """
    Was there a stockout because of ___ for any vaccine?
    """
    tb_cause = tb[
        (tb["description"].str.contains("What was the cause of the national stock-out"))
        & (tb["description"].str.contains("vaccine"))
    ]
    # Dropping rows where the values are 'Yes' or 'No' as they are incorrect
    tb_cause = tb_cause[~tb_cause["value"].isin(["Yes", "No"])]
    # Fix typo in the data
    tb_cause["value"] = tb_cause["value"].replace("Inaccurtae forecasts", "Inaccurate forecasts")

    tb_agg = (
        tb_cause.assign(yes="Yes")
        .pivot_table(index=["country", "year"], columns="value", values="yes", aggfunc="first")
        .fillna("No")
        .reset_index()
    )
    tb_agg = tb_agg.melt(
        id_vars=["country", "year"],  # Columns to keep
        var_name="reason_for_stockout",  # Name for the column created from the pivoted columns
        value_name="stockout",  # Name for the values ("yes"/"no")
    )

    tb_agg["stockout"].metadata.origins = origin

    return tb_agg


def national_stockout_for_any_vaccine(tb: Table) -> Table:
    """
    Was there a vaccine stockout of any vaccine in the country in the given year?
    """
    # Get the rows relevant to national vaccine stockouts (i.e. not subnational, or stockouts of syringes)

    tb_agg = tb[
        tb["description"].str.contains("Was there a stock-out at the national level")
        & (tb["description"].str.contains("vaccine"))
    ]
    tb_agg = tb_agg.dropna(subset=["value"])
    tb_agg = (
        tb_agg.assign(is_yes=tb_agg["value"].eq("Yes"))
        .groupby(["country", "year"], as_index=False)
        .agg(
            any_yes=("is_yes", "any"),  # Whether there is any "Yes"
        )
        .assign(any_national_vaccine_stockout=lambda df: df["any_yes"].map({True: "Yes", False: "No"}))
        .drop(columns=["any_yes"])
    )

    return tb_agg


def how_many_countries_had_stockouts(tb: Table, origin: Origin) -> Table:
    """
    At a global level, how many countries had stockouts in the given year?
    """
    tb_agg = tb[
        tb["description"].str.contains("Was there a stock-out at the national level")
        & (tb["description"].str.contains("vaccine"))
    ]
    tb_agg = tb_agg[tb_agg["value"] == "Yes"]

    tb_each_vaccine = how_many_countries_had_stockouts_of_each_vaccine(tb_agg)
    tb_all = tb_agg.groupby("year")["country"].nunique().reset_index(name="num_countries_with_stockout")
    tb_all["description"] = "Any vaccine"
    tb_all["country"] = "World"
    tb_agg = pr.concat([tb_all, tb_each_vaccine], ignore_index=True)
    tb_agg["num_countries_with_stockout"].metadata.origins = origin
    return tb_agg


def how_many_countries_had_stockouts_of_each_vaccine(tb_agg: Table) -> Table:
    """
    At a global level, how many countries had stockouts of each vaccine in the given year?
    """
    tb_agg["description"] = tb_agg["description"].str.replace(
        "Was there a stock-out at the national level of ", "", regex=False
    )
    tb_agg["description"] = tb_agg["description"].str.replace("?", "", regex=False)
    tb_agg = (
        tb_agg.groupby(["year", "description"])["country"].nunique().reset_index(name="num_countries_with_stockout")
    )
    tb_agg["country"] = "World"
    return tb_agg


def district_level_stockout_for_any_vaccine(tb: Table) -> Table:
    """
    Was there a vaccine stockout at the district-level of any vaccine in the country in the given year?
    """
    # Get the rows relevant to district level vaccine stockouts (i.e. not national, or stockouts of syringes)

    tb_agg = tb[
        tb["description"].str.contains("Was there a vaccine stock-out at the district level")
        & (tb["description"].str.contains("vaccine"))
    ]
    tb_agg = tb_agg.dropna(subset=["value"])
    tb_agg = (
        tb_agg.assign(is_yes=tb_agg["value"].eq("Yes"))
        .groupby(["country", "year"], as_index=False)["is_yes"]
        .any()
        .assign(any_district_level_vaccine_stockout=lambda df: df["is_yes"].map({True: "Yes", False: "No"}))
        .drop(columns=["is_yes"])
    )

    # tb = tb.merge(tb_agg, on=["country", "year"], how="left")

    return tb_agg
