"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Dataset, Table

from etl.data_helpers import geo
from etl.data_helpers.misc import expand_time_column
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("oxcgrt_policy")
    ds_regions = paths.load_dataset("regions")
    ds_income = paths.load_dataset("income_groups")

    # Read table from meadow dataset.
    tb = ds_meadow["oxcgrt_policy_compact"].reset_index()
    tb_vax = ds_meadow["oxcgrt_policy_vaccines"].reset_index()
    tb_stringency = ds_meadow["oxcgrt_policy_stringency"].reset_index()

    #
    # Process data.
    #
    # Keep only national data
    tb = tb.loc[tb["regioncode"].isnull()].drop(columns="regioncode")
    # Merge tables
    tb = tb.merge(tb_vax, how="outer", on=["countryname", "date"], validate="one_to_one")
    tb = tb.merge(tb_stringency, how="outer")
    # Column renaming
    tb = tb.rename(
        columns={
            "countryname": "country",
            "stringencyindex_average": "stringency_index",
            "stringencyindex_vaccinated": "stringency_index_vax",
            "stringencyindex_nonvaccinated": "stringency_index_nonvax",
            "stringencyindex_weightedaverage": "stringency_index_weighted_average",
            "containmenthealthindex_average": "containment_health_index",
        }
    )
    tb = tb.drop(columns=["stringencyindex_simpleaverage"])

    # Harmonize
    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
    )
    # Parse date
    tb["date"] = pd.to_datetime(tb["date"], format="%Y%m%d")

    # Add continents and income group names as two new columns
    tb_counts = add_region_names(tb, ds_regions, ds_income)

    # Count countries per region
    tb_counts = get_num_countries_per_region(tb_counts)

    # Copy metadata
    tb_counts["num_countries"] = tb_counts["num_countries"].copy_metadata(tb["c8ev_international_travel_controls"])

    #
    # Save outputs.
    #
    tables = [
        tb.format(["country", "date"], short_name="oxcgrt_policy"),
        tb_counts.format(["country", "date", "restriction_name", "restriction_level"], short_name="country_counts"),
    ]
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir,
        tables=tables,
        check_variables_metadata=True,
        default_metadata=ds_meadow.metadata,
        formats=["csv", "feather"],
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def add_region_names(tb: Table, ds_regions: Dataset, ds_income: Dataset) -> Table:
    """Add two new columns:

    - continent: Name of the continent of `country`.
    - income_group: Name of the income group of `country`.
    """
    # Check we have all dates covered
    ## 1/ No day jump? I.e. country time series contain day-by-day values
    assert (tb.groupby("country").date.diff().dt.days.dropna() == 1).all(), "There are some day jumps!"
    ## 2/ All dates? I.e. starting and ending date are the same for all countries
    assert tb.groupby("country").date.min().nunique() == 1, "More than one starting date!"
    assert tb.groupby("country").date.max().nunique() == 1, "More than one ending date!"

    # Build dictionary mapping countries to regions (and income groups)
    countries_to_continent = geo.countries_to_continent_mapping(ds_regions)
    countries_to_income = geo.countries_to_income_mapping(ds_regions, ds_income)

    # Complete table with entries of countries missing
    countries_all = set(countries_to_continent)

    tb = expand_time_column(
        df=tb,
        entity_col="country",
        time_col="date",
        entities_complete=countries_all,
        mode="full_range",
    )

    # Add continent
    tb["continent"] = tb["country"].map(countries_to_continent)
    tb["income_group"] = tb["country"].map(countries_to_income)

    return tb


def get_num_countries_per_region(tb: Table) -> Table:
    ## input: country, date, continent, income_group, restriction_A, restriction_B
    ## output: region, date, restriction_name, restriction_level (region is continent and/or income_group)
    cols = [
        "c4m_restrictions_on_gatherings",  # 0, 1, 2, 3, 4
        "h6m_facial_coverings",  # 0, 1, 2, 3, 4
        "c8ev_international_travel_controls",  # 0, 1, 2, 3, 4
    ]

    # Fill NaNs with -1
    tb[cols] = tb[cols].fillna(-1)

    # Unpivot
    tb = tb.melt(
        id_vars=["date", "continent", "income_group"],
        value_vars=cols,
        var_name="restriction_name",
        value_name="restriction_level",
    )
    tb = pr.concat(
        [
            tb.drop(columns="continent").rename(columns={"income_group": "country"}),
            tb.drop(columns="income_group").rename(columns={"continent": "country"}),
        ]
    )

    # Actual counting
    tb = tb.groupby(["country", "date", "restriction_name", "restriction_level"], observed=True, as_index=False).size()

    # Rename column
    tb = tb.rename(
        columns={
            "size": "num_countries",
        }
    )
    return tb
