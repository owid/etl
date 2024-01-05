"""Create a garden dataset from the meadow dataset."""
from typing import Dict, List, cast

from owid.catalog import Table, Variable
from owid.datautils import dataframes, io
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# It may happen that the data for the most recent year is incomplete.
# If so, define the following to be last year fully informed.
# LAST_INFORMED_YEAR = 2021
LAST_INFORMED_YEAR = None

# Labels for the variables showing whether any sector is covered by an ETS or a carbon tax at the national or only
# sub-national level.
LABEL_ETS_NOT_COVERED = "No ETS"
LABEL_ETS_COVERED = "Has an ETS"
LABEL_ETS_COVERED_ONLY_SUBNATIONAL = "Has an ETS only at a sub-national level"
LABEL_TAX_NOT_COVERED = "No carbon tax"
LABEL_TAX_COVERED = "Has a carbon tax"
LABEL_TAX_COVERED_ONLY_SUBNATIONAL = "Has a carbon tax only at a sub-national level"
# If a country-years has both national and sub-national coverage, mention only the national and ignore subnational.
LABEL_ETS_COVERED_NATIONAL_AND_SUBNATIONAL = "Has an ETS"
LABEL_TAX_COVERED_NATIONAL_AND_SUBNATIONAL = "Has a carbon tax"

# Columns to keep from raw dataset and how to rename them.
COLUMNS = {
    "jurisdiction": "country",
    "year": "year",
    "ipcc_code": "ipcc_code",
    "product": "product",
    "sector_name": "sector_name",
    "tax": "tax",
    "ets": "ets",
    "tax_rate_excl_ex_clcu": "tax_rate_gross",
    "tax_rate_incl_ex_clcu": "tax_rate_net",
    "ets_price": "ets_price",
}

# Mapping of countries and the regions of the country included in the sub-national dataset.
# In the future, it would be good to load this mapping as additional data (however, the mapping is hardcoded in the
# original repository, so it's not trivial to get this mapping automatically).
COUNTRY_MEMBERS_FILE = paths.directory / "world_carbon_pricing.country_members.json"


def sanity_checks(tb: Table) -> None:
    """Sanity checks on the raw data.

    Parameters
    ----------
    tb : Table
        Raw data from meadow.

    """
    column_checks = (
        tb.groupby("jurisdiction")
        .agg(
            {
                # Columns 'tax' and 'ets' must contain only 0 and/or 1.
                "tax": lambda x: set(x) <= {0, 1},
                "ets": lambda x: set(x) <= {0, 1},
            }
        )
        .all()
    )
    # Column tax_id either is nan or has one value, which is the iso code of the country followed by "tax"
    # (e.g. 'aus_tax'). However there is at least one exception, Norway has 'nor_tax_I', so maybe the data is
    # expected to have more than one 'tax_id'.

    # Similarly, 'ets_id' is either nan, or usually just one value, e.g. "eu_ets" for EU countries, or "nzl_ets",
    # "mex_ets", etc. However for the UK there are two, namely {'gbr_ets', 'eu_ets'}.

    error = f"Unexpected content in columns {column_checks[~column_checks].index.tolist()}."
    assert column_checks.all(), error

    # If the last year in the data is the current year, or if the data for the last year is missing, raise a warning.
    if (
        tb["year"].max() == int(paths.version.split("-")[0])
        or tb[["year", "ets_price"]].groupby(["year"], observed=True).sum(min_count=1)["ets_price"].isnull().iloc[-1]
    ):
        log.warning("The last year in the data may be incomplete. Define LAST_INFORMED_YEAR.")


def prepare_data(tb_national: Table) -> Table:
    """Prepare data.

    Parameters
    ----------
    tb_national : Table
        Raw data.

    Returns
    -------
    tb_national : Table
        Clean data.

    """
    tb_national = tb_national.copy()

    # Select and rename columns.
    tb_national = tb_national[list(COLUMNS)].rename(columns=COLUMNS, errors="raise")

    # Column 'product' has many nans. Convert them into empty strings.
    tb_national["product"] = Variable(tb_national["product"].cat.add_categories("").fillna("")).copy_metadata(
        tb_national["product"]
    )

    if LAST_INFORMED_YEAR is not None:
        # Keep only data points prior to (or at) a certain year.
        tb_national = tb_national[tb_national["year"] <= LAST_INFORMED_YEAR].reset_index(drop=True)

    return tb_national


def get_coverage_for_any_sector(tb: Table) -> Table:
    """Create a table showing whether a country has any sector covered by an ets/carbon tax.

    Parameters
    ----------
    tb : Table
        Original national or sub-national data, disaggregated by sector.

    Returns
    -------
    tb_any_sector : tb
        Coverage for any sector.

    """
    # Create a simplified table that gives, for each country and year, whether the country has any sector(-fuel)
    # that is covered by at least one tax instrument. And idem for ets.
    tb_any_sector = (
        tb.reset_index()
        .groupby(["country", "year"], observed=True)
        .agg({"ets": lambda x: min(x.sum(), 1), "tax": lambda x: min(x.sum(), 1)})
        .astype(int)
        .reset_index()
    )

    return tb_any_sector


def prepare_subnational_data(tb_subnational: Table, country_members: Dict[str, List[str]]) -> Table:
    """Create a table showing whether a country has any sub-national jurisdiction for which any sector is covered by
    an ets/carbon tax.

    The 'country' column of this table does not need to be harmonized, since we are mapping the original
    sub-national jurisdiction names to the harmonized name of the country.

    Parameters
    ----------
    tb_subnational : Table
        Sub-national data, disaggregated by sector.

    Returns
    -------
    tb_subnational : Table
        Processed sub-national data.

    """
    # Prepare subnational data.
    tb_subnational = prepare_data(tb_subnational)
    # Map subnational regions to their corresponding country.
    subregions_to_country = {
        subregion: country for country in list(country_members) for subregion in country_members[country]
    }
    tb_subnational["country"] = dataframes.map_series(
        series=tb_subnational["country"],
        mapping=subregions_to_country,
        warn_on_missing_mappings=True,
        warn_on_unused_mappings=True,
    )
    # Get coverage of "any sector", where we only care about having at least one sector covered by carbon tax/ets.
    tb_subnational = get_coverage_for_any_sector(tb=tb_subnational)

    return tb_subnational


def combine_national_and_subnational_data(tb_any_sector_national: Table, tb_any_sector_subnational: Table) -> Table:
    """Combine national and sub-national data on whether countries have any sector covered by a tax instrument.

    The returned table will have the following labels:
    * Whether a country-year has no sector covered.
    * Whether a country-year has at least one sector covered at the national level.
    * Whether a country-year has at least one sector in one sub-national jurisdiction covered, but no sector covered at
      the national level.
    * Whether a country-year has at least one sector in both a sub-national and the national jurisdiction covered.
      However, for now we disregard this option, by using the same label as for only national coverage.

    Parameters
    ----------
    tb_any_sector_national : Table
        National data on whether countries have any sector covered by a tax instrument.
    tb_any_sector_subnational : Table
        Sub-national data on whether countries have any sector covered by a tax instrument.

    Returns
    -------
    tb_any_sector : Table
        Combined table showing whether a country has at least one sector covered by a tax instrument at a national
        level, or only at the sub-national level, or not at all.

    """
    # Combine national and subnational data.
    tb_any_sector = tb_any_sector_national.merge(
        tb_any_sector_subnational, on=["country", "year"], how="left", suffixes=("_national", "_subnational")
    ).fillna(0)

    # Create two new columns ets and tax, that are:
    # * 0 if no ets/tax exists.
    # * 1 if there is a national ets/tax and not a subnational ets/tax.
    # * 2 if there is a subnational ets/tax and not a national ets/tax.
    # * 3 if there are both a national and a subnational ets/tax.
    tb_any_sector = tb_any_sector.assign(
        **{
            "ets": tb_any_sector["ets_national"] + 2 * tb_any_sector["ets_subnational"],
            "tax": tb_any_sector["tax_national"] + 2 * tb_any_sector["tax_subnational"],
        }
    )[["country", "year", "ets", "tax"]]

    # Now replace 0, 1, 2, and 3 by their corresponding labels.
    ets_mapping = {
        0: LABEL_ETS_NOT_COVERED,
        1: LABEL_ETS_COVERED,
        2: LABEL_ETS_COVERED_ONLY_SUBNATIONAL,
        3: LABEL_ETS_COVERED_NATIONAL_AND_SUBNATIONAL,
    }
    tax_mapping = {
        0: LABEL_TAX_NOT_COVERED,
        1: LABEL_TAX_COVERED,
        2: LABEL_TAX_COVERED_ONLY_SUBNATIONAL,
        3: LABEL_TAX_COVERED_NATIONAL_AND_SUBNATIONAL,
    }
    tb_any_sector["ets"] = dataframes.map_series(series=tb_any_sector["ets"], mapping=ets_mapping)
    tb_any_sector["tax"] = dataframes.map_series(series=tb_any_sector["tax"], mapping=tax_mapping)

    return cast(Table, tb_any_sector)


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load main dataset from meadow, and read its main tables.
    ds_meadow = paths.load_dataset("world_carbon_pricing")
    tb_national = ds_meadow["world_carbon_pricing_national_level"].reset_index()
    tb_subnational = ds_meadow["world_carbon_pricing_subnational_level"].reset_index()

    # Load dictionary mapping sub-national jurisdictions to their countries.
    country_members = io.load_json(COUNTRY_MEMBERS_FILE)

    #
    # Process data.
    #
    # Sanity checks on raw data.
    sanity_checks(tb=tb_national)
    sanity_checks(tb=tb_subnational)

    # Prepare data.
    tb = prepare_data(tb_national=tb_national)

    # Harmonize country names.
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Create a simplified table for "any sector" of national data.
    tb_any_sector_national = get_coverage_for_any_sector(tb=tb)

    # Create a simplified table with the coverage for "any sector" of subnational data.
    tb_any_sector_subnational = prepare_subnational_data(tb_subnational=tb_subnational, country_members=country_members)

    # Combine national and subnational data.
    tb_any_sector = combine_national_and_subnational_data(
        tb_any_sector_national=tb_any_sector_national, tb_any_sector_subnational=tb_any_sector_subnational
    )

    # Rename tables.
    tb.metadata.short_name = "world_carbon_pricing"
    tb_any_sector.metadata.short_name = "world_carbon_pricing_any_sector"

    # Set an appropriate index and sort conveniently.
    tb = (
        tb.set_index(["country", "year", "ipcc_code", "product"], verify_integrity=True).sort_index().sort_index(axis=1)
    )
    tb_any_sector = tb_any_sector.set_index(["country", "year"], verify_integrity=True).sort_index().sort_index(axis=1)

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(
        dest_dir=dest_dir,
        tables=[tb, tb_any_sector],
        default_metadata=ds_meadow.metadata,
        check_variables_metadata=True,
    )
    ds_garden.save()
