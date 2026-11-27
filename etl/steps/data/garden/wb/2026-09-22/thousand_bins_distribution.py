"""Create the garden dataset of the 1000-bins income distribution, and the welfare and estimation type of each
country-year from PIP."""

from owid.catalog import Table
from owid.catalog import processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Regions are defined with three-letter codes, so we map them to their full names.
REGIONS_MAPPING = {
    "EAS": "East Asia and Pacific",
    "ECS": "Europe and Central Asia",
    "LCN": "Latin America and Caribbean",
    "MEA": "Middle East, North Africa, Afghanistan and Pakistan",
    "NAC": "North America",
    "SAS": "South Asia",
    "SSF": "Sub-Saharan Africa",
}

# How PIP arrived at each country-year's lineup estimate, renamed for readers. "CMD estimation" is PIP's model for
# country-years without usable survey data, which still predicts an income- or consumption-based distribution.
ESTIMATION_TYPES = {
    "survey": "survey",
    "interpolation": "interpolated",
    "extrapolation": "extrapolated",
    "CMD estimation": "modeled",
}


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("thousand_bins_distribution")

    # Read table from meadow dataset.
    tb = ds_meadow.read("thousand_bins_distribution")

    # Load PIP meadow dataset (same release as the bins), which carries the welfare type of its lineup estimates.
    ds_pip = paths.load_dataset("world_bank_pip", channel="meadow")
    tb_pip = ds_pip.read("world_bank_pip", safe_types=False)

    #
    # Process data.
    #

    # Rename columns, regions and multiply pop by 1,000,000.
    tb = rename_columns_regions_and_multiply_pop(tb=tb, regions_mapping=REGIONS_MAPPING)

    # Assert that there are no negative values for avg and that avg data is monotonically increasing by each quantile.
    tb = sanity_checks(tb=tb)

    # Build the welfare type (income or consumption) and estimation type behind each country-year of the bins.
    tb_welfare_type = create_welfare_type_table(tb_pip=tb_pip, tb=tb)

    tb = paths.regions.harmonize_names(tb=tb)
    tb_welfare_type = paths.regions.harmonize_names(tb=tb_welfare_type)

    # Set an appropriate index and sort conveniently.
    tb = tb.format(["country", "year", "region", "quantile"])
    tb_welfare_type = tb_welfare_type.format(["country", "year"], short_name="welfare_type")

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(tables=[tb, tb_welfare_type], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()


def rename_columns_regions_and_multiply_pop(tb: Table, regions_mapping: dict) -> Table:
    """Rename columns, regions and multiply pop by 1,000,000."""
    # Rename columns
    tb = tb.rename(
        columns={"region_code": "region", "welf": "avg", "code": "country"},
        errors="raise",
    )

    # Rename region column with REGIONS_MAPPING. Assert that all regions are mapped.
    assert set(tb["region"].unique()) == set(REGIONS_MAPPING.keys()), (
        f"There are undefined regions in `region`: {set(tb['region'].unique()) - set(REGIONS_MAPPING.keys())}"
    )
    tb["region"] = tb["region"].map(REGIONS_MAPPING)

    # Multiply pop by 1,000,000
    tb["pop"] *= 1e6

    # Drop region_name column. Also drop pipvintage column as it is not needed
    tb = tb.drop(columns=["region_name", "pipvintage"])

    return tb


def create_welfare_type_table(tb_pip: Table, tb: Table) -> Table:
    """Create a country-year table of the welfare type (income or consumption) and estimation type behind the bins.

    The bins follow PIP's lineup estimates, which are PIP's filled data. PIP labels every lineup row with the welfare
    type it is based on, including the rows it models where no usable survey exists: its model predicts either an
    income- or a consumption-based distribution (PIP handbook, section 5.8). So both columns are PIP's own labels, with
    no inference. Both tables still use ISO codes in `country` here, so they are matched on code before harmonization.
    """
    # Keep lineup estimates for countries, in 2021 PPPs like the bins (regions have no welfare type).
    mask = tb_pip["filled"] & (tb_pip["ppp_version"] == 2021) & tb_pip["welfare_type"].notna()
    tb_pip = tb_pip.loc[mask, ["country_code", "year", "reporting_level", "welfare_type", "estimation_type"]]
    # Use national estimates, or the only reporting level a country has (e.g. urban for Argentina).
    has_national = tb_pip.groupby(["country_code", "year"], observed=True)["reporting_level"].transform(
        lambda levels: (levels == "national").any()
    )
    tb_pip = tb_pip.loc[(tb_pip["reporting_level"] == "national") | ~has_national]
    tb_welfare_type = (
        tb_pip[["country_code", "year", "welfare_type", "estimation_type"]]
        .drop_duplicates()
        .rename(columns={"country_code": "country"}, errors="raise")
        .reset_index(drop=True)
    )
    tb_welfare_type["welfare_type"] = tb_welfare_type["welfare_type"].astype("string")

    # Each country-year must have a single welfare and estimation type across poverty lines.
    duplicated = tb_welfare_type[tb_welfare_type.duplicated(["country", "year"], keep=False)]
    assert duplicated.empty, f"Country-years with more than one welfare or estimation type in PIP:\n{duplicated}"
    assert set(tb_welfare_type["welfare_type"]) <= {"income", "consumption"}, (
        f"Unexpected welfare types: {set(tb_welfare_type['welfare_type'])}"
    )
    unexpected = set(tb_welfare_type["estimation_type"].astype("string")) - set(ESTIMATION_TYPES)
    assert not unexpected, f"Unexpected estimation types in PIP: {unexpected}"
    tb_welfare_type["estimation_type"] = (
        tb_welfare_type["estimation_type"].astype("string").map(ESTIMATION_TYPES).astype("string")
    )

    # Keep the country-years of the bins, and check that every one of them has a match in PIP.
    tb_country_years = tb[["country", "year"]].drop_duplicates()
    tb_welfare_type = pr.merge(tb_country_years, tb_welfare_type, on=["country", "year"], how="left", indicator=True)
    missing = tb_welfare_type[tb_welfare_type["_merge"] != "both"]
    assert missing.empty, f"Country-years of the bins missing from PIP lineup estimates:\n{missing}"
    tb_welfare_type = tb_welfare_type.drop(columns=["_merge"])
    assert tb_welfare_type[["welfare_type", "estimation_type"]].notna().all().all(), (
        "Country-years of the bins with no welfare or estimation type."
    )

    return tb_welfare_type


def sanity_checks(tb: Table) -> Table:
    """
    Check that there are no negative values for avg.
    Check that data is monotonically increasing in quantile.
    """
    # Check that there are no negative values for avg.

    mask = tb["avg"] < 0
    if not tb[mask].empty:
        paths.log.info(f"There are {len(tb[mask])} negative values for avg and will be transformed to zero.")
        tb["avg"] = tb["avg"].clip(lower=0)

    # Check that data is monotonically increasing in avg by country, year and quantile.
    tb = tb.sort_values(by=["country", "year", "quantile"]).reset_index(drop=True)

    mask = tb.groupby(["country", "year"])["avg"].diff() < 0

    if not tb[mask].empty:
        paths.log.info(f"There are {len(tb[mask])} values for avg that are not monotonically increasing.")
        paths.log.info("These values will be transformed to the previous value.")

        tb.loc[mask, "avg"] = tb.groupby(["country", "year"])["avg"].shift(1).loc[mask]

    return tb
