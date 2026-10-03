"""Load a meadow dataset and create a garden dataset."""

import json

from owid.catalog import Table
from owid.catalog import processing as pr
from structlog import get_logger

from etl.data_helpers import geo
from etl.data_helpers.geo import add_region_aggregates, list_countries_in_region
from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("consumption_controlled_substances: start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("consumption_controlled_substances")

    # Read table from meadow dataset.
    tb = ds_meadow["consumption_controlled_substances"].reset_index()

    #
    # Process data.
    #
    log.info("consumption_controlled_substances: process data, creating table")
    tb_garden = process_table(tb)
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_garden], default_metadata=ds_meadow.metadata)
    # Update metadata
    ds_garden.update_metadata(paths.metadata_path)
    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("consumption_controlled_substances: end")


def process_table(tb: Table) -> Table:
    # Dropna
    tb = tb.dropna(subset=["consumption"]).astype({"consumption": "float32"})
    # Check country mapping
    _check_country_mapping()
    # Harmonize countries
    log.info("consumption_controlled_substances: harmonizing countries")
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    # Add EU28
    log.info("consumption_controlled_substances: add regions")
    tb = add_regions(tb)
    # Estimate total consumption of ozone-depleting substances (summation over all chemicals except HFCs)
    log.info("consumption_controlled_substances: estimating total")
    chemicals_ignore = [
        "Hydrofluorocarbons (HFCs)",
    ]
    tb_depleting = tb[~tb["chemical"].isin(chemicals_ignore)]
    tb_total = (
        tb_depleting.groupby(["country", "year"], observed=True, as_index=False)[["consumption"]]
        .sum()
        .assign(chemical="All (Ozone-depleting)")
    )
    tb = pr.concat([tb, tb_total], ignore_index=True).sort_values(["country", "year", "chemical"])
    # Add zero-filled column
    tb = add_consumption_zerofilled(tb)
    # Add consumption relative to 1986
    tb = add_consumption_rel_1986(tb)
    # Remove data for regions in last year
    tb = remove_last_year_for_regions(tb)
    # Set indices
    tb = tb.set_index(["country", "year", "chemical"])
    # Drop NaNs and set dtype
    tb = tb.astype({"consumption": "float32", "consumption_zf": "float32"})
    tb.metadata.short_name = paths.short_name
    return tb


def add_regions(tb: Table) -> Table:
    id_vars = ["country", "year"]
    var_name = "chemical"
    value_name = "consumption"
    # Add data for the World
    tb_world = tb.groupby(["year", "chemical"], as_index=False)[[value_name]].sum().assign(country="World")
    tb = pr.concat([tb, tb_world], ignore_index=True)
    # Pivot
    tb_pivot = tb.pivot(index=id_vars, columns=[var_name], values=value_name).reset_index()
    # Add continent data
    regions = ["Asia", "Africa", "North America", "South America", "Oceania"]
    # Load population
    population = paths.load_dataset("population").read("population")
    for region in regions:
        countries_that_must_have_data = geo.list_countries_in_region_that_must_have_data(
            region=region,
            population=population,
        )
        tb_pivot = add_region_aggregates(
            tb_pivot,
            region=region,
            countries_that_must_have_data=countries_that_must_have_data,
            frac_allowed_nans_per_year=0.2,
            num_allowed_nans_per_year=None,
        )
    # Unpivot back
    tb = tb_pivot.melt(id_vars=id_vars, var_name=var_name, value_name=value_name).dropna(subset=[value_name])
    # Add EU28 data
    tb = add_eu28(tb)
    # Add Europe data
    tb = add_europe(tb)
    return tb


def add_eu28(tb: Table) -> Table:
    """Add EU28 data to the table.

    This dataset provides data for European Union as a changing entity (i.e. member states vary over time). This
    function estimates EU 28, as a fixed entity, by summing up the data for all EU 28 members over time.

    EU 27 cannot be estimated because there is no UK data in the dataset prior to Brexit (2021).

    It removes the data for individual EU 28 member states.
    """
    # Get list of all EU28 members
    eu28_members = list_countries_in_region("European Union (27)") + ["United Kingdom", "European Union"]
    # Add EU28 data
    tb = _add_region(tb, eu28_members, "European Union (28)")
    return tb


def add_europe(tb: Table) -> Table:
    assert "European Union (28)" in tb.country.unique(), (
        "Check data! It looks like `European Union (28)` is not present."
    )
    # EU states
    europe_members = list_countries_in_region("Europe") + ["European Union (28)"]
    assert len(set(tb.country).intersection(europe_members)) == 18, (
        "Check data! It might be that individual EU 28 member states are still present."
    )
    # Add EU data
    tb = _add_region(tb, europe_members, "Europe", remove_members=False)
    return tb


def _add_region(tb: Table, members: list[str], region: str, remove_members: bool = True) -> Table:
    """Aggregate data for a region.

    This function is useful when adding regions that are not currently considered by etl.data_helpers.geo.add_region_aggregates.
    For instance "Europe Union (28)". Or when a region is built differently, e.g. Europe = EU 28 + ...
    """
    # Mask
    msk_region = tb["country"].isin(members)
    tb_region = tb[msk_region].copy()
    tb_region["country"] = region
    tb_region = tb_region.groupby(["country", "year", "chemical"], as_index=False)[["consumption"]].sum()
    if remove_members:
        tb = pr.concat([tb[~msk_region], tb_region], ignore_index=True)
    else:
        tb = pr.concat([tb, tb_region], ignore_index=True)
    return tb


def _check_country_mapping():
    with open(paths.country_mapping_path) as f:
        dix = json.load(f)
    assert len(dix.values()) == len(set(dix.values())), (
        "There are multiple countries with the same standardised name. Join step in Meadow might not be working"
        " properly."
    )


def add_consumption_zerofilled(tb: Table) -> Table:
    id_vars = ["country", "year"]
    var_name = "chemical"
    value_name = "consumption"
    tb = tb.pivot(index=id_vars, columns=[var_name], values=value_name).reset_index()
    tb = tb.melt(id_vars=id_vars, var_name=var_name, value_name=value_name)
    tb["consumption_zf"] = tb["consumption"].fillna(0)
    return tb


def add_consumption_rel_1986(tb: Table) -> Table:
    """Add column with ratio of consumption to 1986 consumption."""
    # Initial columns and new column names
    columns = list(tb.columns)
    new_col = "consumption_rel_1986"
    # Get consumption in 1986, where it is not zero
    tb_1986 = tb[(tb["year"] == 1986) & (tb["consumption"] > 0)]
    # Merge and estimate ratio
    tb = tb.merge(tb_1986, on=["country", "chemical"], suffixes=("", "_1986"), how="left")
    tb[new_col] = (100 * tb["consumption"] / tb["consumption_1986"]).round(2)
    return tb[columns + [new_col]]


def remove_last_year_for_regions(tb: Table) -> Table:
    """Remove datapoint for latest available year in regions.

    Data for latest year for regions is usually an underestimate, because just a subset of countries have reported data."""
    REGIONS = [
        "Africa",
        "Asia",
        "Europe",
        "European Union (27)",
        "European Union (28)",
        "North America",
        "Oceania",
        "South America",
        "World",
    ]
    last_year = tb["year"].max()
    tb = tb[~((tb["year"] == last_year) & (tb["country"].isin(REGIONS)))]
    return tb
