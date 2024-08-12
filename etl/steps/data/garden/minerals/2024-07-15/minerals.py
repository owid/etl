"""Compilation of minerals data from different origins.

Currently, the three sources of minerals data are:
* From BGS we extract imports, exports, and production.
* From USGS (current) we extract production and reserves.
* From USGS (historical) we extract production and unit value.

Initially, I thought of creating long tables in the garden steps of USGS current, USGS historical, and BGS data.

However, there is little overlap between the three origins (at the country-year-commodity-subcommodity-unit level).
If we combined all production data into one column (as it would be common to do in a garden step), the resulting data
would show as having 3 origins, whereas in reality most data points would come from just one origin.

So it seems more convenient to create wide tables, where each column has its own origin, and then combine the wide
tables (on those few columns where there is overlap).

"""
import warnings
from typing import List, Optional, Tuple

import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Dataset, Table, VariablePresentationMeta
from owid.datautils.dataframes import combine_two_overlapping_dataframes
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Initialize logger.
log = get_logger()

# Ignore PerformanceWarning (that happens when finding the maximum of world data, when creating region aggregates).
warnings.simplefilter(action="ignore", category=pd.errors.PerformanceWarning)

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Prefix used for "share" columns.
SHARE_OF_GLOBAL_PREFIX = "share_of_global_"

# There are many historical regions with overlapping data with their successor countries.
# Accept only overlaps on the year when the historical country stopped existing.
ACCEPTED_OVERLAPS = [
    {1991: {"USSR", "Armenia"}},
    # {1991: {"USSR", "Belarus"}},
    {1991: {"USSR", "Russia"}},
    {1992: {"Czechia", "Czechoslovakia"}},
    {1992: {"Slovakia", "Czechoslovakia"}},
    {1990: {"Germany", "East Germany"}},
    {1990: {"Germany", "West Germany"}},
    {2010: {"Netherlands Antilles", "Bonaire Sint Eustatius and Saba"}},
    {1990: {"Yemen", "Yemen People's Republic"}},
]


def adapt_flat_table(tb_flat: Table) -> Table:
    tb_flat = tb_flat.copy()
    # For consistency with other tables, rename columns and adapt column types.
    tb_flat = tb_flat.rename(
        columns={
            column: tb_flat[column].metadata.title for column in tb_flat.columns if column not in ["country", "year"]
        },
        errors="raise",
    )
    tb_flat = tb_flat.astype({column: "Float64" for column in tb_flat.columns if column not in ["country", "year"]})
    return tb_flat


def _gather_notes_and_footnotes_from_metadata(tb_flat: Table, column: str) -> Tuple[Optional[str], Optional[str]]:
    # Get notes from description_from_producer field, if given.
    # And gather footnotes from grapher config, if given.
    notes = None
    footnotes = None
    # For "share" columns, we also want to fetch the notes of the original column.
    column = column.replace(SHARE_OF_GLOBAL_PREFIX, "")
    if column in tb_flat.columns:
        notes = tb_flat[column].metadata.description_from_producer
        if tb_flat[column].metadata.presentation.grapher_config is not None:
            footnotes = tb_flat[column].metadata.presentation.grapher_config["note"]

    return notes, footnotes


def _combine_notes(notes_list: List[Optional[str]], separator: str) -> str:
    # Add notes to metadata.
    combined_notes = ""
    notes_exist = False
    for notes in notes_list:
        if notes:
            if notes_exist:
                combined_notes += separator
            combined_notes += notes
            notes_exist = True

    return combined_notes


def improve_metadata(tb: Table, tb_usgs_flat: Table, tb_bgs_flat: Table, tb_usgs_historical_flat: Table) -> Table:
    tb = tb.copy()

    # Improve metadata of new columns.
    for column in tb.drop(columns=["country", "year"]).columns:
        # Extract combination from the column name.
        metric, commodity, sub_commodity, unit = column.split("|")

        # Improve metric, commodity and subcommodity names.
        metric = metric.replace("_", " ").capitalize()
        commodity = commodity.capitalize()

        # Ensure the variable has a title.
        tb[column].metadata.title = column

        # Create a title_public.
        if sub_commodity == "Refinery":
            title_public = f"Refined {commodity.lower()} {metric.lower()}"
        else:
            title_public = f"{commodity} {metric.lower()}"
        if tb[column].metadata.presentation is None:
            tb[column].metadata.presentation = VariablePresentationMeta()
        if column.startswith(SHARE_OF_GLOBAL_PREFIX):
            title_public = title_public.replace("share of global ", "") + " as a share of the global total"
        tb[column].metadata.presentation.title_public = title_public

        # Create a short description (that will also appear as the chart subtitle).
        description_short = None
        if metric in [
            "Production",
            "Imports",
            "Exports",
            "Share of global production",
            "Share of global imports",
            "Share of global exports",
        ]:
            if sub_commodity == "Mine":
                if not metric.startswith("Share"):
                    description_short = f"Measured as mined production, in {unit}."
                else:
                    description_short = (
                        "Measured based on mined, rather than [refined](#dod:refined-production), production."
                    )
            elif sub_commodity == "Refinery":
                if not metric.startswith("Share"):
                    description_short = f"Measured in {unit}. Mineral [refining](#dod:refined-production) takes mined or raw minerals, and separates them into a final product of pure metals and minerals."
                else:
                    description_short = "Mineral [refining](#dod:refined-production) takes mined or raw minerals, and separates them into a final product of pure metals and minerals."
            elif sub_commodity == "Smelter":
                description_short = f"Measured in {unit}. [Smelting](#dod:smelting) takes raw minerals and produces metals through high-temperature processes."
        elif metric in ["Reserves", "Share of global reserves"]:
            description_short = "Mineral [reserves](#dod:mineral-reserve) are resources that have been evaluated and can be mined economically with current technologies."
        elif metric == "Unit value":
            description_short = (
                f"Value of 1 tonne of {commodity.lower()}, in US dollars per tonne, adjusted for inflation."
            )
        if description_short is not None:
            tb[column].metadata.description_short = description_short

        # Add unit and short_unit.
        tb[column].metadata.unit = unit
        if unit.startswith("tonnes"):
            # Create short unit.
            tb[column].metadata.short_unit = "t"
        elif unit == "constant 1998 US$ per tonne":
            tb[column].metadata.short_unit = "$/t"
        else:
            log.warning(f"Unexpected unit for column: {column}")
            tb[column].metadata.short_unit = ""
        if metric.startswith("Share "):
            tb[column].metadata.unit = "%"
            tb[column].metadata.short_unit = "%"

        # Gather notes and footnotes from USGS' current metadata.
        notes_usgs, footnotes_usgs = _gather_notes_and_footnotes_from_metadata(tb_flat=tb_usgs_flat, column=column)

        # Gather notes and footnotes from USGS' historical metadata.
        notes_usgs_historical, footnotes_usgs_historical = _gather_notes_and_footnotes_from_metadata(
            tb_flat=tb_usgs_historical_flat, column=column
        )

        # Gather notes and footnotes from BGS' metadata.
        notes_bgs, footnotes_bgs = _gather_notes_and_footnotes_from_metadata(tb_flat=tb_bgs_flat, column=column)

        # Add notes to metadata.
        combined_notes = _combine_notes(notes_list=[notes_bgs, notes_usgs, notes_usgs_historical], separator="\n\n")
        if len(combined_notes) > 0:
            tb[column].metadata.description_from_producer = combined_notes

        # Insert (or append) additional footnotes:
        footnotes_additional = ""
        if metric in ["Reserves", "Share of global reserves"]:
            footnotes_additional += "Reserves can increase over time as new mineral deposits are discovered and others become economically feasible to extract."
        elif metric == "Unit value":
            footnotes_additional += "This data is expressed in constant 1998 US$ per tonne."

        # Add footnotes to metadata.
        combined_footnotes = _combine_notes(
            notes_list=[footnotes_bgs, footnotes_usgs, footnotes_usgs_historical, footnotes_additional], separator=" "
        )
        if len(combined_footnotes) > 0:
            if tb[column].metadata.presentation.grapher_config is None:
                tb[column].metadata.presentation.grapher_config = {}
            tb[column].metadata.presentation.grapher_config["note"] = combined_footnotes

    return tb


def inspect_overlaps(
    tb: Table,
    tb_usgs_flat: Table,
    tb_usgs_historical_flat: Table,
    tb_bgs_flat: Table,
    minerals: Optional[List[str]] = None,
) -> None:
    import pandas as pd
    import plotly.express as px

    for column in tb.drop(columns=["country", "year"]).columns:
        if minerals:
            mineral = column.split("|")[1]
            if mineral not in minerals:
                continue
        # Initialize an empty dataframe, and add data to it from whatever source has data for it.
        _df = pd.DataFrame()
        if column in tb_bgs_flat.columns:
            _df = pd.concat(
                [_df, tb_bgs_flat[["country", "year", column]].assign(**{"source": "BGS"})], ignore_index=True
            )
        if column in tb_usgs_flat.columns:
            _df = pd.concat(
                [_df, tb_usgs_flat[["country", "year", column]].assign(**{"source": "USGS current"})], ignore_index=True
            )
        if column in tb_usgs_historical_flat.columns:
            _df = pd.concat(
                [_df, tb_usgs_historical_flat[["country", "year", column]].assign(**{"source": "USGS historical"})],
                ignore_index=True,
            )
        _df = _df.dropna().reset_index(drop=True)
        for country in _df["country"].unique():
            _df_country = _df[_df["country"] == country]
            if _df_country["source"].nunique() > 1:
                px.line(
                    _df_country, x="year", y=column, color="source", markers=True, title=f"{column} - {country}"
                ).show()


def add_share_of_global_columns(tb: Table) -> Table:
    # Create a table for global data.
    tb_world = tb[tb["country"] == "World"].drop(columns="country").reset_index(drop=True)
    # Replace zeros by nan (to avoid division by zero).
    tb_world = tb_world.replace(0, pd.NA)
    # Create a temporary table with global data for each column in the original table.
    _tb = tb.merge(tb_world, on="year", how="left", suffixes=("", "_world_share"))

    # Create a dictionary to store the new share columns, and another to store each column's metadata.
    new_columns = {}
    metadata = {}
    for column in tb.columns:
        if (column.split("|")[0] in ["exports", "imports", "production", "reserves"]) and (
            tb_world[column].notnull().any()
        ):
            new_columns[f"{SHARE_OF_GLOBAL_PREFIX}{column}"] = _tb[column] / _tb[f"{column}_world_share"] * 100
            metadata[f"{SHARE_OF_GLOBAL_PREFIX}{column}"] = tb[column].metadata

    # Add the new share columns to the original table.
    tb = pr.concat([tb, Table(new_columns)], axis=1)

    for column in tb.drop(columns=["country", "year"]).columns:
        if len(tb[column].metadata.origins) == 0:
            tb[column].metadata = metadata[column].copy()

    return tb


def add_global_data(tb: Table, ds_regions: Dataset) -> Table:
    # Firstly, remove "World (BGS)", which is the global aggregate that was created by us in the BGS garden step.
    tb = tb[tb["country"] != "World (BGS)"].reset_index(drop=True)

    # There are 22 columns that exist both in BGS and USGS, and where USGS includes "Other".
    # I checked if "Other" is usually small compared to the contribution of all other BGS countries, but it depends.
    # About half of the times, "Other" is larger than the sum of all other BGS countries, and half it's smaller.
    # for column in tb.drop(columns=["country", "year"]).columns:
    #     if (column in tb_usgs_flat.columns) and (column in tb_bgs_flat.columns):
    #         if (not tb_usgs_flat[tb_usgs_flat["country"] == "Other"][column].dropna().empty) and (not tb_bgs_flat[column].dropna().empty):
    #             usgs_countries = set(tb_usgs_flat[~tb_usgs_flat["country"].isin(["Other", "World"])].dropna(subset=column)["country"])
    #             bgs_countries = set(tb_bgs_flat[tb_bgs_flat["country"] != "World (BGS)"].dropna(subset=column)["country"])
    #             other = bgs_countries - usgs_countries
    #             print(len(other))
    #             bgs_other = tb_bgs_flat[tb_bgs_flat["country"].isin(other)].groupby(["year"], observed=True, as_index=False).agg({column: "sum"})
    #             usgs_other = tb_usgs_flat[tb_usgs_flat["country"] == "Other"].groupby(["year"], observed=True, as_index=False).agg({column: "sum"})
    #             compared = bgs_other.merge(usgs_other, on="year", how="inner", suffixes=("_bgs", "_usgs"))
    #             compared["ape"] = abs(compared[f"{column}_usgs"] - compared[f"{column}_bgs"]) / compared[f"{column}_bgs"] * 100

    # The safest option is to remove "Other": After combining BGS and USGS, it's impossible to know which countries
    # are included in "Other", and it can therefore be misleading.
    # Note also that initially we created regions for both BGS and USGS, and, when comparing them, the region aggregates
    # from USGS were almost always smaller than those in BGS. This indicates that BGS tends to have data disaggregated
    # for more countries. This does not necessarily mean that "World" is usually smaller in USGS (it isn't).
    # It simply means that "Other" carries an important contribution in USGS data, and it's always missing in regions.
    # Therefore, it doesn't make much sense to keep USGS regions, or to keep "Other".
    tb = tb[tb["country"] != "Other"].reset_index(drop=True)

    # Create the usual regions, to be able to safely construct the aggregate for "World".
    regions = {
        "Africa": {},
        "Asia": {},
        "Europe": {},
        "North America": {},
        "Oceania": {},
        "South America": {},
        "World": {},
    }
    tb = geo.add_regions_to_table(
        tb=tb,
        regions=regions,
        ds_regions=ds_regions,
        min_num_values_per_year=1,
        accepted_overlaps=ACCEPTED_OVERLAPS,
        keep_original_region_with_suffix=" (USGS)",
    )

    # Now that we have a World aggregate (and we are sure there is no double-counting) remove all other regions.
    regions_to_remove = [region for region in regions if region != "World"]
    tb = tb.loc[~tb["country"].isin(regions_to_remove)].reset_index(drop=True)

    # Take the maximum between the World aggregate we just created, and the original World from USGS.
    # Ideally, we would first gather as much data from all countries as possible, and then create the aggregate for the
    # World. But we often have data only for the US and/or the world (from USGS).
    # In such cases, World obviously includes data from many countries that are not disaggregated in the data.
    # Therefore, in those cases, we want to use the original "World" instead.
    # But in other cases, the sum of individual countries is larger than USGS's World, which could be because USGS'
    # data does not account for all countries. In those cases, we want to use the sum of individual countries.
    # Therefore, we take the maximum between the two.
    # However, there is a caveat: Maybe BGS data for individual countries is inaccurate, or we are combining two
    # indicators that mean slightly different things.
    # To minimize this risk, we first visually inspected the data (where there was overlap between BGS and USGS).
    # But note that the risk is still there, in cases where BGS and USGS do not overlap.

    # Sanity check: The only repeated rows should be for World (namely the original and the current aggregate).
    error = "Unexpected duplicated data."
    world_entities = ["World", "World (USGS)"]
    assert set(
        tb[tb.replace("World (USGS)", "World").duplicated(subset=["country", "year"], keep=False)]["country"]
    ) == set(world_entities), error
    # Create a table with world data, keeping only the maximum (between the aggregate and the original USGS world data).
    tb_world_max = (
        tb[tb["country"].isin(world_entities)]
        .replace("World (USGS)", "World")
        .groupby(["country", "year"], observed=True, as_index=False)
        .max()
    )

    # Replace the world data in the main table with the world maximum data.
    tb = pr.concat([tb[~tb["country"].isin(world_entities)], tb_world_max], ignore_index=True)
    assert tb[tb.duplicated(subset=["country", "year"], keep=False)].empty, error

    return tb


def run_sanity_checks(tb: Table) -> None:
    # Check that there are no duplicated rows.
    assert tb[tb.duplicated(subset=["country", "year"], keep=False)].empty, "Unexpected duplicated data."
    # Check that there are no missing values.
    for column in [column for column in tb.columns if column.startswith(SHARE_OF_GLOBAL_PREFIX)]:
        if (tb[column] > 100).any():
            log.warning(f"{column} maximum: {tb[column].max():.0f}%")


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load minerals datasets.
    ds_bgs = paths.load_dataset("world_mineral_statistics")
    ds_usgs_historical = paths.load_dataset("historical_statistics_for_mineral_and_material_commodities")
    ds_usgs = paths.load_dataset("mineral_commodity_summaries")

    # Load regions dataset.
    ds_regions = paths.load_dataset("regions")

    # Read tables.
    tb_usgs_historical_flat = ds_usgs_historical.read_table(
        "historical_statistics_for_mineral_and_material_commodities_flat"
    )
    tb_usgs_flat = ds_usgs.read_table("mineral_commodity_summaries_flat")
    tb_bgs_flat = ds_bgs.read_table("world_mineral_statistics_flat")

    #
    # Process data.
    #
    # Adapt USGS current flat table.
    tb_usgs_flat = adapt_flat_table(tb_flat=tb_usgs_flat)

    # Adapt USGS historical flat table.
    tb_usgs_historical_flat = adapt_flat_table(tb_flat=tb_usgs_historical_flat)

    # Adapt BGS flat table.
    tb_bgs_flat = adapt_flat_table(tb_flat=tb_bgs_flat)

    # Given that "World" was created by us (to be able to visually compare BGS and USGS data), rename it.
    # NOTE: We will rename it back to "World" just to allow for visual inspection.
    #  Then, "World (BGS)" will be removed from the data, before creating region aggregates.
    tb_bgs_flat["country"] = tb_bgs_flat["country"].astype("string").replace("World", "World (BGS)")

    # Create a combined flat table.
    # Firstly, combine USGS current and historical. Since the former is more up-to-date, prioritize it.
    # TODO: Sometimes, when combining, the data from one of the sources is completely replaced by the other.
    #  In such cases, it would be better if only the origins of the remaining one appear in the metadata.
    #  This happens, e.g. for Steel production (BGS goes from 1970 to 2022, whereas USGS (historical+current) data goes
    #  from 1943 to 2023). So the result should quote only USGS historical and current as origins.
    tb = combine_two_overlapping_dataframes(
        df1=tb_usgs_flat, df2=tb_usgs_historical_flat, index_columns=["country", "year"]
    )
    # Then, combine the result with BGS data. After inspection, it seems that, where USGS and BGS data overlap, BGS is
    # usually more complete. All region aggregates from BGS have larger values than USGS' region aggregates (even though
    # data for individual countries agrees reasonably well). However, the latest year is always from USGS.
    # So, I decided to remove region aggregates from USGS, and prioritize USGS over BGS data.
    tb = combine_two_overlapping_dataframes(df1=tb, df2=tb_bgs_flat, index_columns=["country", "year"])

    # # Uncomment for debugging purposes, to compare the data from different origins where they overlap.
    # inspect_overlaps(
    #     tb=tb,
    #     tb_usgs_flat=tb_usgs_flat,
    #     tb_usgs_historical_flat=tb_usgs_historical_flat,
    #     tb_bgs_flat=tb_bgs_flat.replace("World (BGS)", "World"),
    #     minerals=["Feldspar"],
    # )

    # Create region aggregates.
    # NOTE: "Other" will be removed from the data (see notes inside the function).
    tb = add_global_data(tb=tb, ds_regions=ds_regions)

    # Create columns for share of world (i.e. production, import, exports and reserves as a share of global).
    tb = add_share_of_global_columns(tb=tb)

    # Improve metadata.
    tb = improve_metadata(
        tb=tb, tb_usgs_flat=tb_usgs_flat, tb_bgs_flat=tb_bgs_flat, tb_usgs_historical_flat=tb_usgs_historical_flat
    )

    # Run sanity checks.
    run_sanity_checks(tb=tb)

    # Format combined table conveniently.
    tb = tb.format(["country", "year"], short_name="minerals").astype("Float64")

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_garden.save()
