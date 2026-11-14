"""Load Eurostat's annual poultry data (the activity of hatcheries) and create a garden dataset with the chicks of laying
hen breeds hatched in European countries.

Eurostat counts chicks hatched for use in the country where they hatch; day-old chicks are also traded, so the chicks
placed in a country are those hatched, minus those exported, plus those imported. Reared pullets traded between
countries are not covered by these statistics.
"""

from owid.catalog import Table
from structlog import get_logger

from etl.helpers import PathFinder

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Eurostat codes of the types of chick, and their names.
CHICK_TYPES = {"A5130O": "laying_hen_chicks", "A5131P": "cockerels_from_sexing"}
# Eurostat codes of the hatchery items, and their names: chicks hatched for use, imported, and exported.
ITEMS = {"CU": "hatched", "IMP": "imported", "EXP": "exported"}
# Indicators to keep (cockerels from sexing are only kept as hatched).
INDICATORS = [
    "laying_hen_chicks_hatched",
    "laying_hen_chicks_imported",
    "laying_hen_chicks_exported",
    "cockerels_from_sexing_hatched",
]
# Countries whose chicks hatched already exclude the chicks exported, so these are not subtracted again.
# NOTE: Eurostat's metadata (SIMS 13.3.4): "The number of chicks hatched is expected to include the chicks that will then
# be exported. For all the countries but BE, this is the case."
COUNTRIES_HATCHED_WITHOUT_EXPORTS = ["Belgium"]


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its main table.
    ds_meadow = paths.load_dataset("poultry_hatcheries")
    tb = ds_meadow.read("poultry_hatcheries")

    #
    # Process data.
    #
    sanity_check_inputs(tb=tb)

    # Select the chicks of laying hen breeds and the cockerels from sexing, and name each indicator.
    tb = tb[tb["chick_type"].isin(CHICK_TYPES) & tb["item"].isin(ITEMS)].dropna(subset=["value"])
    tb["indicator"] = tb["chick_type"].map(CHICK_TYPES) + "_" + tb["item"].map(ITEMS)
    tb = tb[tb["indicator"].isin(INDICATORS)].reset_index(drop=True)
    tb = tb.pivot(index=["country", "year"], columns="indicator", values="value", join_column_levels_with="_")

    # Harmonize country names (EU aggregates are excluded).
    tb = paths.regions.harmonize_names(tb, countries_file=paths.country_mapping_path)

    # Eurostat publishes numbers of chicks in thousands. Convert them into chicks.
    for column in INDICATORS:
        tb[column] *= 1000

    # Chicks of laying hen breeds placed in the country: hatched, minus exported, plus imported (only in years where
    # the three are reported).
    tb["laying_hen_chicks_placed"] = (
        tb["laying_hen_chicks_hatched"] - tb["laying_hen_chicks_exported"] + tb["laying_hen_chicks_imported"]
    )
    # Where chicks hatched already exclude the chicks exported, add only the chicks imported.
    without_exports = tb["country"].isin(COUNTRIES_HATCHED_WITHOUT_EXPORTS)
    tb.loc[without_exports, "laying_hen_chicks_placed"] = (
        tb.loc[without_exports, "laying_hen_chicks_hatched"] + tb.loc[without_exports, "laying_hen_chicks_imported"]
    )

    sanity_check_outputs(tb=tb)

    # Improve table format.
    tb = tb.format(["country", "year"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb])
    ds_garden.save()


def sanity_check_inputs(tb: Table) -> None:
    error = "Expected types of chick or hatchery items are missing."
    assert set(CHICK_TYPES) <= set(tb["chick_type"]) and set(ITEMS) <= set(tb["item"]), error
    error = "All values should be non-negative."
    assert (tb["value"].dropna() >= 0).all(), error


def sanity_check_outputs(tb: Table) -> None:
    error = "Expected countries are missing."
    assert {"France", "Germany", "Spain", "Italy", "Poland", "Netherlands", "Turkey", "United Kingdom"} <= set(
        tb["country"]
    ), error
    # Magnitude check (guards the conversion from thousands): France hatches tens of millions of laying hen chicks.
    france = tb[(tb["country"] == "France") & (tb["year"] == 2024)]["laying_hen_chicks_hatched"].item()
    error = f"Unexpected number of laying hen chicks hatched in France in 2024: {france:,.0f}."
    assert 2e7 < france < 1e8, error
    # Countries whose chicks hatched already exclude exports should never get a negative number of chicks placed.
    placed = tb[tb["country"].isin(COUNTRIES_HATCHED_WITHOUT_EXPORTS)]["laying_hen_chicks_placed"].dropna()
    error = "Negative chicks placed in a country whose chicks hatched already exclude exports."
    assert not placed.empty and (placed >= 0).all(), error
    # Some countries report more cockerels from sexing than laying hen chicks in some years, which Eurostat attributes
    # to differences in how countries interpret the category. Flag them for review rather than fail.
    suspicious = tb[tb["cockerels_from_sexing_hatched"] > tb["laying_hen_chicks_hatched"]]
    if not suspicious.empty:
        summary = suspicious.groupby("country")["year"].agg(
            lambda years: f"{len(years)} years, {years.min()}-{years.max()}"
        )
        log.warning(f"Countries with more cockerels from sexing than laying hen chicks hatched: {summary.to_dict()}")
    error = "Indicators should carry the origin of the snapshot."
    assert all(len(tb[column].metadata.origins) == 1 for column in INDICATORS + ["laying_hen_chicks_placed"]), error
