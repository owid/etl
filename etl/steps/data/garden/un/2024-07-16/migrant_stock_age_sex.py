"""Harmonize the UN's migrant stock by age and sex, and the resident population reported alongside it."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)

# UN M49 gives every country and area a code below 900 and every aggregate one above it, so the
# workbook's own codes tell them apart. The aggregates are dropped: they overlap each other -- SDG
# groupings, M49 regions, development groups and income groups all in one column, three of them
# defined by what they exclude -- and OWID builds its own aggregates from countries anyway.
MAX_COUNTRY_LOCATION_CODE = 894

# The entities the workbook covers, as of the 2020 revision. Asserted so that a revision adding or
# dropping countries is noticed here rather than in whatever reads this table.
EXPECTED_COUNTRIES = 235


def run() -> None:
    #
    # Load inputs.
    #
    ds_meadow = paths.load_dataset("migrant_stock_age_sex")
    tb = ds_meadow.read("migrant_stock_age_sex")

    #
    # Process data.
    #
    tb = tb[tb["location_code"] <= MAX_COUNTRY_LOCATION_CODE].reset_index(drop=True)
    tb = paths.regions.harmonize_names(tb, country_col="country", countries_file=paths.country_mapping_path)

    assert tb["country"].nunique() == EXPECTED_COUNTRIES, (
        f"The workbook now covers {tb['country'].nunique()} countries and areas, not {EXPECTED_COUNTRIES}."
    )
    # Migrants are a subset of the residents counted alongside them, so a country where they are not
    # would mean the two sheets have drifted apart -- except where the UN reports no population at
    # all for a territory, which it does for 34 of them and publishes as zero rather than as a gap.
    over = tb[(tb["population"] > 0) & (tb["migrant_stock"] > tb["population"])]
    assert over.empty, f"Migrant stock exceeds the resident population in {len(over)} rows, e.g. {over.head(3)}"

    tb = tb.format(["country", "year", "location_code", "sex", "age"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)
    ds_garden.save()
