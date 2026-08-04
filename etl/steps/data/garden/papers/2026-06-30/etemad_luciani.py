"""Garden step for Etemad & Luciani's historical energy production data (1900-1979)."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# The tonne of oil equivalent is a conventional unit, defined as exactly 41.868 gigajoules by the UN's
# International Recommendations for Energy Statistics (paragraph 4.21, see
# https://unstats.un.org/unsd/energystats/methodology/documents/IRES-web.pdf).
# So 1 Mtoe = 41.868 PJ, and 1 PJ = 1000 / 3600 TWh, giving 1 Mtoe = 11.63 TWh.
MTOE_TO_TWH = 41.868 * 1e3 / 3600


def run() -> None:
    #
    # Load inputs.
    #
    ds_meadow = paths.load_dataset("etemad_luciani")
    tb = ds_meadow.read("etemad_luciani")

    #
    # Process data.
    #
    # Harmonize country names (reusing the Shift Data Portal mapping, since the data comes from the same file).
    tb = paths.regions.harmonize_names(
        tb,
        countries_file=paths.country_mapping_path,
        excluded_countries_file=paths.excluded_countries_path,
    )

    # Convert fossil fuel production from Mtoe to TWh.
    for source in ["coal", "oil", "gas"]:
        tb[f"{source}_production_twh"] = tb[source] * MTOE_TO_TWH

    # Keep only fossil fuel production columns.
    tb = tb[["country", "year", "coal_production_twh", "oil_production_twh", "gas_production_twh"]]

    # Drop rows with no fossil fuel production data.
    tb = tb.dropna(subset=["coal_production_twh", "oil_production_twh", "gas_production_twh"], how="all").reset_index(
        drop=True
    )

    # Sanity checks.
    assert not tb.duplicated(subset=["country", "year"]).any(), "Duplicate (country, year) rows after harmonization."
    assert int(tb["year"].min()) == 1900 and int(tb["year"].max()) == 1979, "Unexpected Etemad & Luciani year range."
    for col in ["coal_production_twh", "oil_production_twh", "gas_production_twh"]:
        assert (tb[col].dropna() >= 0).all(), f"Negative values in {col}."

    # Set an appropriate index and sort conveniently.
    tb = tb.format(short_name=paths.short_name)

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb])
    ds_garden.save()
