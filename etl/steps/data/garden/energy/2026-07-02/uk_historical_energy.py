"""Garden step for the UK historical energy data (NIC / Fouquet)."""

from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Conversion factor from million tonnes of coal to terawatt-hours.
# The physical factor is 6.85 MWh per tonne, and converting million tonnes to TWh leaves it numerically unchanged
# (1 Mt = 1e6 tonnes and 1 MWh = 1e-6 TWh, and those two cancel), so million tonnes * 6.85 = TWh.
# Rather than assume a textbook calorific value, we calibrate this factor from the overlap between this NIC series (in
# tonnes) and the Shift series (in TWh) for UK coal: over 1900-1950 the implied factor is a near-constant ~6.85 MWh per
# tonne (median 6.85, stable to <3%), and it is 6.85 exactly at the 1900 splice year, so the pre-1900 segment joins the
# 1900+ Shift series with no step at the splice. This is consistent with the calorific value implied by the NIC file's
# own "Units" sheet (1 tonne of coal = 0.588 tonnes of oil equivalent ~ 6.84 MWh).
MT_TO_TWH = 6.85


def sanity_check_inputs(tb: Table) -> None:
    assert not tb["year"].duplicated().any(), "Duplicate years in NIC coal data."
    assert tb["year"].min() == 1700, "NIC coal data no longer starts in 1700."
    assert tb["year"].max() >= 2018, "NIC coal data no longer extends to 2018."
    assert (tb["coal_production_mt"].dropna() >= 0).all(), "Negative coal production found in NIC data."
    # Cross-check a couple of well-known values (production peak in 1913 ~ 292 Mt; ~15 Mt in 1800).
    assert abs(tb.loc[tb["year"] == 1913, "coal_production_mt"].item() - 292) < 2, (
        "1913 UK coal production is not ~292 Mt."
    )
    assert abs(tb.loc[tb["year"] == 1800, "coal_production_mt"].item() - 15) < 2, (
        "1800 UK coal production is not ~15 Mt."
    )


def sanity_check_outputs(tb: Table) -> None:
    assert not tb[["country", "year"]].duplicated().any(), "Duplicate (country, year) rows in output."
    assert (tb["coal_production_twh"].dropna() >= 0).all(), "Negative coal production (TWh) in output."
    assert set(tb["country"]) == {"United Kingdom"}, "Output should only contain the United Kingdom."


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its main table.
    ds_meadow = paths.load_dataset("uk_historical_energy")
    tb = ds_meadow.read("uk_historical_energy")

    #
    # Process data.
    #
    sanity_check_inputs(tb=tb)

    # Drop years with no coal production reported (e.g. the trailing empty rows).
    tb = tb.dropna(subset=["coal_production_mt"]).reset_index(drop=True)

    # Convert coal production from million tonnes to terawatt-hours.
    tb["coal_production_twh"] = tb["coal_production_mt"] * MT_TO_TWH
    tb = tb.drop(columns=["coal_production_mt"])

    # This series corresponds to the United Kingdom.
    tb["country"] = "United Kingdom"

    sanity_check_outputs(tb=tb)

    # Set an appropriate index and sort conveniently.
    tb = tb.format(["country", "year"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb])
    ds_garden.save()
