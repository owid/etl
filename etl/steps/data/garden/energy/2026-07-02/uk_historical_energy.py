"""Garden step for the UK historical energy data (NIC / Fouquet)."""

from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Conversion factor from million tonnes of coal to terawatt-hours, taken entirely from the coal energy content stated
# in the NIC file's own "Units" sheet: 1 tonne of coal = 0.588 tonnes of oil equivalent (toe), 1 toe = 41,870,000 kJ,
# and 1 kWh = 3,600 kJ. So one tonne of coal = 0.588 * 41,870,000 / 3,600 kWh ~ 6.84 MWh. Converting million tonnes to
# TWh leaves the number unchanged (1 Mt = 1e6 tonnes and 1 MWh = 1e-6 TWh cancel out), so: TWh = million tonnes * 6.84.
MT_TO_TWH = 0.588 * 41_870_000 / 3_600 / 1_000  # tonnes of coal -> MWh, i.e. TWh per million tonnes (~6.84)


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
