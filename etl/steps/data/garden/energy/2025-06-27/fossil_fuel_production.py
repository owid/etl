"""Garden step for Fossil fuel production dataset (part of the OWID Energy dataset), based on a combination of the
Energy Institute Statistical Review dataset and Shift data on fossil fuel production.

Coal production is additionally extended before 1900 with two trusted historical sources: Smil (2017) for the World, and
the NIC / Fouquet UK historical energy dataset for the United Kingdom. These only fill years where neither the Energy
Institute nor Shift have data (i.e. before 1900), so the modern series is left unchanged.

"""

import numpy as np
from owid.catalog import Table
from owid.catalog import processing as pr
from owid.datautils import dataframes

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Conversion factors.
# Terawatt-hours to kilowatt-hours.
TWH_TO_KWH = 1e9


def prepare_statistical_review_data(tb_review: Table) -> Table:
    """Prepare Statistical Review data.

    Parameters
    ----------
    tb_review : Table
        Statistical Review data.

    Returns
    -------
    tb_review : Table
        Selected data from the Statistical Review.

    """
    tb_review = tb_review.reset_index()

    columns = {
        "country": "country",
        "year": "year",
        "coal_production_twh": "Coal production (TWh)",
        "gas_production_twh": "Gas production (TWh)",
        "oil_production_twh": "Oil production (TWh)",
    }
    tb_review = tb_review[list(columns)].rename(columns=columns, errors="raise")

    return tb_review


def prepare_shift_data(tb_shift: Table) -> Table:
    """Prepare Shift data.

    Parameters
    ----------
    tb_shift : Table
        Shift data.

    Returns
    -------
    shift_table : Table
        Selected data from Shift.

    """
    tb_shift = tb_shift.reset_index()

    columns = {
        "country": "country",
        "year": "year",
        "coal": "Coal production (TWh)",
        "gas": "Gas production (TWh)",
        "oil": "Oil production (TWh)",
    }
    tb_shift = tb_shift[list(columns)].rename(columns=columns, errors="raise")

    return tb_shift


def prepare_smil_data(tb_smil: Table) -> Table:
    """Prepare Smil data (used to extend World coal production before 1900).

    Parameters
    ----------
    tb_smil : Table
        Smil data.

    Returns
    -------
    tb_smil : Table
        Selected World coal production data from Smil.

    """
    columns = {
        "country": "country",
        "year": "year",
        "coal__twh_direct_energy": "Coal production (TWh)",
    }
    tb_smil = tb_smil[list(columns)].rename(columns=columns, errors="raise")

    # Keep only rows with coal data (Smil coal production is only given for the World).
    tb_smil = tb_smil.dropna(subset=["Coal production (TWh)"]).reset_index(drop=True)

    return tb_smil


def prepare_uk_historical_data(tb_uk: Table) -> Table:
    """Prepare UK historical energy data (used to extend UK coal production before 1900).

    Parameters
    ----------
    tb_uk : Table
        UK historical energy data (NIC / Fouquet).

    Returns
    -------
    tb_uk : Table
        Selected UK coal production data.

    """
    columns = {
        "country": "country",
        "year": "year",
        "coal_production_twh": "Coal production (TWh)",
    }
    tb_uk = tb_uk[list(columns)].rename(columns=columns, errors="raise")

    return tb_uk


def combine_data(tb_review: Table, tb_shift: Table, tb_historical: Table) -> Table:
    """Combine Statistical Review, Shift, and historical coal data.

    Data sources are combined by priority: Energy Institute Statistical Review (from 1965) is prioritized, then Shift
    (from 1900), and finally the historical coal data (Smil for the World and NIC / Fouquet for the UK, before 1900).
    Since the historical data only fills years where neither the Energy Institute nor Shift report values, the modern
    (1900+) series is left unchanged, and only the pre-1900 coal tail is added.

    Parameters
    ----------
    tb_review : Table
        Processed Statistical Review table.
    tb_shift : Table
        Processed Shift table.
    tb_historical : Table
        Processed historical coal table (Smil for the World and NIC / Fouquet for the UK).

    Returns
    -------
    combined : Table
        Combined data.

    """
    # Check that there are no duplicated rows in any of the input datasets.
    assert tb_review[tb_review.duplicated(subset=["country", "year"])].empty, "Duplicated rows in Statistical Review."
    assert tb_shift[tb_shift.duplicated(subset=["country", "year"])].empty, "Duplicated rows in Shift data."
    assert tb_historical[tb_historical.duplicated(subset=["country", "year"])].empty, (
        "Duplicated rows in historical coal data."
    )

    # Combine Shift data (which goes further back in the past) with Statistical Review data (which is more up-to-date).
    # On coincident rows, prioritize Statistical Review data.
    index_columns = ["country", "year"]
    combined = dataframes.combine_two_overlapping_dataframes(df1=tb_review, df2=tb_shift, index_columns=index_columns)

    # Extend coal production before 1900 with the historical data, giving it the lowest priority (so it only fills years
    # where neither the Energy Institute nor Shift report data).
    combined = dataframes.combine_two_overlapping_dataframes(
        df1=combined, df2=tb_historical, index_columns=index_columns
    )

    # Remove rows that only have nan.
    combined = combined.dropna(subset=combined.drop(columns=["country", "year"]).columns, how="all")

    # Sort data appropriately.
    combined = combined.sort_values(index_columns).reset_index(drop=True)

    return combined


def add_annual_change(tb: Table) -> Table:
    """Add annual change variables to combined Statistical Review and Shift data.

    Parameters
    ----------
    tb : Table
        Combined Statistical Review and Shift data.

    Returns
    -------
    combined : Table
        Combined data after adding annual change variables.

    """
    combined = tb.copy()

    # Calculate annual change.
    combined = combined.sort_values(["country", "year"]).reset_index(drop=True)
    # Only consider changes between consecutive years as annual changes. Some historical series are sparse (e.g. the
    # World coal series is decadal before 1900), and a naive row-to-row change there would be a multi-year change
    # mislabeled as an annual (previous-year) change, so we blank those out.
    is_consecutive = combined.groupby("country", observed=True)["year"].diff() == 1
    for cat in ("Coal", "Oil", "Gas"):
        pct_change = (
            combined.groupby("country", observed=True)[f"{cat} production (TWh)"].pct_change(fill_method=None) * 100
        )
        abs_change = combined.groupby("country", observed=True)[f"{cat} production (TWh)"].diff()
        combined[f"Annual change in {cat.lower()} production (%)"] = pct_change.where(is_consecutive)
        combined[f"Annual change in {cat.lower()} production (TWh)"] = abs_change.where(is_consecutive)

    return combined


def add_per_capita_variables(tb: Table) -> Table:
    """Add per-capita variables to combined Statistical Review and Shift data.

    Parameters
    ----------
    tb : Table
        Combined Statistical Review and Shift data.
    ds_population : Dataset
        Population dataset.

    Returns
    -------
    combined : Table
        Combined data after adding per-capita variables.

    """
    tb = tb.copy()

    # List countries for which we expect to have no population.
    # These are countries and regions defined by the Energy Institute and Shift.
    expected_countries_without_population = [
        country for country in tb["country"].unique() if (("(EI)" in country) or ("(Shift)" in country))
    ]
    # Add population to data.
    combined = paths.regions.add_population(
        tb=tb,
        warn_on_missing_countries=True,
        interpolate_missing_population=True,
        expected_countries_without_population=expected_countries_without_population,
    )

    # Calculate production per capita.
    for cat in ("Coal", "Oil", "Gas"):
        combined[f"{cat} production per capita (kWh)"] = (
            combined[f"{cat} production (TWh)"] / combined["population"] * TWH_TO_KWH
        )
    combined = combined.drop(errors="raise", columns=["population"])

    return combined


def remove_spurious_values(tb: Table) -> Table:
    """Remove spurious infinity values.

    These values are generated when calculating the annual change of a variable that is zero or nan the previous year.

    Parameters
    ----------
    tb : Table
        Data that may contain infinity values.

    Returns
    -------
    tb : Table
        Corrected data.

    """
    # Replace any infinity value by nan.
    tb = tb.replace([np.inf, -np.inf], np.nan)

    # Remove rows that only have nan.
    tb = tb.dropna(subset=tb.drop(columns=["country", "year"]).columns, how="all").reset_index(drop=True)

    return tb


def sanity_check_inputs(tb_review: Table, tb_shift: Table, tb_historical: Table) -> None:
    # No duplicate (country, year) rows in the historical coal data.
    assert not tb_historical.duplicated(subset=["country", "year"]).any(), (
        "Duplicate (country, year) rows in historical coal data."
    )

    # Historical coal production is non-negative.
    assert (tb_historical["Coal production (TWh)"].dropna() >= 0).all(), "Negative historical coal production found."

    # Expected historical coverage: World from 1800 (Smil), United Kingdom from 1700 (NIC / Fouquet).
    error = "Historical coal coverage has changed."
    assert tb_historical[tb_historical["country"] == "World"]["year"].min() == 1800, error
    assert tb_historical[tb_historical["country"] == "United Kingdom"]["year"].min() == 1700, error

    # The historical sources should agree with Shift where they overlap (1900), so the pre-1900 tail joins the modern
    # series with no step at the splice.
    for country in ["World", "United Kingdom"]:
        hist = tb_historical[(tb_historical["country"] == country) & (tb_historical["year"] == 1900)][
            "Coal production (TWh)"
        ]
        shift = tb_shift[(tb_shift["country"] == country) & (tb_shift["year"] == 1900)]["Coal production (TWh)"]
        assert not hist.empty and not shift.empty, (
            f"Missing 1900 overlap between historical and Shift data for {country}."
        )
        rel_diff = abs(hist.item() - shift.item()) / shift.item()
        assert rel_diff < 0.15, (
            f"Historical and Shift coal for {country} disagree by {rel_diff:.0%} at the 1900 splice."
        )


def sanity_check_outputs(tb: Table) -> None:
    # No duplicate (country, year) rows.
    assert not tb.duplicated(subset=["country", "year"]).any(), "Duplicate (country, year) rows in output."

    # Production values are non-negative.
    for col in ["Coal production (TWh)", "Oil production (TWh)", "Gas production (TWh)"]:
        assert (tb[col].dropna() >= 0).all(), f"Negative values found in '{col}'."

    # Expected coal coverage after extending with historical data: World from 1800, United Kingdom from 1700.
    coal = tb.dropna(subset=["Coal production (TWh)"])
    error = "Coal coverage after extending with historical data has changed."
    assert coal[coal["country"] == "World"]["year"].min() == 1800, error
    assert coal[coal["country"] == "United Kingdom"]["year"].min() == 1700, error

    # Continuity across the pre-1900 -> modern splice for the UK, which has annual coal data around 1900 (the World
    # historical data is decadal before 1900, so there is no 1899 value to compare; its splice quality is checked at the
    # 1900 overlap in sanity_check_inputs instead).
    uk_coal = coal[coal["country"] == "United Kingdom"].set_index("year")["Coal production (TWh)"]
    rel_diff = abs(uk_coal.loc[1900] - uk_coal.loc[1899]) / uk_coal.loc[1900]
    assert rel_diff < 0.10, f"Discontinuity in UK coal production at the 1899->1900 splice ({rel_diff:.0%})."


def run() -> None:
    #
    # Load data.
    #
    # Load Statistical Review dataset and read its main table.
    ds_review = paths.load_dataset("statistical_review_of_world_energy")
    tb_review = ds_review.read("statistical_review_of_world_energy", reset_index=False)

    # Load Shift dataset and read its main table.
    ds_shift = paths.load_dataset("energy_production_from_fossil_fuels")
    tb_shift = ds_shift.read("energy_production_from_fossil_fuels", reset_index=False)

    # Load Smil dataset and read its main table (used to extend World coal production before 1900).
    ds_smil = paths.load_dataset("smil_2017")
    tb_smil = ds_smil.read("smil_2017")

    # Load UK historical energy dataset and read its main table (used to extend UK coal production before 1900).
    ds_uk = paths.load_dataset("uk_historical_energy")
    tb_uk = ds_uk.read("uk_historical_energy")

    #
    # Process data.
    #
    # Prepare Statistical Review data.
    tb_review = prepare_statistical_review_data(tb_review=tb_review)

    # Prepare Shift data on fossil fuel production.
    tb_shift = prepare_shift_data(tb_shift=tb_shift)

    # Prepare historical coal data (Smil for the World and NIC / Fouquet for the UK) and combine into one table.
    tb_smil = prepare_smil_data(tb_smil=tb_smil)
    tb_uk = prepare_uk_historical_data(tb_uk=tb_uk)
    tb_historical = pr.concat([tb_smil, tb_uk], ignore_index=True)

    # Run sanity checks on inputs (including that historical and Shift data agree at their 1900 overlap).
    sanity_check_inputs(tb_review=tb_review, tb_shift=tb_shift, tb_historical=tb_historical)

    # The historical sources extend beyond 1900 (Smil to 1960, NIC to 2018), but they are only meant to fill the
    # pre-1900 tail. Restrict them to before 1900 so they can never backfill a later gap in the Energy Institute or
    # Shift data.
    tb_historical = tb_historical[tb_historical["year"] < 1900].reset_index(drop=True)

    # Combine Statistical Review, Shift, and historical coal data.
    tb = combine_data(tb_review=tb_review, tb_shift=tb_shift, tb_historical=tb_historical)

    # Add annual change.
    tb = add_annual_change(tb=tb)

    # Add per-capita variables.
    tb = add_per_capita_variables(tb=tb)

    # Remove spurious values and rows that only have nans.
    tb = remove_spurious_values(tb=tb)

    # Run sanity checks on outputs.
    sanity_check_outputs(tb=tb)

    # Create an appropriate index and sort conveniently.
    tb = tb.format(short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb])
    ds_garden.save()
