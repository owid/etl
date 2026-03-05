"""Load a snapshot and create a meadow dataset."""

import numpy as np
import owid.catalog.processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Sheets and columns to extract.
SHEETS_AND_COLUMNS = {
    "SOCIOECONOMIC": [
        "SC_CON_DSF_",  # Average download speed
    ],
    "SDG": ["SD_POP_HGR_"],  # Share of population living in the high green area
    "INFRASTRUCTURES": [
        "IN_ROA_DEN_",  # Road network density
        "IN_ROA_LEN_",  # Road length
    ],
    "HEALTH": [
        "HL_SHP_HOS_",  # Share of population with access to hospitals
        "HL_SHP_PHA_",  # Share of population with access to pharmacies
        "HL_FCL_HOS_",  # Number of hospitals
        "HL_FCL_PHA_",  # Number of pharmacies
    ],
}

# Column and indicator mapping.
COLUMN_MAPPING = {
    "SC_CON_DSF_": "Average download speed",
    "SD_POP_HGR_": "Share of population living in the high green area",
    "IN_ROA_DEN_": "Road network density",
    "IN_ROA_LEN_": "Road length",
    "HL_SHP_HOS_": "Share of population with access to hospitals",
    "HL_SHP_PHA_": "Share of population with access to pharmacies",
    "HL_FCL_HOS_": "Number of hospitals",
    "HL_FCL_PHA_": "Number of pharmacies",
}

# Indicators that are raw counts — converted to per 100,000 at city level before aggregation.
COUNT_INDICATORS = {"Number of hospitals", "Number of pharmacies"}

# Indicators that are raw totals — converted to per-person at city level before aggregation.
LENGTH_INDICATORS = {"Road length"}


def run() -> None:
    #
    # Load inputs.
    #
    snap = paths.load_snapshot("ghsl_stats_in_the_city.zip")

    #
    # Process data.
    #
    extracted_data = []
    city_country = ["GC_UCN_MAI_2025", "GC_CNT_GAD_2025", "ID_UC_G0"]

    with snap.extracted() as archive:
        # Load population data once — needed for weighting in every sheet.
        tb_ghsl = archive.read("GHS_UCDB_GLOBE_R2024A.xlsx", sheet_name="GHSL")
        pop_columns = [col for col in tb_ghsl.columns if col.startswith("GH_POP_TOT_")]
        tb_population = tb_ghsl[city_country + pop_columns].copy()

        # GHSL population is only available at 5-year intervals. Build a lookup so
        # each indicator year can be snapped to the nearest available population year.
        pop_years = np.array(sorted(int(c[-4:]) for c in pop_columns))

        def nearest_pop_year(year: str) -> str:
            return str(pop_years[np.argmin(np.abs(pop_years - int(year)))])

        # Melt population to long format once, outside the sheet loop.
        tb_pop_melted = tb_population.melt(
            id_vars=city_country,
            value_vars=pop_columns,
            var_name="pop_col",
            value_name="population",
        )
        tb_pop_melted["year"] = tb_pop_melted["pop_col"].str[-4:]
        tb_pop_melted = tb_pop_melted.rename(columns={"GC_UCN_MAI_2025": "city", "GC_CNT_GAD_2025": "country"}).drop(
            columns=["pop_col"]
        )

        for sheet, column_prefixes in SHEETS_AND_COLUMNS.items():
            tb = archive.read("GHS_UCDB_GLOBE_R2024A.xlsx", sheet_name=sheet)

            # Select indicator columns plus city identifiers.
            selected_columns = [col for col in tb.columns if any(col.startswith(p) for p in column_prefixes)]
            selected_columns += [col for col in city_country if col in tb.columns]
            tb = tb[selected_columns]

            tb = pr.merge(tb, tb_population, on=city_country, how="inner")
            tb = tb.rename(columns={"GC_UCN_MAI_2025": "city", "GC_CNT_GAD_2025": "country"})

            # Melt indicator columns to long format.
            value_vars = [col for col in tb.columns if any(col.startswith(p) for p in COLUMN_MAPPING)]
            id_vars = ["city", "country", "ID_UC_G0"] + pop_columns
            tb = tb.melt(id_vars=id_vars, value_vars=value_vars, var_name="indicator", value_name="value")

            # Clean values and extract year from column name suffix.
            tb["value"] = tb["value"].replace("-", np.nan)
            tb["value"] = pr.to_numeric(tb["value"], errors="coerce")
            tb["year"] = tb["indicator"].str[-4:]
            tb["indicator"] = tb["indicator"].str[:-4].map(COLUMN_MAPPING)

            # Snap each indicator year to the nearest available population year,
            # then merge. This handles annual indicators (e.g. download speed 2020–2023)
            # and single-year snapshots (e.g. road data at 2024) that don't align
            # exactly with the 5-year population series.
            tb["pop_year"] = tb["year"].map(nearest_pop_year)
            tb = tb.drop(columns=pop_columns)
            tb = pr.merge(
                tb,
                tb_pop_melted,
                left_on=["city", "country", "ID_UC_G0", "pop_year"],
                right_on=["city", "country", "ID_UC_G0", "year"],
                how="left",
                suffixes=("", "_pop"),
            ).drop(columns=["year_pop", "pop_year"])

            tb = tb.drop(columns=["ID_UC_G0"])
            extracted_data.append(tb)

    tb = pr.concat(extracted_data)
    tb["value"].metadata.origins = [snap.m.origin]

    #
    # Aggregate to country level — all indicators use population weighting.
    #
    # The strategy is to express every indicator as a rate or mean at city level
    # before aggregating, so that a single weighted-mean formula handles all cases:
    #
    #   country value = Σ(city value × city population) / Σ(city population)
    #
    # For raw counts (hospitals, pharmacies): divide by city population × 100,000
    #   → weighted mean of per-100k rates = Σ(count) / Σ(pop) × 100,000  ✓
    #
    # For road length (total metres): divide by city population
    #   → weighted mean of metres-per-person = Σ(length) / Σ(pop)  ✓
    #
    # For everything else (shares, speeds, densities): already a rate, no conversion needed.
    #

    # Convert raw counts to per-100,000 at city level.
    mask_count = tb["indicator"].isin(COUNT_INDICATORS)
    tb.loc[mask_count, "value"] = tb.loc[mask_count, "value"] / tb.loc[mask_count, "population"] * 100_000

    # Convert road length to metres per person at city level.
    mask_length = tb["indicator"].isin(LENGTH_INDICATORS)
    tb.loc[mask_length, "value"] = tb.loc[mask_length, "value"] / tb.loc[mask_length, "population"]
    tb.loc[mask_length, "indicator"] = "Road length per inhabitant"

    # Population-weighted mean for all indicators.
    tb["weighted_value"] = tb["value"] * tb["population"]
    tb_agg = tb.groupby(["country", "year", "indicator"], as_index=False).agg(
        weighted_value=("weighted_value", "sum"),
        population=("population", "sum"),
    )
    tb_agg["value"] = tb_agg["weighted_value"] / tb_agg["population"]
    tb = tb_agg[["country", "year", "indicator", "value"]]

    # Improve table format.
    tb = tb.format(["country", "year", "indicator"])

    #
    # Save outputs.
    #
    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)
    ds_meadow.save()
