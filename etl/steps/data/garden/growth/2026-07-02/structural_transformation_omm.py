"""Long-run data on employment by broad economic sector (agriculture, industry, services).

Combines three sources:
  - World Bank World Development Indicators (WDI): sector employment shares (ILO-modeled,
    1991 onwards) and total employment.
  - The historical compilation built in structural_transformation_historical (Herrendorf,
    Rogerson and Valentinyi 2014, updated with the GGDC 10-Sector Database), covering ten
    countries since 1800 (Belgium, Finland, France, Japan, the Netherlands, South Korea,
    Spain, Sweden, the United Kingdom and the United States).
  - Broadberry and Gardner (2013): benchmark estimates of the share of the labor force
    employed in agriculture in five European countries (France, Italy, the Netherlands,
    Poland and the United Kingdom), 1300-1800, joined with employment shares in all three
    sectors around 1980 for the same countries from an archived edition of the WDI (the
    World Bank later replaced those series with ILO-modeled estimates that begin in
    1991).

The agriculture employment numbers splice the historical compilation with WDI,
replicating the design of the previously published dataset (see
https://ourworldindata.org/agri-employment-sources): for each country, WDI is used from
its first available year onwards; the compilation only contributes years strictly before
that. This avoids mixing definitions within the modern segment of a series. The years
1986-1990 are excluded from the historical series: the definitional break between the
historical persons-engaged data and the ILO-modeled data from 1991 is large for some
countries, and excluding these years keeps the transition between the two sources
consistent. Employment numbers for industry and services could be built with the same
methodology, but they are not published for now; the compilation's industry and services
numbers remain available in structural_transformation_historical.

The employment shares join the benchmark estimates with WDI from 1991: for agriculture,
Broadberry and Gardner (1300-1800) plus the values around 1980 from the archived WDI
release; for industry and services, the values around 1980 only.

The compilation's value added shares by sector are not published here; they remain
available in structural_transformation_historical.
"""

from owid.catalog import Table
from owid.catalog import processing as pr
from structlog import get_logger

from etl.helpers import PathFinder

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

SECTORS = ["agriculture", "industry", "services"]
SHARE_EMPLOYED_COLUMNS = [f"share_employed_{sector}" for sector in SECTORS]
# Employment numbers are only published for agriculture (see the module docstring).
INDICATOR_COLUMNS = SHARE_EMPLOYED_COLUMNS + ["number_employed_agriculture"]

# WDI indicators used (columns of the wide wdi table, named after the WDI codes).
WDI_COLUMNS = {
    "sl_agr_empl_zs": "share_employed_agriculture",
    "sl_ind_empl_zs": "share_employed_industry",
    "sl_srv_empl_zs": "share_employed_services",
}

# Countries covered by the historical compilation.
COMPILATION_COUNTRIES = [
    "Belgium",
    "Finland",
    "France",
    "Japan",
    "Netherlands",
    "South Korea",
    "Spain",
    "Sweden",
    "United Kingdom",
    "United States",
]

# Threshold for the splice discontinuity report (relative change).
JUMP_THRESHOLD_NUMBERS = 0.3


def run() -> None:
    #
    # Load inputs.
    #
    ds_historical = paths.load_dataset("structural_transformation_historical")
    ds_broadberry = paths.load_dataset("broadberry_gardner")
    ds_archive = paths.load_dataset("wdi_employment_by_sector_archive")
    ds_wdi = paths.load_dataset("wdi")

    tb_historical = ds_historical.read("structural_transformation_historical")
    tb_broadberry = ds_broadberry.read("broadberry_gardner")
    tb_archive = ds_archive.read("wdi_employment_by_sector_archive")
    tb_wdi = ds_wdi.read("wdi", safe_types=False)

    #
    # Process data.
    #
    sanity_check_inputs(tb_historical=tb_historical, tb_broadberry=tb_broadberry, tb_archive=tb_archive, tb_wdi=tb_wdi)

    # Benchmark estimates of the employment shares: Broadberry and Gardner (agriculture,
    # 1300-1800) joined with the three-sector values around 1980 from the archived WDI
    # release. Both are routed through the harmonization mapping for transparency
    # (identity mappings).
    tb_benchmarks = pr.concat([tb_broadberry, tb_archive], ignore_index=True)
    tb_benchmarks = paths.regions.harmonize_names(tb=tb_benchmarks)

    tb_wdi = prepare_wdi(tb_wdi)
    tb_historical = prepare_historical(tb_historical)

    report_splice_discontinuities(tb_wdi=tb_wdi, tb_historical=tb_historical)

    tb = combine_sources(tb_wdi=tb_wdi, tb_historical=tb_historical, tb_benchmarks=tb_benchmarks)

    sanity_check_outputs(tb)

    # Improve table format.
    tb = tb.format(["country", "year"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_historical.metadata)
    ds_garden.save()


def prepare_wdi(tb: Table) -> Table:
    """Select the WDI indicators and derive the number employed in agriculture."""
    tb = tb[["country", "year", "number_employed"] + list(WDI_COLUMNS)].copy()
    tb = tb.rename(columns=WDI_COLUMNS, errors="raise")

    # Number employed in agriculture: the agriculture share of total employment times total
    # employment (derived in the WDI garden step from the ILO-modeled employment-to-population
    # ratio and the UN population aged 15 and over).
    tb["number_employed_agriculture"] = (tb["share_employed_agriculture"] / 100 * tb["number_employed"]).round()
    tb = tb.drop(columns=["number_employed"])

    tb = tb.dropna(subset=INDICATOR_COLUMNS, how="all").reset_index(drop=True)

    return tb


def prepare_historical(tb: Table) -> Table:
    """Keep the compilation's agriculture employment numbers for the years before the WDI era."""
    tb = tb.copy()

    # Exclude 1986-1990 to keep the transition to the ILO-modeled series (from 1991)
    # consistent; see the module docstring.
    tb = tb[~tb["year"].between(1986, 1990)].reset_index(drop=True)

    return tb[["country", "year", "number_employed_agriculture"]]


def combine_sources(tb_wdi: Table, tb_historical: Table, tb_benchmarks: Table) -> Table:
    """Combine the historical sources with WDI, using WDI from its first year per country."""
    tb = pr.merge(tb_wdi, tb_historical, on=["country", "year"], how="outer", suffixes=("", "_hist"))

    # Benchmark estimates: the only pre-WDI values of the employment shares.
    tb_benchmarks = tb_benchmarks.rename(columns={column: f"{column}_hist" for column in SHARE_EMPLOYED_COLUMNS})
    tb = pr.merge(tb, tb_benchmarks, on=["country", "year"], how="outer")

    # Columns with a historical segment; the industry and services employment numbers are
    # computed from WDI only (see the module docstring).
    spliced_columns = SHARE_EMPLOYED_COLUMNS + ["number_employed_agriculture"]
    for column in spliced_columns:
        # First year with WDI data, per country.
        first_wdi_year = tb[tb[column].notna()].groupby("country", observed=True)["year"].min()
        cutoff = tb["country"].map(first_wdi_year)

        # Historical values only strictly before the first WDI year (comparisons with a
        # missing cutoff are False, so countries without WDI data keep all historical years).
        historical = tb[f"{column}_hist"].copy()
        historical[tb["year"] >= cutoff] = float("nan")

        origins = union_origins(tb[column].m.origins, tb[f"{column}_hist"].m.origins)
        tb[column] = tb[column].combine_first(historical)
        tb[column].m.origins = origins

    tb = tb.drop(columns=[f"{column}_hist" for column in spliced_columns])
    tb = tb.dropna(subset=INDICATOR_COLUMNS, how="all").reset_index(drop=True)

    return tb


def union_origins(origins_a: list, origins_b: list) -> list:
    """Union of two lists of origins, preserving order."""
    origins = []
    for origin in list(origins_a) + list(origins_b):
        if origin not in origins:
            origins.append(origin)
    return origins


def report_splice_discontinuities(tb_wdi: Table, tb_historical: Table) -> None:
    """Warn about large jumps between the last historical value and the first WDI value.

    Some jumps are expected: historical employment covers persons engaged while WDI covers
    ILO-modeled employment aged 15 and over. The warnings surface them for review, they
    are not errors.
    """
    column = "number_employed_agriculture"
    for country in tb_historical["country"].unique():
        hist_values = tb_historical[tb_historical["country"] == country].dropna(subset=[column])
        wdi_values = tb_wdi[tb_wdi["country"] == country].dropna(subset=[column])
        if hist_values.empty or wdi_values.empty:
            continue
        first_wdi_year = wdi_values["year"].min()
        hist_before = hist_values[hist_values["year"] < first_wdi_year]
        if hist_before.empty:
            continue
        last_hist_year = hist_before["year"].max()
        wdi_value = wdi_values.loc[wdi_values["year"] == first_wdi_year, column].iloc[0]
        hist_value = hist_before.loc[hist_before["year"] == last_hist_year, column].iloc[0]
        jump = abs(wdi_value - hist_value) / hist_value
        if jump > JUMP_THRESHOLD_NUMBERS:
            log.warning(
                f"Splice discontinuity in {country}, {column}: {hist_value:.0f} ({int(last_hist_year)}) -> "
                f"{wdi_value:.0f} ({int(first_wdi_year)}), jump of {jump:.2f}."
            )


def sanity_check_inputs(tb_historical: Table, tb_broadberry: Table, tb_archive: Table, tb_wdi: Table) -> None:
    error = "Historical compilation does not contain the expected countries."
    assert set(tb_historical["country"]) == set(COMPILATION_COUNTRIES), error

    error = "Broadberry and Gardner data does not contain the expected countries."
    assert set(tb_broadberry["country"]) == {"France", "Italy", "Netherlands", "Poland", "United Kingdom"}, error

    error = "Archived WDI values should cover the same five countries around 1980, for all three sectors."
    assert set(tb_archive["country"]) == set(tb_broadberry["country"]), error
    assert set(tb_archive["year"]) == {1980, 1981}, error
    assert tb_archive[SHARE_EMPLOYED_COLUMNS].notna().all().all(), error

    error = "WDI table is missing expected columns."
    assert set(list(WDI_COLUMNS) + ["number_employed"]) <= set(tb_wdi.columns), error


def sanity_check_outputs(tb: Table) -> None:
    error = "Unexpected columns in the output table."
    assert set(tb.columns) == set(["country", "year"] + INDICATOR_COLUMNS), error

    error = "Duplicate (country, year) rows in the output table."
    assert not tb.duplicated(subset=["country", "year"]).any(), error

    for column in SHARE_EMPLOYED_COLUMNS:
        error = f"{column} has values outside [0, 100]."
        assert tb[column].dropna().between(0, 100.01).all(), error
        error = f"{column} has exact zeros, which would be placeholder values."
        assert (tb[column].dropna() != 0).all(), error

    error = "number_employed_agriculture has negative or zero values."
    assert (tb["number_employed_agriculture"].dropna() > 0).all(), error

    error = "Modern era should cover most countries in the world."
    assert tb[tb["year"] == 2019]["share_employed_agriculture"].notna().sum() > 150, error

    error = "Years 1986-1990 should be excluded (no historical values, and WDI starts in 1991)."
    assert tb[tb["year"].between(1986, 1990)].empty, error

    # No gap between the end of the historical series and the start of the WDI series for
    # the compilation countries: the agriculture employment series should be continuous at
    # the splice.
    for country in COMPILATION_COUNTRIES:
        tb_country = tb[tb["country"] == country]
        years = tb_country.dropna(subset=["number_employed_agriculture"])["year"]
        error = f"Gap around the splice point in {country}, number_employed_agriculture."
        recent = years[years.between(1985, 2000)]
        assert set(range(1991, 2000)) <= set(recent), error

    error = "Benchmark years must survive the splice."
    for country, year in [("Poland", 1500), ("Italy", 1300), ("Poland", 1981), ("Italy", 1980)]:
        assert tb.loc[(tb["country"] == country) & (tb["year"] == year), "share_employed_agriculture"].notna().all(), (
            error
        )

    # The employment shares before 1991 only hold the benchmark estimates: Broadberry and
    # Gardner (29 agriculture values, 1300-1800) plus the archived WDI values around 1980
    # (5 values for each of the three sectors).
    pre_wdi = tb[tb["year"] < 1991]
    error = "Unexpected pre-1991 agriculture employment shares beyond the benchmark estimates."
    assert int(pre_wdi["share_employed_agriculture"].notna().sum()) == 34, error
    error = "Pre-1991 industry and services employment shares should only hold the archived WDI values."
    assert int(pre_wdi["share_employed_industry"].notna().sum()) == 5, error
    assert int(pre_wdi["share_employed_services"].notna().sum()) == 5, error

    # Spot checks on the benchmark estimates.
    value = tb.loc[(tb["country"] == "Poland") & (tb["year"] == 1500), "share_employed_agriculture"].iloc[0]
    error = f"Poland 1500 Broadberry benchmark = {value}, expected 75.3."
    assert abs(value - 75.3) < 0.01, error

    value = tb.loc[(tb["country"] == "Italy") & (tb["year"] == 1980), "share_employed_agriculture"].iloc[0]
    error = f"Italy 1980 archived WDI value = {value}, expected 14.0."
    assert abs(value - 14.0) < 0.01, error

    value = tb.loc[(tb["country"] == "Poland") & (tb["year"] == 1981), "share_employed_industry"].iloc[0]
    error = f"Poland 1981 archived WDI industry value = {value}, expected 38.9."
    assert abs(value - 38.9) < 0.01, error
