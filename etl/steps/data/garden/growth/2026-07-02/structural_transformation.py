"""Long-run data on employment and GDP by broad economic sector (agriculture, industry, services).

Combines three sources:
  - World Bank World Development Indicators (WDI): sector employment shares (ILO-modeled,
    1991 onwards), sector value added as a share of GDP (1960 onwards), and total employment.
  - The historical compilation built in structural_transformation_historical (Herrendorf,
    Rogerson and Valentinyi 2014, updated with the GGDC 10-Sector Database and the Swedish
    Historical National Accounts), covering ten currently rich countries since 1800.
  - Broadberry and Gardner (2013): benchmark estimates of the share of the labor force
    employed in agriculture in five European countries, 1300-1981.

Splice rule: for each country and indicator, WDI is used from its first available year
onwards; historical sources only contribute years strictly before that. This avoids mixing
definitions within the modern segment of a series. Precedence among historical sources for
employment shares: shares derived from the compilation's employment numbers first, then the
Broadberry and Gardner benchmarks.
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
NUMBER_EMPLOYED_COLUMNS = [f"number_employed_{sector}" for sector in SECTORS]
SHARE_GDP_COLUMNS = [f"share_gdp_{sector}" for sector in SECTORS]
INDICATOR_COLUMNS = SHARE_EMPLOYED_COLUMNS + NUMBER_EMPLOYED_COLUMNS + SHARE_GDP_COLUMNS

# WDI indicators used (columns of the wide wdi table, named after the WDI codes).
WDI_COLUMNS = {
    "sl_agr_empl_zs": "share_employed_agriculture",
    "sl_ind_empl_zs": "share_employed_industry",
    "sl_srv_empl_zs": "share_employed_services",
    "nv_agr_totl_zs": "share_gdp_agriculture",
    "nv_ind_totl_zs": "share_gdp_industry",
    "nv_srv_totl_zs": "share_gdp_services",
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

# Thresholds for the splice discontinuity report.
JUMP_THRESHOLD_SHARES = 15  # percentage points
JUMP_THRESHOLD_NUMBERS = 0.3  # relative change


def run() -> None:
    #
    # Load inputs.
    #
    ds_historical = paths.load_dataset("structural_transformation_historical")
    ds_broadberry = paths.load_dataset("broadberry_gardner")
    ds_wdi = paths.load_dataset("wdi")

    tb_historical = ds_historical.read("structural_transformation_historical")
    tb_broadberry = ds_broadberry.read("broadberry_gardner")
    tb_wdi = ds_wdi.read("wdi", safe_types=False)

    #
    # Process data.
    #
    sanity_check_inputs(tb_historical=tb_historical, tb_broadberry=tb_broadberry, tb_wdi=tb_wdi)

    tb_wdi = prepare_wdi(tb_wdi)
    tb_historical = prepare_historical(tb_historical)

    report_splice_discontinuities(tb_wdi=tb_wdi, tb_historical=tb_historical)

    tb = combine_sources(tb_wdi=tb_wdi, tb_historical=tb_historical, tb_broadberry=tb_broadberry)

    sanity_check_outputs(tb)

    # Improve table format.
    tb = tb.format(["country", "year"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_historical.metadata)
    ds_garden.save()


def prepare_wdi(tb: Table) -> Table:
    """Select the WDI indicators and derive the number employed by sector."""
    tb = tb[["country", "year", "number_employed"] + list(WDI_COLUMNS)].copy()
    tb = tb.rename(columns=WDI_COLUMNS, errors="raise")

    # NOTE: WDI carries placeholder zeros in the sector value added shares of Brazil in
    # 1960-1980. A zero share of GDP is not plausible for any of these broad sectors, so
    # rows with a zero share are treated as missing altogether (the non-zero values in
    # those rows are placeholders too, e.g. identical industry and services shares).
    placeholder = (tb[SHARE_GDP_COLUMNS] == 0).any(axis=1)
    tb.loc[placeholder, SHARE_GDP_COLUMNS] = float("nan")

    # Number employed by sector: sector share of total employment times total employment
    # (derived in the WDI garden step from the ILO-modeled employment-to-population ratio
    # and the UN population aged 15 and over).
    for sector in SECTORS:
        tb[f"number_employed_{sector}"] = (tb[f"share_employed_{sector}"] / 100 * tb["number_employed"]).round()
    tb = tb.drop(columns=["number_employed"])

    tb = tb.dropna(subset=INDICATOR_COLUMNS, how="all").reset_index(drop=True)

    return tb


def prepare_historical(tb: Table) -> Table:
    """Derive employment shares from the compilation's employment numbers."""
    tb = tb.copy()

    number_employed_total = tb[NUMBER_EMPLOYED_COLUMNS].sum(axis=1, min_count=len(NUMBER_EMPLOYED_COLUMNS))
    for sector in SECTORS:
        tb[f"share_employed_{sector}"] = tb[f"number_employed_{sector}"] / number_employed_total * 100

    return tb[["country", "year"] + INDICATOR_COLUMNS]


def combine_sources(tb_wdi: Table, tb_historical: Table, tb_broadberry: Table) -> Table:
    """Combine WDI with the historical sources, using WDI from its first year per country."""
    tb = pr.merge(tb_wdi, tb_historical, on=["country", "year"], how="outer", suffixes=("", "_hist"))

    # Broadberry and Gardner benchmarks: lowest precedence for the agriculture employment share.
    tb_broadberry = tb_broadberry.rename(columns={"share_employed_agriculture": "share_employed_agriculture_bg"})
    tb = pr.merge(tb, tb_broadberry, on=["country", "year"], how="outer")
    origins_bg = tb["share_employed_agriculture_bg"].m.origins
    tb["share_employed_agriculture_hist"] = tb["share_employed_agriculture_hist"].combine_first(
        tb["share_employed_agriculture_bg"]
    )
    tb["share_employed_agriculture_hist"].m.origins = union_origins(
        tb["share_employed_agriculture_hist"].m.origins, origins_bg
    )
    tb = tb.drop(columns=["share_employed_agriculture_bg"])

    for column in INDICATOR_COLUMNS:
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

    tb = tb.drop(columns=[f"{column}_hist" for column in INDICATOR_COLUMNS])
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

    Some jumps are expected: utilities move from services (historical convention) to
    industry (WDI/ISIC) at the splice, and historical employment covers persons engaged
    while WDI covers ILO-modeled employment aged 15 and over. The warnings surface them
    for review, they are not errors.
    """
    for country in tb_historical["country"].unique():
        tb_hist_country = tb_historical[tb_historical["country"] == country]
        tb_wdi_country = tb_wdi[tb_wdi["country"] == country]
        for column in INDICATOR_COLUMNS:
            hist_values = tb_hist_country.dropna(subset=[column])
            wdi_values = tb_wdi_country.dropna(subset=[column])
            if hist_values.empty or wdi_values.empty:
                continue
            first_wdi_year = wdi_values["year"].min()
            hist_before = hist_values[hist_values["year"] < first_wdi_year]
            if hist_before.empty:
                continue
            last_hist_year = hist_before["year"].max()
            wdi_value = wdi_values.loc[wdi_values["year"] == first_wdi_year, column].iloc[0]
            hist_value = hist_before.loc[hist_before["year"] == last_hist_year, column].iloc[0]
            if column in NUMBER_EMPLOYED_COLUMNS:
                jump = abs(wdi_value - hist_value) / hist_value
                threshold = JUMP_THRESHOLD_NUMBERS
                unit = ""
            else:
                jump = abs(wdi_value - hist_value)
                threshold = JUMP_THRESHOLD_SHARES
                unit = " pp"
            if jump > threshold:
                log.warning(
                    f"Splice discontinuity in {country}, {column}: {hist_value:.0f} ({int(last_hist_year)}) -> "
                    f"{wdi_value:.0f} ({int(first_wdi_year)}), jump of {jump:.2f}{unit}."
                )


def sanity_check_inputs(tb_historical: Table, tb_broadberry: Table, tb_wdi: Table) -> None:
    error = "Historical compilation does not contain the expected countries."
    assert set(tb_historical["country"]) == set(COMPILATION_COUNTRIES), error

    error = "Broadberry and Gardner data does not contain the expected countries."
    assert set(tb_broadberry["country"]) == {"France", "Italy", "Netherlands", "Poland", "United Kingdom"}, error

    error = "WDI table is missing expected columns."
    assert set(list(WDI_COLUMNS) + ["number_employed"]) <= set(tb_wdi.columns), error


def sanity_check_outputs(tb: Table) -> None:
    error = "Unexpected columns in the output table."
    assert set(tb.columns) == set(["country", "year"] + INDICATOR_COLUMNS), error

    error = "Duplicate (country, year) rows in the output table."
    assert not tb.duplicated(subset=["country", "year"]).any(), error

    for column in SHARE_EMPLOYED_COLUMNS + SHARE_GDP_COLUMNS:
        error = f"{column} has values outside [0, 100]."
        assert tb[column].dropna().between(0, 100.01).all(), error
        error = f"{column} has exact zeros, which are placeholder values in the source."
        assert (tb[column].dropna() != 0).all(), error

    for column in NUMBER_EMPLOYED_COLUMNS:
        error = f"{column} has negative or zero values."
        assert (tb[column].dropna() > 0).all(), error

    error = "Modern era should cover most countries in the world."
    assert tb[tb["year"] == 2019]["share_employed_agriculture"].notna().sum() > 150, error

    # No gap between the end of the historical series and the start of the WDI series for
    # the compilation countries: each series should be continuous at the splice point.
    for country in COMPILATION_COUNTRIES:
        tb_country = tb[tb["country"] == country]
        for column in INDICATOR_COLUMNS:
            years = tb_country.dropna(subset=[column])["year"]
            error = f"Gap around the splice point in {country}, {column}."
            recent = years[years.between(1985, 2000)]
            assert set(range(1991, 2000)) <= set(recent) or column in SHARE_GDP_COLUMNS, error

    error = "Broadberry and Gardner benchmark years must survive the splice."
    for country, year in [("Poland", 1500), ("Italy", 1300), ("Poland", 1981)]:
        assert tb.loc[(tb["country"] == country) & (tb["year"] == year), "share_employed_agriculture"].notna().all(), (
            error
        )

    # Spot checks: derived historical share and a Broadberry benchmark.
    value = tb.loc[(tb["country"] == "United Kingdom") & (tb["year"] == 1801), "share_employed_agriculture"].iloc[0]
    error = f"UK 1801 derived employment share in agriculture = {value}, expected ~30.8."
    assert abs(value - 30.8) < 0.5, error

    value = tb.loc[(tb["country"] == "Poland") & (tb["year"] == 1500), "share_employed_agriculture"].iloc[0]
    error = f"Poland 1500 Broadberry benchmark = {value}, expected 75.3."
    assert abs(value - 75.3) < 0.01, error
