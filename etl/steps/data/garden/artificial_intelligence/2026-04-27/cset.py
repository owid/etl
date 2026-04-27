import owid.catalog.processing as pr

from etl.catalog_helpers import last_date_accessed
from etl.helpers import PathFinder

paths = PathFinder(__file__)

REGIONS = ["North America", "South America", "Europe", "Africa", "Asia", "Oceania", "World"]
PER_CAPITA_COLS = ["num_patent_applications", "num_patent_granted", "num_articles"]
INVESTMENT_COLS = ["disclosed_investment", "estimated_investment", "estimated_investment_projected"]
# Variables not used in any chart — dropped to reduce indicator count.
COLS_TO_DROP = ["disclosed_investment", "num_citations", "num_patent_granted_per_mil"]


def run() -> None:
    ds_meadow = paths.load_dataset("cset")
    tb = ds_meadow.read("cset")

    ds_us_cpi = paths.load_dataset("us_consumer_prices")
    tb_us_cpi = ds_us_cpi.read("us_consumer_prices")

    tb = paths.regions.harmonize_names(tb)

    # Include Africa so it contributes to the World aggregate, then clear its
    # investment values — too few countries (<40% of population) to be meaningful.
    tb = paths.regions.add_aggregates(
        tb=tb,
        regions=REGIONS,
        index_columns=["country", "year", "field"],
        min_num_values_per_year=1,
    )
    africa_mask = tb["country"] == "Africa"
    for col in INVESTMENT_COLS:
        if col in tb.columns:
            tb[col] = tb[col].where(~africa_mask)

    # Clear projected values for years where actual data exists, except for the
    # last actual year per country/field which is kept in _projected to connect
    # the actual and projected lines in Grapher charts.
    if "estimated_investment_projected" in tb.columns:
        last_actual_year = (
            tb[tb["estimated_investment"].notna()]
            .groupby(["country", "field"])["year"]
            .max()
            .rename("last_actual_year")
        )
        tb = tb.merge(last_actual_year.reset_index(), on=["country", "field"], how="left")
        is_last_actual = tb["year"] == tb["last_actual_year"]
        has_actual = tb["estimated_investment"].notna()
        tb["estimated_investment_projected"] = tb["estimated_investment_projected"].where(~has_actual | is_last_actual)
        tb = tb.drop(columns=["last_actual_year"])

    tb = adjust_investment_for_inflation(tb, tb_us_cpi)

    tb = paths.regions.add_population(tb=tb)
    for col in PER_CAPITA_COLS:
        tb[f"{col}_per_mil"] = tb[col] / (tb["population"] / 1e6)
    tb = tb.drop(columns=["population"])

    tb = tb.drop(columns=[c for c in COLS_TO_DROP if c in tb.columns])

    tb = tb.format(["country", "year", "field"], short_name=paths.short_name)

    ds_garden = paths.create_dataset(
        tables=[tb],
        default_metadata=ds_meadow.metadata,
        yaml_params={"date_accessed": last_date_accessed(tb), "year": last_date_accessed(tb)[-4:]},
    )
    ds_garden.save()


def adjust_investment_for_inflation(tb, tb_us_cpi):
    """Convert investment columns from nominal millions USD to constant 2021 USD."""
    investment_cols = [col for col in tb.columns if "investment" in col]

    # Scale from millions to dollars
    tb[investment_cols] = tb[investment_cols].astype("float64") * 1e6

    # Build CPI adjustment factor (base year: 2021)
    cpi_2021 = tb_us_cpi.loc[tb_us_cpi["year"] == 2021, "all_items"].values[0]
    cpi_adj = tb_us_cpi[["year"]].assign(cpi_adj_2021=tb_us_cpi["all_items"] / cpi_2021)

    tb = pr.merge(tb, cpi_adj, on="year", how="inner")
    for col in investment_cols:
        tb[col] = (tb[col] / tb["cpi_adj_2021"]).round()
    return tb.drop(columns=["cpi_adj_2021"])
