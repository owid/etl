import owid.catalog.processing as pr

from etl.catalog_helpers import last_date_accessed
from etl.helpers import PathFinder

paths = PathFinder(__file__)

REGIONS = ["North America", "South America", "Europe", "Africa", "Asia", "Oceania", "World"]
PER_CAPITA_COLS = ["num_patent_applications", "num_patent_granted", "num_articles"]
INVESTMENT_COLS = ["disclosed_investment", "estimated_investment", "estimated_investment_projected"]


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

    tb = adjust_investment_for_inflation(tb, tb_us_cpi)

    tb = paths.regions.add_population(tb=tb)
    for col in PER_CAPITA_COLS:
        tb[f"{col}_per_mil"] = tb[col] / (tb["population"] / 1e6)
    tb = tb.drop(columns=["population"])

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
