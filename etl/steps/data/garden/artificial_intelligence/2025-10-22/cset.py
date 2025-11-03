import owid.catalog.processing as pr
from owid.catalog import Dataset, Table

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
# Regions for which aggregates will be created.
REGIONS = ["North America", "South America", "Europe", "Africa", "Asia", "Oceania", "World"]


def run() -> None:
    #
    # Load inputs.
    #

    ds_meadow = paths.load_dataset("cset")
    tb = ds_meadow.read("cset")

    ds_us_cpi = paths.load_dataset("us_consumer_prices")
    tb_us_cpi = ds_us_cpi.read("us_consumer_prices")

    ds_population = paths.load_dataset("population")
    tb_population = ds_population.read("population")

    ds_regions = paths.load_dataset("regions")
    #
    # Process data.
    #
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    tb = add_regions(tb, ds_regions)

    # Adjust investment columns for inflation using the US Consumer Price Index (CPI)
    _investment_cols = [col for col in tb.columns if "investment" in col]
    tb[_investment_cols] = tb[_investment_cols].astype("float64")
    tb.loc[:, _investment_cols] *= 1e6

    cpi_2021 = tb_us_cpi.loc[tb_us_cpi["year"] == 2021, "all_items"].values[0]
    tb_us_cpi["cpi_adj_2021"] = tb_us_cpi["all_items"] / cpi_2021
    tb_us_cpi_2021 = tb_us_cpi[["cpi_adj_2021", "year"]].copy()
    tb_cpi_inv = pr.merge(tb, tb_us_cpi_2021, on="year", how="inner")

    for col in _investment_cols:
        tb_cpi_inv[col] = round(tb_cpi_inv[col] / tb_cpi_inv["cpi_adj_2021"])

    tb_cpi_inv = tb_cpi_inv.drop("cpi_adj_2021", axis=1)

    # Calculate the number of patent applications, patents granted, and articles per million people
    tb = pr.merge(
        tb_cpi_inv,
        tb_population[["country", "year", "population"]].astype({"population": "float64"}),
        how="left",
        on=["country", "year"],
    )

    for col in ["num_patent_applications", "num_patent_granted", "num_articles"]:
        tb[f"{col}_per_mil"] = tb[col] / (tb["population"] / 1e6)

    tb = tb.drop("population", axis=1)

    # Set values to NaN for the specified regions and columns
    # tb.loc[tb["country"].isin(regional_aggregates), columns_to_nan] = np.nan
    tb = tb.format(["country", "year", "field"])
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata)

    ds_garden.save()


def add_regions(tb: Table, ds_regions: Dataset) -> Table:
    # Add region aggregates.
    tb = geo.add_regions_to_table(
        tb,
        regions=REGIONS,
        index_columns=["country", "year", "field"],
        ds_regions=ds_regions,
        frac_allowed_nans_per_year=0.85,
    )

    return tb
