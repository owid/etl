"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Reference year to consider when computing percentage changes.
REFERENCE_YEAR = 2010


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset and read its main table.
    ds_garden = paths.load_dataset("gdp_and_co2_decoupling")
    tb_garden = ds_garden["gdp_and_co2_decoupling"].reset_index()

    #
    # Process data.
    #
    # Select percentage changes only from a specific year.
    tb = tb_garden[tb_garden["year"] == REFERENCE_YEAR].reset_index(drop=True)

    # Starting in 2000, 38 countries increased their per capita GDP (with a global increase of 45.8%) while decreasing their per capita consumption emissions (with a global **increase** of 8.6%).
    # Starting in 2005, 45 countries increased their per capita GDP (with a global increase of 29.8%) while decreasing their per capita consumption emissions (with a global decrease of 0.4%).
    # Starting in 2010, 50 countries increased their per capita GDP (with a global increase of 16.5%) while decreasing their per capita consumption emissions (with a global decrease of 5.8%).

    # Set an appropriate index and sort conveniently.
    tb = tb.set_index(["country", "year"]).sort_index()

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir, tables=[tb], default_metadata=ds_garden.metadata, check_variables_metadata=True
    )
    ds_grapher.save()


# To quickly inspect the decoupling of GDP per capita vs consumption-based emissions per capita, use this function.
# def plot_decoupling(tb, countries=None):
#     import plotly.express as px
#     import owid.catalog.processing as pr
#     from tqdm.auto import tqdm
#     _tb = tb.copy().astype({"country": str})
#     column = "gdp_per_capita_change"
#     emissions_column = "consumption_emissions_per_capita_change"
#     if countries is None:
#         countries = sorted(set(_tb["country"]))
#     for country in tqdm(countries):
#         tb_old = _tb[_tb["country"] == country][["country", "year", column, emissions_column]].reset_index(drop=True)
#         if (tb_old[emissions_column].isna().all()) or (tb_old[column].isna().all()):
#             continue
#         title = tb_old[column].metadata.title or column
#         tb_new = tb_old.copy()
#         tb_new["year"] = 2020
#         tb_old[column] = 0
#         tb_old[emissions_column] = 0
#         tb_plot = pr.concat([tb_old, tb_new], ignore_index=True)
#         tb_plot = tb_plot.melt(id_vars=["country", "year"], var_name="Indicator")
#         plot = px.line(tb_plot, x="year", y="value", color="Indicator", title=f"{country} - {title}")
#         plot.show()
