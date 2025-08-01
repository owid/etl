"""Grapher step for the primary energy consumption dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load data.
    #
    # Load garden dataset and read its main table.
    ds_garden = paths.load_dataset("primary_energy_consumption")
    tb_garden = ds_garden.read("primary_energy_consumption")

    #
    # Process data.
    #
    # Remove unnecessary columns.
    tb = tb_garden.drop(columns=["gdp", "population", "source"], errors="raise")

    ####################################################################################################################
    # TODO: Remove, temporary code for a specific chart on top energy consumers.

    # The USSR in the Statistical Review ends in 1984, and successors start in 1985. Remove those years of successors.
    # Specifically, remove Russia and Ukraine (the only ones that are relevant for the chart we are working on).
    tb.loc[(tb["year"] < 1992) & (tb["country"].isin(["Russia", "Ukraine"])), "primary_energy_consumption__twh"] = None

    # regions = [
    #     "Africa",
    #     "Africa (EI)",
    #     "Africa (EIA)",
    #     "Asia",
    #     "Asia & Oceania (EIA)",
    #     "Asia Pacific (EI)",
    #     "CIS (EI)",
    #     "Central & South America (EIA)",
    #     "Eastern Europe and Eurasia (EIA)",
    #     "Eurasia (EIA)",
    #     "Europe",
    #     "Europe (EI)",
    #     "Europe (EIA)",
    #     "European Union (27)",
    #     "High-income countries",
    #     "Lower-middle-income countries",
    #     "Middle East (EI)",
    #     "Middle East (EIA)",
    #     "Non-OECD (EI)",
    #     "Non-OECD (EIA)",
    #     "Non-OPEC (EIA)",
    #     "North America",
    #     "North America (EI)",
    #     "OECD (EI)",
    #     "OECD (EIA)",
    #     "OPEC (EIA)",
    #     "Other Americas (EIA)",
    #     "Other Asia-Pacific (EIA)",
    #     "Persian Gulf (EIA)",
    #     "South America",
    #     "South and Central America (EI)",
    #     "Upper-middle-income countries",
    #     "Western Europe (EIA)",
    #     "World",
    # ]
    # # Find out, for each year, the top five countries in terms of primary energy consumption.
    # top_consumers = {
    #     year: tb[(tb["year"] == year) & (~tb["country"].isin(regions))]
    #     .sort_values(tb.columns[2], ascending=False)
    #     .iloc[0:5]["country"]
    #     .tolist()
    #     for year in sorted(set(tb["year"]))
    # }
    # top = sorted(set(sum(top_consumers.values(), [])))

    # for year in sorted(set(tb["year"])):
    #     top_countries = (
    #         tb[(tb["year"] == year) & (~tb["country"].isin(regions))]
    #         .sort_values(tb.columns[2], ascending=False)["country"]
    #         .tolist()
    #     )
    #     assert set(top_countries[0:5]) < set(top)
    #     print(year, f"Top 5: {', '.join(top_countries[0:5])}", f"6th: {top_countries[6]}", f"7th: {top_countries[7]}")

    ####################################################################################################################

    # Format table conveniently.
    tb = tb.format()

    #
    # Save outputs.
    #
    ds_grapher = paths.create_dataset(tables=[tb])
    ds_grapher.save()
