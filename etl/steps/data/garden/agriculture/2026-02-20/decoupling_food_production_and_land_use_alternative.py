"""Create a dataset with indicators on food supply (in kilocalories) and agricultural land use (in hectares).

TODO: Check if there's an update on the relevant indicators in FAOSTAT.

The goal is to create a visualization showing that some countries managed to feed more people with less land.
In other words, we need to select countries that have managed to decouple per capita food supply and land use.

The main issue is that food supply includes imported food that was produced in land of a different country.
So we need to find a way to avoid showing decoupled countries that simply offshored their land use.

We propose the following approach:
- We use 3-year rollowing averages, to avoid variability at the year level (e.g. due to COVID, bad harvest, stock changes, etc.).
# TODO: Should we consider a longer rolling window? Those three years may be particularly low due to COVID.
- We pick our relevant window, which will be 1963 (meaning the average of 1961, 1962, and 1963) to 2022 (meaning the average of 2020, 2021, 2022). This is the biggest window possible given the available data.
# TODO: Should we consider a shorter baseline window (following a similar logic to the step on decoupling GDP and emissions)?
- We select countries where:
  - Food supply has increased in that period by at least 5%.
  - Land use has decreased in that period by at least 5%.
  - Imports are lower than 20% of domestic supply in the latest year. This imports cap avoids selecting countries that have offshored land use, i.e. they are feeding more people by using more land elsewhere.

TODO: Analogously, we could consider applying a cap on exports in the first year. It's possible that some countries used to export a lot of food and now they export very little. Those countries may show an increase in food supply and a significant decrease in land use; but this decrease in land use would be due to feeding fewer people elsewhere, rather than feeding more people in their own country. I'm not sure if this is necessary, and I imagine this may not be a common case. Try it and see if it makes a relevant difference.

"""

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def show_decoupled_countries(
    food_column,
    tb_fbsc,
    tb_grouped,
    min_food_pct_change=5,
    min_land_pct_change=5,
    max_net_share_imports=20,
    max_net_share_median_over_years=10,
):
    import plotly.express as px

    year_max = tb_grouped["year"].max()
    year_min = tb_grouped["year"].min()

    # Calculate the share of net annual imports of each country-year.
    # This is, for each country-year (after aggregating all the main food item groups):
    # 100 * | imports - exports | / domestic supply
    # Relevant element codes:
    # 005611: Imports (tonnes)
    # 005911: Exports (tonnes)
    # 005511: Production (tonnes)
    # Instead of production, it may be more appropriate to use domestic supply instead (Domestic supply = Production + Imports − Exports ± Stock changes).
    # 005301: Domestic supply (tonnes)
    imports = (
        tb_fbsc[
            (tb_fbsc["item_code"].isin(sorted(set(sum([items for _, items in FOOD_GROUPS_FBSC.items()], [])))))
            & (tb_fbsc["element_code"].isin(["005611", "005911", "005301"]))
        ]
        .reset_index(drop=True)
        .groupby(["country", "year", "element"], as_index=False)
        .agg({"value": "sum"})
        .pivot(index=["country", "year"], columns=["element"], join_column_levels_with="_")
        .format()
        .reset_index()
    )
    # Calculate the net import share (share of domestic production that is imported).
    imports["net_import_share"] = (
        100 * (imports["value_imports"] - imports["value_exports"]) / imports["value_domestic_supply"]
    )

    check = tb_grouped.copy()
    # Add column of net imports share.
    if max_net_share_median_over_years is None:
        check = check.merge(imports, on=["country", "year"], how="left")
    else:
        # Instead of simply imposing a maximum net import share, we impose a maximum median (over the last 10 years) net import share, for each country.
        imports = (
            imports[(imports["year"] > (year_max - max_net_share_median_over_years))]
            .groupby("country", as_index=False)
            .agg({"net_import_share": "median"})
        )
        check = check.merge(imports, on=["country"], how="left")
    check = check.merge(check[(check["year"] == year_min)], how="left", on=["country"], suffixes=("", "_baseline"))
    check[f"{food_column}_change"] = (
        100 * (check[food_column] - check[f"{food_column}_baseline"]) / check[f"{food_column}_baseline"]
    )
    check["agricultural_land_change"] = (
        100 * (check["agricultural_land"] - check["agricultural_land_baseline"]) / check["agricultural_land_baseline"]
    )
    # List countries that could be added to the chart as examples of decoupling.
    # Sort them by land use relative change.
    _check = check[check["year"] == year_max]
    if food_column == "production_energy":
        decoupled = (
            _check[
                (_check["agricultural_land_change"] <= -min_land_pct_change)
                & (_check[f"{food_column}_change"] >= min_food_pct_change)
            ]
            .dropna(subset=[f"{food_column}_change", "agricultural_land_change"], how="any")
            .sort_values("agricultural_land_change")["country"]
            .unique()
            .tolist()
        )
    else:
        decoupled = (
            _check[
                (_check["net_import_share"] <= max_net_share_imports)
                & (_check["agricultural_land_change"] <= -min_land_pct_change)
                & (_check[f"{food_column}_change"] >= min_food_pct_change)
            ]
            .dropna(subset=[f"{food_column}_change", "agricultural_land_change"], how="any")
            .sort_values("agricultural_land_change")["country"]
            .unique()
            .tolist()
        )
    # Remove regions.
    decoupled = [country for country in decoupled if "(FAO)" not in country if country not in geo.REGIONS]
    # Plot decoupling curves for those countries.
    columns = [f"{food_column}_change", "agricultural_land_change"]
    for country in sorted(decoupled):
        px.line(
            check[check["country"] == country][["year"] + columns].melt(id_vars=["year"]),
            x="year",
            y="value",
            color="variable",
            markers=True,
            title=country,
        ).show()

    # Print resulting list of decoupled countries.
    print(", ".join(decoupled))


def run() -> None:
    #
    # Load inputs.
    #
    # Load FAOSTAT combined food balances dataset, and read its main table.
    ds_fbsc = paths.load_dataset("faostat_fbsc")
    tb_fbsc = ds_fbsc.read("faostat_fbsc")

    # Load FAOSTAT land use dataset, and read its main table.
    ds_rl = paths.load_dataset("faostat_rl")
    tb_rl = ds_rl.read("faostat_rl")

    #
    # Process data.
    #
    # Select relevant elements from the land use data.
    # Item "Agricultural land" (00006610).
    # Element code "Area" (005110) in hectares.
    tb_rl = tb_rl[(tb_rl["element_code"] == "005110") & (tb_rl["item_code"] == "00006610")].reset_index(drop=True)
    error = "Units of area have changed."
    assert set(tb_rl["unit"]) == {"hectares"}, error
    tb_rl = tb_rl[["country", "year", "value"]].rename(columns={"value": "agricultural_land"}, errors="raise")

    # Select relevant elements from the food balances data.
    # Select item "Total" ("00002901").
    # Select element "Food available for consumption" ("0664pc"), in kcal/capita/day. It was converted from the original "Food supply (kcal/capita/day)" to total (by multiplying by FAO population) and then divided by informed OWID population (except for FAO regions, that were divided by FAO population).
    tb = tb_fbsc[(tb_fbsc["element_code"] == "0664pc") & (tb_fbsc["item_code"] == "00002901")].reset_index(drop=True)
    # Sanity check.
    assert set(tb["element"]) == {"Food available for consumption"}
    assert set(tb["unit"]) == {"kilocalories per day per capita"}
    assert tb[tb["value"].isnull()].empty
    # Keep only relevant columns.
    tb = tb[["country", "year", "value"]].rename(columns={"value": "food_energy"}, errors="raise")

    ####################################################################################################################
    # TODO: Consider removing:
    # I think the most meaningful metrics to use are:
    # - Per capita food supply (in kcal/capita/day).
    # - Land use (in ha/year).
    # Alternatively, we could use per capita yearly food supply (kcal/capita/year), or total yearly food supply (kcal/year).
    # Convert kcal/capita/day to kcal/capita/year.
    # tb.loc[(tb["element_code"] == "0664pc"), "food_energy"] *= 365

    # Calculate the total food supply (not per capita).
    # tb_total = paths.regions.add_population(tb=tb, warn_on_missing_countries=False)
    # tb_total["food_energy"] *= tb_total["population"]
    ####################################################################################################################

    # Combine food supply and agricultural land.
    tb = tb.merge(tb_rl, on=["country", "year"], how="outer")

    # Remove regions from the list of countries.
    tb = tb[~tb["country"].isin(paths.regions.regions_all)].reset_index(drop=True)
    # Remove custom (FAO) regions.
    tb = tb[~tb["country"].str.contains("(FAO)", regex=False)].reset_index(drop=True)

    # Keep only rows that have data on both food supply and land use.
    _n_rows_before = len(tb)
    tb = tb.dropna(how="any").reset_index(drop=True)
    error = "Unexpectedly high number of rows lost when merging food supply and land use."
    assert (100 * (_n_rows_before - len(tb)) / _n_rows_before) < 23, error

    # TODO: Use rolling averages, and take some of the additional logic from the GDP vs emissions step.

    # Select countries that feed more people with less land, excluding those that did that by using land elsewhere.

    ####################################################################################################################
    # TODO: Update these conclusions.
    # Conclusions:
    # This is the list of countries for which:
    # - Agricultural production increased by >= 5% in the latest year with respect to 1961.
    # - Agricultural land use decreased by >= 5% in the latest year with respect to 1961.
    # Hong Kong, Cyprus, Italy, Greece, Poland, Sweden, Hungary, Austria, New Zealand, South Korea, Australia, Guyana, Netherlands, Iran, Chile, Mongolia, Spain, Eswatini, Finland, France, Denmark, United Kingdom, Jordan, Germany, Switzerland, Argentina, Uruguay, Romania, Kiribati, Taiwan, Iceland, Bulgaria, Algeria, United States, Albania, Canada
    # show_decoupled_countries(food_column="production_energy", tb_fbsc=tb_fbsc, tb_grouped=tb_grouped, min_food_pct_change=5, min_land_pct_change=5)

    # This is the list of countries for which:
    # - Food supply increased by >= 5% in the latest year with respect to 1961.
    # - Agricultural land use decreased by >= 5% in the latest year with respect to 1961.
    # - The median net import share over the last 10 years is < 20%.
    # Italy, Greece, Poland, Hungary, Austria, New Zealand, Australia, Guyana, Iran, Chile, Spain, Eswatini, France, Denmark, Germany, Argentina, Uruguay, Romania, Mauritius, Kiribati, Iceland, United States, Albania, Canada
    # show_decoupled_countries(food_column="food_energy", tb_fbsc=tb_fbsc, tb_grouped=tb_grouped, min_food_pct_change=5, min_land_pct_change=5, max_net_share_imports=20, max_net_share_median_over_years=10)

    # This is the list of countries for which:
    # - Food supply increased by >= 5% in the latest year with respect to 1961.
    # - Agricultural land use decreased by >= 5% in the latest year with respect to 1961.
    # - The net import share is < 20% every year.
    # Greece, Poland, Hungary, Austria, New Zealand, Australia, Guyana, Iran, Chile, Spain, Eswatini, France, Denmark, Germany, Argentina, Uruguay, Romania, Kiribati, Iceland, United States, Albania, Canada
    # show_decoupled_countries(food_column="food_energy", tb_fbsc=tb_fbsc, tb_grouped=tb_grouped, min_food_pct_change=5, min_land_pct_change=5, max_net_share_imports=20, max_net_share_median_over_years=None)
    ####################################################################################################################

    # Improve table formats.
    tb = tb.format(keys=["country", "year"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_fbsc.metadata)
    ds_garden.save()
