"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
# Regions to create aggregates for.
REGIONS_TO_ADD = [
    "World",
    "North America",
    "South America",
    "Europe",
    "European Union (27)",
    "Africa",
    "Asia",
    "Oceania",
    "Low-income countries",
    "Upper-middle-income countries",
    "Lower-middle-income countries",
    "High-income countries",
]


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("fra")

    # Read table from meadow dataset.
    tb = ds_meadow.read("fra")
    # Basic columns to keep for now
    columns_to_keep = [
        "country",
        "year",
        "_1a_forestarea",
        "_1d_expansion",
        "_1d_afforestation",
        "_1d_nat_exp",
        "_1d_deforestation",
    ]
    tb = tb[columns_to_keep]

    # Process data.
    #
    # Harmonize country names.
    tb = paths.regions.harmonize_names(tb=tb)

    # Add regions to the table.
    tb = paths.regions.add_aggregates(
        tb=tb,
        country_col="country",
        aggregations={
            "_1a_forestarea": "sum",
            "_1d_expansion": "sum",
            "_1d_afforestation": "sum",
            "_1d_nat_exp": "sum",
            "_1d_deforestation": "sum",
        },
        regions=REGIONS_TO_ADD,
        min_num_values_per_year=1,
    )
    # Convert from 1000 ha to ha
    tb[["_1a_forestarea", "_1d_expansion", "_1d_afforestation", "_1d_nat_exp", "_1d_deforestation"]] = (
        tb[["_1a_forestarea", "_1d_expansion", "_1d_afforestation", "_1d_nat_exp", "_1d_deforestation"]] * 1000
    )
    # Linearly interpolate forest area for missing years
    tb = linearly_interpolate_forest_area(tb)
    # Calculate additional variables
    tb = calculate_net_change_in_forest_area(tb)
    tb = calculate_share_of_global_forest_area(tb)
    tb = calculate_annual_change_in_forest_area_as_share_forest_area(tb)
    tb = calculate_annual_deforestation_as_share_of_forest_area(tb)
    tb = calculate_share_of_global_deforestation(tb)
    tb = calculate_share_of_annual_global_forest_expansion(tb)
    # Improve table format.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()


def linearly_interpolate_forest_area(tb: Table) -> Table:
    # Add rows for missing years and linearly interpolate forest area
    all_years = range(tb["year"].min(), tb["year"].max() + 1)
    country_all_years = (
        tb[["country"]]
        .drop_duplicates()
        .assign(key=1)
        .merge(Table.from_dict({"year": all_years}).assign(key=1), on="key")
        .drop(columns=["key"])
    )
    tb = country_all_years.merge(tb, on=["country", "year"], how="left")

    tb = tb.sort_values(by=["country", "year"])
    # linearly interpolate forest area
    tb["_1a_forestarea"] = tb.groupby("country")["_1a_forestarea"].transform(
        lambda group: group.interpolate(method="linear")
    )
    return tb


def calculate_share_of_global_forest_area(tb: Table) -> Table:
    tb_global = tb[tb["country"] == "World"][["year", "_1a_forestarea"]].rename(
        columns={"_1a_forestarea": "global_forest_area"}
    )
    tb = tb.merge(tb_global, on="year", how="left")
    tb["forestarea_share_global"] = (tb["_1a_forestarea"] / tb["global_forest_area"]) * 100
    tb = tb.drop(columns=["global_forest_area"])
    assert (
        tb["forestarea_share_global"].max() <= 100
    ), "Error in calculating share of global forest area: values exceed 100%"
    return tb


def calculate_net_change_in_forest_area(tb: Table) -> Table:
    tb["net_change_forest_area"] = tb["_1d_expansion"] - tb["_1d_deforestation"]
    return tb


def calculate_annual_change_in_forest_area_as_share_forest_area(tb: Table) -> Table:
    tb["annual_change_forest_area_share"] = (tb["net_change_forest_area"] / tb["_1a_forestarea"]) * 100
    return tb


def calculate_annual_deforestation_as_share_of_forest_area(tb: Table) -> Table:
    tb["annual_deforestation_share_forest_area"] = (tb["_1d_deforestation"] / tb["_1a_forestarea"]) * 100
    return tb


def calculate_share_of_global_deforestation(tb: Table) -> Table:
    tb_global_deforestation = tb[tb["country"] == "World"][["year", "_1d_deforestation"]].rename(
        columns={"_1d_deforestation": "global_deforestation"}
    )
    tb = tb.merge(tb_global_deforestation, on="year", how="left")
    tb["deforestation_share_global"] = (tb["_1d_deforestation"] / tb["global_deforestation"]) * 100
    tb = tb.drop(columns=["global_deforestation"])
    assert (
        tb["deforestation_share_global"].max() <= 100
    ), "Error in calculating share of global deforestation: values exceed 100%"
    return tb


def calculate_share_of_annual_global_forest_expansion(tb: Table) -> Table:
    tb_global_expansion = tb[tb["country"] == "World"][["year", "_1d_expansion"]].rename(
        columns={"_1d_expansion": "global_forest_expansion"}
    )
    tb = tb.merge(tb_global_expansion, on="year", how="left")
    tb["expansion_share_global"] = (tb["_1d_expansion"] / tb["global_forest_expansion"]) * 100
    tb = tb.drop(columns=["global_forest_expansion"])
    assert (
        tb["expansion_share_global"].max() <= 100
    ), "Error in calculating share of annual global forest expansion: values exceed 100%"
    return tb
