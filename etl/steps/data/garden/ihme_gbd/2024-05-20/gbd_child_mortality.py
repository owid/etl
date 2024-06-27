"""Load a meadow dataset and create a garden dataset."""


from owid.catalog import Table
from owid.catalog import processing as pr

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
REGIONS = ["North America", "South America", "Europe", "Africa", "Asia", "Oceania"]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("gbd_child_mortality")

    # Read table from meadow dataset.
    tb = ds_meadow["gbd_child_mortality"].reset_index()
    ds_regions = paths.load_dataset("regions")
    #
    # Process data.
    #
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    # Add regional aggregates
    msk = tb["metric"] == "Number"
    tb_number = tb[msk]
    tb_other = tb[~msk]
    tb_number = geo.add_regions_to_table(
        tb_number,
        ds_regions,
        regions=REGIONS,
        index_columns=["country", "year", "metric", "measure", "cause", "age", "sex"],
    )
    tb = pr.concat([tb_number, tb_other])
    # Split into two tables: one for deaths, one for DALYs
    tb_deaths = tb[tb["measure"] == "Deaths"].copy()
    tb_deaths = add_additional_age_groups(tb_deaths)
    # Creating a table where the disease is the entity - for total under 5 deaths by cause - for a specific chart
    tb_ent = disease_as_entity(tb_deaths)
    # Creating a table where the disease is the entity - under 5 death rates for India - for a specific chart
    tb_india = under_five_death_rate_india(tb_deaths)
    # Creating a table for infant mortality rate - for a specific chart
    tb_infant = global_infant_mortality_rate(tb_deaths)
    tb_dalys = tb[tb["measure"] == "DALYs (Disability-Adjusted Life Years)"].copy()
    # Shorten the metric name for DALYs
    tb_dalys["measure"] = "DALYs"

    # Drop the measure column
    tb_deaths = tb_deaths.drop(columns="measure")
    tb_dalys = tb_dalys.drop(columns="measure")

    # Format the tables
    tb_deaths = tb_deaths.format(
        ["country", "year", "metric", "age", "sex", "cause"], short_name="gbd_child_mortality_deaths"
    )
    tb_dalys = tb_dalys.format(
        ["country", "year", "metric", "age", "sex", "cause"], short_name="gbd_child_mortality_dalys"
    )
    tb_ent = tb_ent.format(
        ["country", "year"],
        short_name="gbd_child_mortality_slope",
    )
    tb_india = tb_india.format(["country", "year"], short_name="gbd_child_mortality_india")

    tb_infant = tb_infant.format(["country", "year"], short_name="gbd_child_mortality_infant")
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir,
        tables=[tb_deaths, tb_dalys, tb_ent, tb_india, tb_infant],
        check_variables_metadata=True,
        default_metadata=ds_meadow.metadata,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def global_infant_mortality_rate(tb: Table) -> Table:
    """
    Creating the data format needed for this chart: https://ourworldindata.org/grapher/infant-death-rates-by-cause-by-sex
    """

    tb_infant = tb[
        (tb["measure"] == "Deaths")
        & (tb["country"] == "World")
        & (tb["age"] == "<1 year")
        & (tb["metric"] == "Rate")
        & (tb["year"] == tb["year"].max())
    ]
    tb_infant = tb_infant[["year", "cause", "value", "sex"]].copy()
    tb_infant = tb_infant.rename(columns={"value": "infant_death_rate", "cause": "country"})

    tb_pivot = tb_infant.pivot(values=["infant_death_rate"], index=["year", "country"], columns=["sex"])
    tb_pivot.columns = ["_".join(col).strip() if isinstance(col, tuple) else col for col in tb_pivot.columns.values]
    tb_pivot = tb_pivot.reset_index()

    return tb_pivot


def under_five_death_rate_india(tb: Table) -> Table:
    """
    Creating the data format needed for this chart:
    https://ourworldindata.org/grapher/child-deaths-by-cause-by-sex-india

    """
    tb_india = tb[
        (tb["measure"] == "Deaths")
        & (tb["country"] == "India")
        & (tb["age"] == "<5 years")
        & (tb["metric"] == "Rate")
        & (tb["year"] == tb["year"].max())
    ]
    tb_india = tb_india[["year", "cause", "value", "sex"]].copy()
    tb_india = tb_india.rename(columns={"value": "under_five_death_rate", "cause": "country"})
    # pivot by sex

    tb_pivot = tb_india.pivot(values=["under_five_death_rate"], index=["year", "country"], columns=["sex"])
    tb_pivot.columns = ["_".join(col).strip() if isinstance(col, tuple) else col for col in tb_pivot.columns.values]
    tb_pivot = tb_pivot.reset_index()

    return tb_pivot


def add_additional_age_groups(tb: Table) -> Table:
    """
    Adding additional age groups to the table, 28-264 days and 1-4 years. These age-groups were previously supplied but IHME, but no longer are.
    """
    tb_number = tb[tb["metric"] == "Number"]
    # calculating 28-364 days
    tb_neonatal = tb_number[tb_number["age"] == "<28 days"]
    tb_infant = tb_number[tb_number["age"] == "<1 year"]

    tb_diff = tb_neonatal.merge(
        tb_infant, on=["country", "year", "sex", "cause", "measure", "metric"], suffixes=("_neonatal", "_infant")
    )
    tb_diff["age"] = "28-364 days"
    tb_diff["value"] = tb_diff["value_infant"] - tb_diff["value_neonatal"]

    # Calculating 1-4 years
    tb_child = tb_number[tb_number["age"] == "<5 years"]

    tb_1_4 = tb_child.merge(
        tb_infant, on=["country", "year", "sex", "cause", "measure", "metric"], suffixes=("_child", "_infant")
    )
    tb_1_4["age"] = "1-4 years"
    tb_1_4["value"] = tb_1_4["value_child"] - tb_1_4["value_infant"]

    tb = pr.concat([tb, tb_diff, tb_1_4])
    # remove columns ending in _neonatal, _child and _infant
    tb = tb.drop(columns=tb.filter(like="_").columns)

    return tb


def disease_as_entity(tb: Table) -> Table:
    """
    For this Slope chart (https://ourworldindata.org/grapher/global-child-deaths-by-cause) we need to have the death rates for <5s, where the country is the disease or injury
    """
    tb_ent = tb[
        (tb["age"] == "<5 years")
        & (tb["metric"] == "Number")
        & (tb["sex"] == "Both")
        & (tb["country"] == "World")
        & (tb["year"].isin([tb["year"].min(), tb["year"].max()]))
    ]

    tb_ent = tb_ent[["year", "cause", "value"]].rename(columns={"cause": "country", "value": "under_five_deaths"})

    tb_ent = tb_ent[tb_ent["country"] != "All causes"]
    return tb_ent
