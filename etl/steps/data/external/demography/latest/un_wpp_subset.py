from owid.catalog import Table
from owid.catalog import processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("un_wpp")
    #
    # Process data.
    #
    tables = [
        ds_garden["population"],
        ds_garden["growth_rate"],
        ds_garden["natural_change_rate"],
        ds_garden["fertility_rate"],
        ds_garden["migration"],
        ds_garden["deaths"],
        ds_garden["births"],
        ds_garden["life_expectancy"],
        ds_garden["mortality_rate"],
    ]

    simple_tables = [get_simple_data(tb) for tb in tables]
    tb = pr.multi_merge(simple_tables, on=["country", "year", "sex", "age", "variant"], how="outer")
    tb = tb.drop(columns=["population_density"])
    tb["check_change"] = tb["births"] - tb["deaths"] + tb["net_migration"]
    tb["change_diff"] = tb["population_change"] - tb["check_change"]

    tb = tb.format(["country", "year", "sex", "age", "variant"])
    ds_garden = paths.create_dataset(
        tables=[tb],
        check_variables_metadata=True,
        default_metadata=ds_garden.metadata,
        formats=["csv"],
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def get_simple_data(tb: Table) -> Table:
    """
    Getting a simple subset of the data, where only have data for all ages, both sexes and the estimated historical data, with the medium variant.
    """

    tb = tb.reset_index()
    tb = tb[(tb["age"] == "all") & (tb["sex"] == "all")]
    tb = tb[tb["variant"].isin(["estimates", "medium"])]
    # tb = tb.format(["country", "year", "sex", "age", "variant"])
    return tb
